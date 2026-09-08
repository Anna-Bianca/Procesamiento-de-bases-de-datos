from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "Procesamiento" / "2 - Normalizacion" / "2_Normalizacion.py"


def load_module():
    spec = importlib.util.spec_from_file_location("normalizacion_paso_2", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


normalizacion = load_module()


class NormalizationUnitTests(unittest.TestCase):
    def test_line_endings_controls_unicode_and_whitespace(self):
        source = "\ufeffCafe\u0301\r\ncon\u200b\t\ttexto\x00\r\n\r\n\r\nFin"
        self.assertEqual(
            normalizacion.normalize_text(source),
            "Café\ncon texto\n\nFin",
        )

    def test_soft_hyphen_repairs_break_and_removes_residual(self):
        self.assertEqual(
            normalizacion.normalize_text("micro\u00ad\nscopia y co\u00adfactor"),
            "microscopia y cofactor",
        )

    def test_visible_hyphen_requires_intact_word_in_same_document(self):
        evidenced = "La micro-\nscopia fue útil. La microscopia confirmó el hallazgo."
        self.assertEqual(
            normalizacion.normalize_text(evidenced),
            "La microscopia fue útil. La microscopia confirmó el hallazgo.",
        )
        legitimate = "El análisis de costo-\nefectividad fue clínicamente relevante."
        self.assertEqual(normalizacion.normalize_text(legitimate), legitimate)

    def test_html_uses_ignored_stack_and_preserves_alt_and_structure(self):
        source = (
            "<nav><div>Inicio</div><script>mal()</script></nav>"
            "<h1>Título</h1><p>Texto <img alt=\"Micrografía\"></p>"
            "<div role=\"navigation\">Siguiente</div><p>Final</p>"
        )
        self.assertEqual(
            normalizacion.normalize_text(source),
            "Título\n\nTexto Micrografía\n\nFinal",
        )

    def test_mismatched_ignored_close_prefers_visible_text(self):
        source = "<nav><span>Inicio</nav><p>Clínica</p>"
        self.assertEqual(normalizacion.normalize_text(source), "InicioClínica")

    def test_normal_words_and_unknown_biomedical_angle_tags_survive(self):
        source = "área base menu nav script style <p>Gen <BRCA1> válido</p>"
        normalized = normalizacion.normalize_text(source)
        self.assertIn("área base menu nav script style", normalized)
        self.assertIn("<brca1>", normalized.casefold())

    def test_only_semicolon_terminated_valid_entities_are_decoded(self):
        source = "A &amp; B &copy C &notanentity; D &amp;amp; E &#99999999;"
        self.assertEqual(
            normalizacion.normalize_text(source),
            "A & B &copy C &notanentity; D & E &#99999999;",
        )

    def test_each_operation_reports_changes_without_persisting_them(self):
        normalized, stats = normalizacion.normalize_text_with_stats("A\r\nB\u200b")
        self.assertEqual(normalized, "A\nB")
        self.assertEqual(stats["finales_linea_iniciales"]["records_affected"], 1)
        self.assertGreaterEqual(stats["controles_e_invisibles"]["substitutions"], 1)

    def test_idempotence_for_individual_and_combined_cases(self):
        cases = [
            "A\r\nB",
            "micro\u00ad\nscopia",
            "Cafe\u0301",
            "&amp;amp; y &copy",
            "<p>Uno</p><p>Dos <img alt='imagen'></p>",
            "  A\t\tB\n\n\n\nC  ",
            "micro-\nscopia microscopia <nav>Inicio</nav>&amp;",
        ]
        for source in cases:
            with self.subTest(source=source):
                once = normalizacion.normalize_text(source)
                self.assertEqual(normalizacion.normalize_text(once), once)

    def test_record_preserves_metadata_and_adds_step_once(self):
        record = {
            "id": "r1",
            "texto": "A\r\nB",
            "metadata": {"x": 1},
            "procesamiento": ["Paso 1: Unificacion"],
        }
        result = normalizacion.normalize_record(
            (1, (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
        )
        output = json.loads(result.output_line)
        self.assertEqual(output["metadata"], {"x": 1})
        self.assertEqual(output["texto"], "A\nB")
        self.assertEqual(output["procesamiento"].count(normalizacion.PASO_PROCESAMIENTO), 1)
        second = normalizacion.normalize_record((1, result.output_line))
        second_output = json.loads(second.output_line)
        self.assertEqual(
            second_output["procesamiento"].count(normalizacion.PASO_PROCESAMIENTO), 1
        )

    def test_invalid_utf8_is_rejected_with_physical_line(self):
        with self.assertRaisesRegex(ValueError, "linea fisica 7"):
            normalizacion.normalize_record((7, b'{"texto":"\xff"}\n'))


class NormalizationFileTests(unittest.TestCase):
    def test_cli_multiprocessing_preserves_record_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "input.jsonl"
            output_path = root / "output.jsonl"
            checkpoint_path = root / "checkpoint.json"
            summary_path = root / "summary.json"
            input_path.write_text(
                "".join(
                    json.dumps(
                        {"id": str(index), "texto": "A\r\nB", "procesamiento": ["Paso 1"]}
                    ) + "\n"
                    for index in range(6)
                ),
                encoding="utf-8",
                newline="\n",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--input-file", str(input_path),
                    "--output-file", str(output_path),
                    "--checkpoint-file", str(checkpoint_path),
                    "--summary-file", str(summary_path),
                    "--workers", "2",
                    "--batch-size", "2",
                    "--progress-every", "0",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            output_records = [
                json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual([record["id"] for record in output_records], [str(i) for i in range(6)])

    def test_streaming_checkpoint_summary_and_completed_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "input.jsonl"
            output_path = root / "output.jsonl"
            checkpoint_path = root / "checkpoint.json"
            summary_path = root / "summary.json"
            records = [
                {"id": "1", "texto": "A\r\nB", "procesamiento": ["Paso 1"]},
                {"id": "2", "texto": "Sin cambios", "procesamiento": ["Paso 1"]},
                {"id": "3", "texto": "C\u200bD", "procesamiento": ["Paso 1"]},
            ]
            input_path.write_text(
                "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records),
                encoding="utf-8",
                newline="\n",
            )
            result = normalizacion.process_file(
                input_path=input_path,
                output_path=output_path,
                checkpoint_path=checkpoint_path,
                summary_path=summary_path,
                batch_size=1,
                max_batch_bytes=1024,
                workers=1,
                progress_every=0,
                resume=False,
                overwrite=False,
            )
            self.assertEqual(result.records_processed, 3)
            self.assertEqual(result.records_changed, 2)
            output_records = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([item["id"] for item in output_records], ["1", "2", "3"])
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertTrue(checkpoint["completed"])
            self.assertEqual(checkpoint["records_processed"], 3)
            self.assertEqual(summary["registros_modificados"], 2)
            self.assertEqual(
                checkpoint["operation_stats"], summary["transformaciones"]
            )
            resumed = normalizacion.process_file(
                input_path=input_path,
                output_path=output_path,
                checkpoint_path=checkpoint_path,
                summary_path=summary_path,
                batch_size=99,
                max_batch_bytes=9999,
                workers=1,
                progress_every=0,
                resume=True,
                overwrite=False,
            )
            self.assertEqual(resumed.records_processed, 3)
            self.assertEqual(resumed.operation_stats, result.operation_stats)

    def test_failed_batch_resume_does_not_duplicate_confirmed_statistics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "input.jsonl"
            output_path = root / "output.jsonl"
            checkpoint_path = root / "checkpoint.json"
            summary_path = root / "summary.json"
            valid = json.dumps(
                {"id": "1", "texto": "A\r\nB", "procesamiento": ["Paso 1"]}
            ).encode("utf-8") + b"\n"
            input_path.write_bytes(valid + b'{"id":"2","texto":"\xff","procesamiento":[]}\n')
            arguments = dict(
                input_path=input_path,
                output_path=output_path,
                checkpoint_path=checkpoint_path,
                summary_path=summary_path,
                batch_size=1,
                max_batch_bytes=1024,
                workers=1,
                progress_every=0,
                overwrite=False,
            )
            with self.assertRaises(ValueError):
                normalizacion.process_file(**arguments, resume=False)
            first_checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(first_checkpoint["records_processed"], 1)
            self.assertEqual(
                first_checkpoint["operation_stats"]["finales_linea_iniciales"]["records_affected"],
                1,
            )
            with self.assertRaises(ValueError):
                normalizacion.process_file(**arguments, resume=True)
            second_checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            self.assertEqual(second_checkpoint["records_processed"], 1)
            self.assertEqual(
                second_checkpoint["operation_stats"]["finales_linea_iniciales"]["records_affected"],
                1,
            )
            self.assertEqual(second_checkpoint["errors"], 2)


if __name__ == "__main__":
    unittest.main()

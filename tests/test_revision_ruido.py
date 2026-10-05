from __future__ import annotations

import csv
import importlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

STEP3_DIR = Path(__file__).resolve().parents[1] / "Procesamiento" / "3 - Eliminar ruido"
if str(STEP3_DIR) not in sys.path:
    sys.path.insert(0, str(STEP3_DIR))
review = importlib.import_module("revision_ruido_core")
workflow = review.workflow
core = importlib.import_module("ruido_core")


def write_rows(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=workflow.CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path):
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        return list(csv.DictReader(source))


class ReviewFixtures(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "candidatos_ruido.csv"
        self.state = self.root / "revision_ruido.sqlite3"
        self.exported = self.root / "candidatos_ruido_revisados.csv"
        self.input_path = self.root / "normalizado.jsonl"
        self.records = []
        texts = [
            ('<script>"á,ß"</script>\nSegunda línea\r\n', "COOKIE_NOTICE", "A", False),
            ("Error 404. Página no encontrada.", "HTTP_ERROR_PAGE", "B", True),
            ("x" * 150_000, "REPEATED_CHARACTERS", "A", False),
            ("Contenido patrocinado", "ADVERTISING", "B", False),
        ]
        for index, (fragment, reason, origin, full) in enumerate(texts, start=1):
            self.records.append({"id": f"r{index}", "texto": fragment if full else f"Antes.\n{fragment}\nDespués.",
                                 "base_de_datos_origen": origin, "archivo_origen": f"{index}.txt",
                                 "ruta_relativa_origen": f"{index}.txt", "meta": {"preservar": True},
                                 "procesamiento": ["Paso 2: Normalizacion"]})
        self.input_path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in self.records), encoding="utf-8")
        input_sha = workflow.full_sha256(self.input_path)
        rows = []
        for index, (fragment, reason, origin, full) in enumerate(texts, start=1):
            start = 0 if full else len("Antes.\n")
            candidate = core.ConsolidatedResult((reason,), (reason,), "registro" if full else "fragmento",
                                                start, start + len(fragment), 0.8,
                                                {reason: [{"signal_count": 2, "ejemplo": "á"}]},
                                                core.DELETE_RECORD if full else core.DELETE_SPAN)
            rows.append(workflow.candidate_row(candidate=candidate, record=self.records[index - 1],
                                               record_number=index, input_sha256=input_sha))
        write_rows(self.source, rows)
        accumulator = workflow.fresh_candidate_set_accumulator()
        workflow.update_candidate_set_accumulator(accumulator, (row["candidate_id"] for row in rows))
        self.summary_path = self.root / "auditoria_resumen.json"
        self.summary_path.write_text(json.dumps({"version_auditoria": core.AUDIT_VERSION, "total_candidatos": len(rows),
                                                "conjunto_inmutable_candidatos": accumulator,
                                                "fingerprint_entrada": {"sha256": input_sha, "path": str(self.input_path)}}), encoding="utf-8")
        self.original_bytes = self.source.read_bytes()

    def prepare(self, **kwargs):
        review.prepare_store(self.source, self.state, **kwargs)
        return review.ReviewStore(self.state)


class PersistenceTests(ReviewFixtures):
    def test_import_filters_large_multiline_and_resume(self):
        store = self.prepare()
        self.assertEqual(store.metadata()["counts"], {"total": 4, "pending": 4, "eliminar": 0, "conservar": 0})
        self.assertIn('\r\n', store.candidate(1)["texto_detectado"])
        self.assertEqual(len(store.candidate(3)["texto_detectado"]), 150_000)
        self.assertEqual(store.count(origin="A"), 2)
        self.assertEqual(store.find(reason="HTTP_ERROR_PAGE"), 2)
        self.assertEqual(store.find(after=4, status=""), 1)
        store.save(1, "eliminar", 'Revisado, con "comillas"\nComentario á.', expected_revision=0)
        store.remember(2)
        with self.assertRaises(FileExistsError):
            self.prepare()
        self.prepare(resume=True)
        self.assertEqual(store.candidate(1)["decision"], "eliminar")
        self.assertEqual(store.metadata()["last_seq"], 2)
        self.assertEqual(self.source.read_bytes(), self.original_bytes)

    def test_interrupted_import_resumes_confirmed_bytes(self):
        def interrupt(current, total, phase):
            if phase == "Importando candidatos a SQLite" and current == 1:
                raise RuntimeError("Interrupción simulada después del checkpoint")
        with self.assertRaises(RuntimeError):
            self.prepare(batch_size=1, progress=interrupt)
        connection = review.connect(self.state)
        try:
            self.assertEqual(review.get_metadata(connection)["counts"]["total"], 1)
        finally:
            connection.close()
        store = self.prepare(resume=True, batch_size=2)
        self.assertEqual(store.count(), 4)
        self.assertEqual(store.candidate(1)["texto_detectado"], read_rows(self.source)[0]["texto_detectado"])

    def test_decision_edits_counters_and_stale_tabs(self):
        store = self.prepare()
        store.save(1, "eliminar", "sí", expected_revision=0)
        with self.assertRaises(review.ReviewConflictError):
            store.save(1, "conservar", "pestaña obsoleta", expected_revision=0)
        store.save(1, "conservar", "corregido", expected_revision=1)
        store.save(1, "", "más tarde", expected_revision=2)
        self.assertEqual(store.metadata()["counts"]["pending"], 4)
        self.assertEqual(store.metadata()["counts"]["eliminar"], 0)
        self.assertEqual(store.candidate(1)["notas_revision"], "más tarde")
        with self.assertRaises(ValueError):
            store.save(1, "borrar", "", expected_revision=3)

    def test_changed_source_and_sample_are_rejected(self):
        self.prepare()
        rows = read_rows(self.source)
        rows[0]["decision"] = "conservar"
        write_rows(self.source, rows)
        with self.assertRaisesRegex(ValueError, "CSV cambió"):
            self.prepare(resume=True)
        write_rows(self.source, rows[:1])
        with self.assertRaisesRegex(ValueError, "exactamente todos"):
            review.prepare_store(self.source, self.root / "muestra.sqlite3")

    def test_only_own_exports_can_be_overwritten(self):
        store = self.prepare()
        self.exported.write_text("archivo ajeno", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            store.export(self.exported)
        with self.assertRaises(ValueError):
            store.export(self.source)
        with self.assertRaises(ValueError):
            store.export(self.root / "muestra_revision_ruido.csv")
        self.assertEqual(self.exported.read_text(encoding="utf-8"), "archivo ajeno")

    def test_export_can_recover_interruption_after_atomic_replace(self):
        store = self.prepare()
        replace = review.os.replace

        def interrupted_replace(source, target):
            replace(source, target)
            raise RuntimeError("Cierre después de replace")

        with mock.patch.object(review.os, "replace", side_effect=interrupted_replace):
            with self.assertRaises(RuntimeError):
                store.export(self.exported)
        self.assertTrue(self.exported.exists())
        self.assertEqual(store.metadata()["export_revision"], -1)
        store.export(self.exported)
        self.assertEqual(store.metadata()["export_revision"], 0)

    def test_unfinished_audit_and_changed_summary_are_rejected(self):
        checkpoint = self.root / "auditoria.checkpoint.json"
        checkpoint.write_text(json.dumps({"completed": False}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "todavía no terminó"):
            self.prepare()
        checkpoint.write_text(json.dumps({"completed": True}), encoding="utf-8")
        self.prepare()
        summary = json.loads(self.summary_path.read_text(encoding="utf-8"))
        summary["otra_clave"] = True
        self.summary_path.write_text(json.dumps(summary), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "resumen.*cambió"):
            self.prepare(resume=True)

    def test_export_complete_csv_compatible_with_apply(self):
        store = self.prepare()
        store.save(1, "eliminar", 'Ruido\n"Confirmado", sí.', expected_revision=0)
        store.save(2, "eliminar", "Error HTTP", expected_revision=0)
        store.save(3, "conservar", "Revisado", expected_revision=0)
        result = store.export(self.exported)
        exported_rows, original_rows = read_rows(self.exported), read_rows(self.source)
        self.assertEqual(len(exported_rows), 4)
        for original, exported in zip(original_rows, exported_rows):
            for column in workflow.CSV_COLUMNS:
                if column not in ("decision", "notas_revision"):
                    self.assertEqual(original[column], exported[column])
        self.assertEqual(exported_rows[3]["decision"], "")
        self.assertEqual(result["counts"]["pending"], 1)
        self.assertEqual(store.metadata()["export_revision"], store.metadata()["revision"])
        # Reexportar una salida propia intacta está permitido.
        store.export(self.exported)
        paths = workflow.ApplyPaths(self.root / "sin_ruido.jsonl", self.root / "eliminaciones_aplicadas.csv",
                                    self.root / "aplicacion_resumen.json", self.root / "aplicacion.checkpoint.json",
                                    self.root / "aplicacion_decisiones.sqlite3")
        summary = workflow.run_apply(input_path=self.input_path, review_csv=self.exported, paths=paths,
                                     batch_size=2, max_batch_bytes=1024 * 1024, progress_every=0,
                                     resume=False, overwrite=False)
        self.assertEqual(summary["decisiones_eliminar"], 2)
        self.assertEqual(summary["candidatos_pendientes"], 1)
        cleaned = [json.loads(line) for line in paths.output_jsonl.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(cleaned), 3)
        self.assertNotIn("r2", {row["id"] for row in cleaned})
        self.assertNotIn("<script>", cleaned[0]["texto"])
        self.assertEqual(cleaned[0]["meta"], {"preservar": True})
        self.assertEqual(cleaned[-1]["texto"], self.records[-1]["texto"])
        self.assertEqual(self.source.read_bytes(), self.original_bytes)


class StreamlitTests(ReviewFixtures):
    def app(self, *, resume=False):
        try:
            from streamlit.testing.v1 import AppTest
        except ImportError:
            self.skipTest("Streamlit no está instalado en este Python")
        arguments = ["--review-csv", str(self.source), "--session-goal", "2", "--preview-chars", "80"]
        if resume:
            arguments.append("--resume")
        script = f"import sys\nsys.path.insert(0, {str(STEP3_DIR)!r})\nfrom revision_ruido_streamlit import main\nmain({arguments!r})\n"
        return AppTest.from_string(script, default_timeout=30).run()

    @staticmethod
    def button(app, label):
        return next(button for button in app.button if button.label == label)

    def test_save_resume_export_and_complete_record_confirmation(self):
        app = self.app()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state["current_seq"], 1)
        app.radio[0].set_value("Eliminar")
        app.text_area[0].set_value("Ruido confirmado á")
        self.button(app, "Guardar y seguir").click().run()
        self.assertFalse(app.exception)
        store = review.ReviewStore(self.state)
        self.assertEqual(store.candidate(1)["decision"], "eliminar")
        self.assertEqual(store.candidate(1)["notas_revision"], "Ruido confirmado á")
        self.assertEqual(app.session_state["current_seq"], 2)
        resumed = self.app(resume=True)
        self.assertFalse(resumed.exception)
        self.assertEqual(resumed.session_state["current_seq"], 2)
        resumed.radio[0].set_value("Eliminar")
        self.button(resumed, "Guardar y seguir").click().run()
        self.assertEqual(store.candidate(2)["decision"], "")
        self.assertTrue(any("confirmá" in error.value for error in resumed.error))
        next(checkbox for checkbox in resumed.checkbox if "registro completo" in checkbox.label).check()
        self.button(resumed, "Guardar y seguir").click().run()
        self.assertEqual(store.candidate(2)["decision"], "eliminar")
        self.button(resumed, "Exportar CSV completo para apply").click().run()
        self.assertFalse(resumed.exception)
        self.assertTrue(self.exported.exists())
        self.assertEqual(len(read_rows(self.exported)), 4)
        self.assertTrue(any("--mode apply" in code.value for code in resumed.code))

    def test_save_without_advancing_and_pending_comments(self):
        app = self.app()
        app.radio[0].set_value("Conservar")
        app.text_area[0].set_value("Documento válido")
        self.button(app, "Guardar sin avanzar").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state["current_seq"], 1)
        self.assertEqual(app.radio[0].value, "Conservar")
        self.button(app, "Siguiente sin guardar").click().run()
        self.assertEqual(app.session_state["current_seq"], 2)
        app.text_area[0].set_value("Revisar más tarde")
        self.button(app, "Marcar pendiente y seguir").click().run()
        store = review.ReviewStore(self.state)
        self.assertEqual(store.candidate(2)["decision"], "")
        self.assertEqual(store.candidate(2)["notas_revision"], "Revisar más tarde")
        self.assertEqual(store.metadata()["counts"]["pending"], 3)


if __name__ == "__main__":
    unittest.main()

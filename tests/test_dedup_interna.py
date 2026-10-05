from __future__ import annotations

import csv
from contextlib import closing, redirect_stderr
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
STEP = ROOT / "Procesamiento" / "4 - Dedup interna"
sys.path.insert(0, str(STEP))
spec = importlib.util.spec_from_file_location("dedup_step4", STEP / "4_Dedup_interna.py")
assert spec and spec.loader
workflow = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = workflow
spec.loader.exec_module(workflow)


def sentences(prefix: str, count: int, start: int = 0) -> str:
    return " ".join(
        " ".join(f"{prefix}{number}_{word}" for word in range(20)) + "."
        for number in range(start, start + count)
    )


def record(id_: str, origin: str, text: str) -> dict:
    return {"id": id_, "texto": text, "base_de_datos_origen": origin,
            "procesamiento": ["Paso 3: Eliminacion de ruido"]}


class DedupFlowTests(unittest.TestCase):
    def run_flow(self, records: list[dict], approve: bool = True):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "input.jsonl"
            output_dir = root / "out"
            input_path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records), encoding="utf-8")
            args = ["--input-file", str(input_path), "--output-dir", str(output_dir),
                    "--batch-size", "2", "--progress-every", "0", "--heartbeat-seconds", "0"]
            self.assertEqual(workflow.main(["--mode", "audit", *args]), 0)
            with (output_dir / "grupos.csv").open(encoding="utf-8-sig", newline="") as stream:
                groups = list(csv.DictReader(stream))
            with (output_dir / "acciones_propuestas.csv").open(encoding="utf-8-sig", newline="") as stream:
                actions = list(csv.DictReader(stream))
            self.assertTrue((output_dir / "miembros.csv").exists())
            if approve:
                for group in groups:
                    group["decision"] = "aprobar"
                with (output_dir / "grupos.csv").open("w", encoding="utf-8-sig", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=workflow.GROUP_COLUMNS)
                    writer.writeheader()
                    writer.writerows(groups)
            self.assertEqual(workflow.main(["--mode", "apply", *args]), 0)
            result = [json.loads(line) for line in (output_dir / "deduplicado.jsonl").read_text(encoding="utf-8").splitlines()]
            summary = json.loads((output_dir / "aplicacion_resumen.json").read_text(encoding="utf-8"))
            return groups, actions, result, summary

    def test_exact_prefers_cowese_even_when_second(self):
        text = sentences("igual", 6)
        groups, actions, output, summary = self.run_flow([
            record("mmedc", "MMedC", text), record("cowese", "CoWeSe", text)])
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["keeper_id"], "cowese")
        self.assertEqual([r["id"] for r in output], ["cowese"])
        self.assertEqual(summary["removed_records"], 1)
        self.assertEqual(actions[0]["action"], "eliminar")

    def test_complete_article_beats_preferred_excerpt(self):
        common = sentences("comun", 6)
        full = common + " " + sentences("extra", 5)
        groups, _actions, output, _summary = self.run_flow([
            record("extracto", "CoWeSe", common), record("completo", "MMedC", full)])
        self.assertEqual(groups[0]["keeper_id"], "completo")
        self.assertEqual([r["id"] for r in output], ["completo"])

    def test_partial_copy_trims_lower_priority_and_keeps_unique(self):
        common = sentences("compartido", 6)
        high = sentences("alto", 3) + " " + common
        low = common + " " + sentences("propio", 3)
        _groups, actions, output, summary = self.run_flow([
            record("alto", "CoWeSe", high), record("bajo", "MMedC", low)])
        self.assertEqual(len(output), 2)
        self.assertTrue(any(a["action"] == "recortar" and a["id"] == "bajo" for a in actions))
        self.assertNotIn("compartido0_0", output[1]["texto"])
        self.assertIn("propio0_0", output[1]["texto"])
        self.assertEqual(summary["trimmed_records"], 1)

    def test_pending_group_changes_nothing_but_processing_label(self):
        text = sentences("igual", 6)
        _groups, _actions, output, summary = self.run_flow([
            record("a", "CoWeSe", text), record("b", "MMedC", text)], approve=False)
        self.assertEqual(len(output), 2)
        self.assertEqual(summary["removed_records"], 0)
        self.assertTrue(all(workflow.STEP_LABEL in r["procesamiento"] for r in output))

    def test_changed_dose_is_never_dropped_as_whole_record(self):
        common = sentences("clinico", 12)
        first = common + " La dosis indicada fue 10 mg."
        second = common + " La dosis indicada fue 20 mg."
        _groups, actions, output, summary = self.run_flow([
            record("diez", "CoWeSe", first), record("veinte", "MMedC", second)])
        self.assertEqual(summary["removed_records"], 0)
        self.assertEqual(len(output), 2)
        self.assertIn("10 mg", output[0]["texto"])
        self.assertIn("20 mg", output[1]["texto"])
        self.assertFalse(any(a["action"] == "eliminar" for a in actions))

    def test_different_header_same_article(self):
        body = sentences("articulo", 12)
        _groups, actions, output, summary = self.run_flow([
            record("portal", "MMedC", "Portal médico sobre cardiología. " + body),
            record("referencia", "CoWeSe", "Artículo de cardiología. " + body)])
        self.assertEqual(summary["removed_records"], 1)
        self.assertEqual([item["id"] for item in output], ["referencia"])
        self.assertTrue(any(item["action"] == "eliminar" for item in actions))

    def test_html_pdf_spacing_and_punctuation(self):
        body = sentences("contenido", 6)
        reformatted = body.replace(". ", ".\n").replace(" ", "  ")
        _groups, _actions, output, summary = self.run_flow([
            record("pdf", "MMedC", reformatted),
            record("html", "CoWeSe", body)])
        self.assertEqual(summary["removed_records"], 1)
        self.assertEqual([item["id"] for item in output], ["html"])

    def test_reviewer_can_override_keeper(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path, output_dir = root / "input.jsonl", root / "out"
            body = sentences("igual", 6)
            input_path.write_text("".join(json.dumps(item) + "\n" for item in (
                record("mmedc", "MMedC", body), record("cowese", "CoWeSe", body))), encoding="utf-8")
            args = ["--input-file", str(input_path), "--output-dir", str(output_dir),
                    "--progress-every", "0", "--heartbeat-seconds", "0"]
            workflow.main(["--mode", "audit", *args])
            review = output_dir / "grupos.csv"
            with review.open(encoding="utf-8-sig", newline="") as stream:
                groups = list(csv.DictReader(stream))
            groups[0]["decision"] = "aprobar"
            groups[0]["keeper_override"] = "1"
            with review.open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=workflow.GROUP_COLUMNS)
                writer.writeheader()
                writer.writerows(groups)
            workflow.main(["--mode", "apply", *args])
            output = [json.loads(line) for line in (output_dir / "deduplicado.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual([item["id"] for item in output], ["mmedc"])

    def test_modified_group_signature_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path, output_dir = root / "input.jsonl", root / "out"
            body = sentences("igual", 6)
            input_path.write_text("".join(json.dumps(item) + "\n" for item in (
                record("a", "MMedC", body), record("b", "CoWeSe", body))), encoding="utf-8")
            args = ["--input-file", str(input_path), "--output-dir", str(output_dir),
                    "--progress-every", "0", "--heartbeat-seconds", "0"]
            workflow.main(["--mode", "audit", *args])
            review = output_dir / "grupos.csv"
            with review.open(encoding="utf-8-sig", newline="") as stream:
                groups = list(csv.DictReader(stream))
            groups[0]["signature"] = "obsoleta"
            groups[0]["decision"] = "aprobar"
            with review.open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=workflow.GROUP_COLUMNS)
                writer.writeheader()
                writer.writerows(groups)
            with self.assertRaisesRegex(ValueError, "modificado"):
                workflow.main(["--mode", "apply", *args])
            self.assertFalse((output_dir / "deduplicado.jsonl").exists())

    def test_heartbeat_reports_during_blocked_phase(self):
        capture = io.StringIO()
        heartbeat = workflow.AuditHeartbeat(0.02)
        with redirect_stderr(capture):
            heartbeat.start()
            heartbeat.phase("generación de pares", active=10, confirmed=9,
                            total=48, unit="bloques")
            time.sleep(0.07)
            heartbeat.close()
        output = capture.getvalue()
        self.assertIn("[ACTIVO] generación de pares", output)
        self.assertIn("en curso 10/48 bloques", output)
        self.assertIn("checkpoint confirmado 9/48", output)

    def test_index_resume_reuses_confirmed_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path, output_dir = root / "input.jsonl", root / "out"
            items = [record(str(n), "CoWeSe", sentences(f"texto{n}", 6)) for n in range(3)]
            input_path.write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")
            args = ["--mode", "audit", "--input-file", str(input_path),
                    "--output-dir", str(output_dir), "--batch-size", "2",
                    "--progress-every", "0", "--heartbeat-seconds", "0"]
            original = workflow.tokenize
            seen = 0

            def interrupted(text):
                nonlocal seen
                seen += 1
                if seen == 3:
                    raise RuntimeError("interrupción simulada")
                return original(text)

            workflow.tokenize = interrupted
            try:
                with self.assertRaisesRegex(RuntimeError, "interrupción simulada"):
                    workflow.main(args)
            finally:
                workflow.tokenize = original
            with closing(sqlite3.connect(output_dir / "dedup_estado.sqlite3")) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM records").fetchone()[0], 2)
            self.assertEqual(workflow.main([*args, "--resume"]), 0)
            with closing(sqlite3.connect(output_dir / "dedup_estado.sqlite3")) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM records").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
STEP3_DIR = REPO_ROOT / "Procesamiento" / "3 - Eliminar ruido"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


core = load_module("ruido_core", STEP3_DIR / "ruido_core.py")
workflow = load_module("eliminar_ruido_paso_3", STEP3_DIR / "3_Eliminar_ruido.py")


def context(text: str, lookup=None, thresholds=None, total_documents=10):
    return core.DetectionContext(
        record_number=1,
        record={"id": "r1", "texto": text},
        total_documents=total_documents,
        thresholds=thresholds or core.Thresholds(),
        global_block_lookup=lookup or (lambda _block_hash: {}),
    )


class MetricsAndDetectorTests(unittest.TestCase):
    def test_empty_metrics_are_safe(self):
        metrics = core.text_metrics("")
        self.assertEqual(metrics["word_count"], 0)
        self.assertEqual(metrics["digit_ratio"], 0)
        self.assertEqual(core.line_length_metrics("", 45)["line_length_stddev"], 0)

    def test_navigation_requires_combined_signals(self):
        text = "Inicio\nBuscar\nContacto\nhttps://example.com\nSiguiente\n\nTexto biomédico narrativo."
        self.assertTrue(core.NavigationResidualDetector().detect(text, context(text)))
        negative = "El contacto celular se evaluó al inicio del tratamiento en pacientes."
        self.assertFalse(core.NavigationResidualDetector().detect(negative, context(negative)))

    def test_cookie_notice_does_not_trigger_on_isolated_word(self):
        positive = "Utilizamos cookies para mejorar el sitio. Aceptar todas o administrar preferencias."
        self.assertTrue(core.CookieNoticeDetector().detect(positive, context(positive)))
        negative = "El estudio analizó el uso de cookies como ejemplo de seguimiento digital."
        self.assertFalse(core.CookieNoticeDetector().detect(negative, context(negative)))

    def test_repeated_header_uses_global_frequency_and_edge(self):
        text = "Inicio | Contacto | Todos los derechos reservados\n\nArtículo clínico extenso."
        lookup = lambda _hash: {
            "document_count": 8,
            "start_count": 8,
            "end_count": 0,
            "middle_count": 0,
            "origin_counts": {"A": 5, "B": 3},
        }
        results = core.RepeatedHeaderFooterDetector().detect(
            text, context(text, lookup=lookup, total_documents=10)
        )
        self.assertTrue(results)
        self.assertEqual(results[0].metrics["global_document_frequency"], 8)
        biomedical_sentence = "No se observaron diferencias significativas entre los grupos."
        self.assertFalse(
            core.RepeatedHeaderFooterDetector().detect(
                biomedical_sentence,
                context(biomedical_sentence, lookup=lookup, total_documents=10),
            )
        )

    def test_link_list_protects_scientific_references(self):
        positive = "Inicio https://a.com\nMás https://b.com\nSiguiente https://c.com"
        self.assertTrue(core.LinkListWithoutContentDetector().detect(positive, context(positive)))
        references = (
            "Referencias\nGarcía et al. (2020). doi:10.1000/a https://doi.org/10.1000/a\n"
            "PMID: 123 https://pubmed.ncbi.nlm.nih.gov/123\n"
            "López et al. (2021). https://example.org/paper"
        )
        self.assertFalse(core.LinkListWithoutContentDetector().detect(references, context(references)))

    def test_advertising_requires_context(self):
        positive = "Contenido patrocinado. Comprar ahora con 20% de descuento: https://shop.com"
        self.assertTrue(core.AdvertisingDetector().detect(positive, context(positive)))
        negative = "El análisis de costo-efectividad mostró un descuento del 20% en costos sanitarios."
        self.assertFalse(core.AdvertisingDetector().detect(negative, context(negative)))

    def test_http_error_requires_code_phrase_and_short_document(self):
        positive = "Error 404. Página no encontrada. The requested file could not be found."
        results = core.HttpErrorPageDetector().detect(positive, context(positive))
        self.assertEqual(results[0].proposed_action, core.DELETE_RECORD)
        negative = (
            "En este estudio se modeló el error 404 como identificador de una respuesta. "
            + "Los pacientes y resultados clínicos fueron analizados. " * 30
        )
        self.assertFalse(core.HttpErrorPageDetector().detect(negative, context(negative)))

    def test_repeated_characters_have_category_thresholds(self):
        positive = "Texto " + "x" * 14 + " final"
        self.assertTrue(core.RepeatedCharactersDetector().detect(positive, context(positive)))
        self.assertFalse(core.RepeatedCharactersDetector().detect("Resultado... normal", context("Resultado... normal")))

    def test_scraped_code_requires_multiple_syntax_signals(self):
        positive = "const x = 1;\nlet y = 2;\nfunction run() { document.querySelector('p'); return x; }"
        self.assertTrue(core.ScrapedCodeDetector().detect(positive, context(positive)))
        negative = "La función de retorno de la clase terapéutica fue evaluada en el estudio."
        self.assertFalse(core.ScrapedCodeDetector().detect(negative, context(negative)))

    def test_near_empty_protects_short_biomedical_titles(self):
        self.assertTrue(core.NearEmptyDocumentDetector().detect("---", context("---")))
        title = "Diabetes mellitus"
        self.assertFalse(core.NearEmptyDocumentDetector().detect(title, context(title)))

    def test_automatic_index_uses_variance_only_as_one_signal(self):
        positive = (
            "Índice\nIntroducción ........ 1\nMétodos ............. 2\n"
            "Resultados .......... 3\nConclusiones ......... 4"
        )
        self.assertTrue(core.AutomaticIndexDetector().detect(positive, context(positive)))
        negative = "Dosis 1\nDosis 2\nDosis 3\nDosis 4\nDosis 5"
        self.assertFalse(core.AutomaticIndexDetector().detect(negative, context(negative)))

    def test_internal_repetition_preserves_first_occurrence(self):
        block = "Este bloque narrativo se repitió por un error de raspado."
        text = f"{block}\n\n{block}\n\n{block}"
        results = core.ExcessiveInternalRepetitionDetector().detect(text, context(text))
        self.assertEqual(len(results), 2)
        self.assertTrue(all(result.start > 0 for result in results))
        self.assertEqual(results[0].metrics["first_occurrence_start"], 0)

    def test_consolidation_merges_overlap_but_not_different_actions(self):
        one = core.DetectorResult("A", "A", "bloque", 0, 10, 0.7, {"a": 1}, core.DELETE_SPAN)
        two = core.DetectorResult("B", "B", "fragmento", 5, 15, 0.9, {"b": 2}, core.DELETE_SPAN)
        full = core.DetectorResult("C", "C", "registro", 0, 20, 0.8, {}, core.DELETE_RECORD)
        consolidated = core.consolidate_results([one, two, full])
        self.assertEqual(len(consolidated), 2)
        span = next(item for item in consolidated if item.proposed_action == core.DELETE_SPAN)
        self.assertEqual((span.start, span.end), (0, 15))
        self.assertEqual(span.reason_codes, ("A", "B"))

    def test_candidate_id_is_stable(self):
        kwargs = {
            "input_sha256": "a" * 64,
            "stable_record_key": "record",
            "reason_codes": ("B", "A"),
            "start": 1,
            "end": 4,
            "fragment_hash": "b" * 64,
        }
        first = core.build_candidate_id(**kwargs)
        self.assertEqual(first, core.build_candidate_id(**kwargs))
        kwargs["reason_codes"] = ("A", "B")
        self.assertEqual(first, core.build_candidate_id(**kwargs))


class SamplingAndWorkflowTests(unittest.TestCase):
    def test_balanced_quota_is_without_replacement(self):
        quotas = workflow.allocate_balanced_quotas({"A": 10, "B": 2, "C": 1}, 8)
        self.assertEqual(sum(quotas.values()), 8)
        self.assertEqual(quotas["C"], 1)
        self.assertEqual(quotas["B"], 2)

    def test_audit_then_apply_only_explicit_deletion(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "normalizado.jsonl"
            output_dir = root / "salida"
            records = [
                {
                    "id": "cookie",
                    "texto": "Utilizamos cookies para mejorar el sitio. Aceptar todas o administrar preferencias.\n\nArtículo clínico conservado.",
                    "base_de_datos_origen": "A",
                    "archivo_origen": "a.txt",
                    "ruta_relativa_origen": "a.txt",
                    "meta": {"preservar": True},
                    "procesamiento": ["Paso 1", "Paso 2: Normalizacion"],
                },
                {
                    "id": "normal",
                    "texto": "Este artículo biomédico describe pacientes, métodos y resultados clínicos.",
                    "base_de_datos_origen": "A",
                    "archivo_origen": "b.txt",
                    "ruta_relativa_origen": "b.txt",
                    "procesamiento": ["Paso 1", "Paso 2: Normalizacion"],
                },
                {
                    "id": "error-conservar",
                    "texto": "Error 404. Página no encontrada. The requested file could not be found.",
                    "base_de_datos_origen": "B",
                    "archivo_origen": "c.txt",
                    "ruta_relativa_origen": "c.txt",
                    "procesamiento": ["Paso 1", "Paso 2: Normalizacion"],
                },
                {
                    "id": "error-eliminar",
                    "texto": "Error 404. Página no encontrada. The requested file could not be found.",
                    "base_de_datos_origen": "B",
                    "archivo_origen": "d.txt",
                    "ruta_relativa_origen": "d.txt",
                    "procesamiento": ["Paso 1", "Paso 2: Normalizacion"],
                },
                {
                    "id": "pendiente",
                    "texto": "Contenido patrocinado. Comprar ahora con 20% de descuento: https://shop.com",
                    "base_de_datos_origen": "C",
                    "archivo_origen": "e.txt",
                    "ruta_relativa_origen": "e.txt",
                    "procesamiento": ["Paso 1", "Paso 2: Normalizacion"],
                },
            ]
            input_path.write_text(
                "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
                encoding="utf-8",
                newline="\n",
            )
            audit_paths = workflow.AuditPaths(
                candidates_csv=output_dir / "candidatos_ruido.csv",
                sample_csv=output_dir / "muestra_revision_ruido.csv",
                summary_json=output_dir / "auditoria_resumen.json",
                checkpoint_json=output_dir / "auditoria.checkpoint.json",
                state_db=output_dir / "auditoria_global.sqlite3",
            )
            summary = workflow.run_audit(
                input_path=input_path,
                paths=audit_paths,
                thresholds=core.Thresholds(repeated_header_min_docs=2, repeated_header_min_ratio=0),
                disabled_detectors=(),
                review_sample_size=20,
                sampling_seed=7,
                batch_size=1,
                max_batch_bytes=4096,
                progress_every=0,
                resume=False,
                overwrite=False,
            )
            self.assertEqual(summary["total_registros_procesados"], 5)
            original_after_audit = [json.loads(line) for line in input_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(original_after_audit, records)
            resumed_audit = workflow.run_audit(
                input_path=input_path,
                paths=audit_paths,
                thresholds=core.Thresholds(repeated_header_min_docs=2, repeated_header_min_ratio=0),
                disabled_detectors=(),
                review_sample_size=20,
                sampling_seed=7,
                batch_size=9,
                max_batch_bytes=99999,
                progress_every=0,
                resume=True,
                overwrite=False,
            )
            self.assertEqual(resumed_audit["total_candidatos"], summary["total_candidatos"])
            with audit_paths.candidates_csv.open("r", encoding="utf-8-sig", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertGreaterEqual(len(rows), 2)
            cookie_row = next(row for row in rows if "COOKIE_NOTICE" in row["codigos_motivo"])
            cookie_row["decision"] = "eliminar"
            error_keep = next(row for row in rows if row["id"] == "error-conservar")
            error_keep["decision"] = "conservar"
            error_delete = next(row for row in rows if row["id"] == "error-eliminar")
            error_delete["decision"] = "eliminar"
            with audit_paths.candidates_csv.open("w", encoding="utf-8-sig", newline="") as destination:
                writer = csv.DictWriter(destination, fieldnames=workflow.CSV_COLUMNS, lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)

            apply_paths = workflow.ApplyPaths(
                output_jsonl=output_dir / "sin_ruido.jsonl",
                applied_csv=output_dir / "eliminaciones_aplicadas.csv",
                summary_json=output_dir / "aplicacion_resumen.json",
                checkpoint_json=output_dir / "aplicacion.checkpoint.json",
                state_db=output_dir / "aplicacion_decisiones.sqlite3",
            )
            apply_summary = workflow.run_apply(
                input_path=input_path,
                review_csv=audit_paths.candidates_csv,
                paths=apply_paths,
                batch_size=1,
                max_batch_bytes=4096,
                progress_every=0,
                resume=False,
                overwrite=False,
            )
            self.assertEqual(apply_summary["decisiones_eliminar"], 2)
            self.assertEqual(apply_summary["candidatos_conservar"], 1)
            self.assertGreater(apply_summary["candidatos_pendientes"], 0)
            output_records = [
                json.loads(line)
                for line in apply_paths.output_jsonl.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(output_records), 4)
            self.assertNotIn("error-eliminar", {record["id"] for record in output_records})
            self.assertIn("error-conservar", {record["id"] for record in output_records})
            cookie_output = next(record for record in output_records if record["id"] == "cookie")
            self.assertNotIn("cookies", cookie_output["texto"].casefold())
            self.assertIn("Artículo clínico conservado", cookie_output["texto"])
            self.assertEqual(cookie_output["meta"], {"preservar": True})
            self.assertTrue(
                all(record["procesamiento"].count(workflow.PASO_PROCESAMIENTO) == 1 for record in output_records)
            )
            with apply_paths.applied_csv.open("r", encoding="utf-8-sig", newline="") as applied_file:
                applied_rows = list(csv.DictReader(applied_file))
            self.assertEqual(len(applied_rows), 2)
            self.assertTrue(all(row["decision"] == "eliminar" for row in applied_rows))
            full_applied = next(row for row in applied_rows if row["id"] == "error-eliminar")
            self.assertEqual(full_applied["resultado_aplicacion"], "eliminacion_registro_aplicada")
            self.assertIn("Página no encontrada", full_applied["texto_detectado"])

            resumed = workflow.run_apply(
                input_path=input_path,
                review_csv=audit_paths.candidates_csv,
                paths=apply_paths,
                batch_size=9,
                max_batch_bytes=99999,
                progress_every=0,
                resume=True,
                overwrite=False,
            )
            self.assertEqual(resumed["candidatos_aplicados"], 2)

    def test_review_import_rejects_unknown_duplicate_and_stale_candidates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            record = {
                "id": "r1",
                "texto": "Bloque de ruido verificable.",
                "base_de_datos_origen": "A",
                "archivo_origen": "a.txt",
                "ruta_relativa_origen": "a.txt",
                "procesamiento": ["Paso 1", "Paso 2: Normalizacion"],
            }
            candidate = core.ConsolidatedResult(
                reason_codes=("TEST_REASON",),
                detector_names=("Prueba",),
                level="bloque",
                start=0,
                end=len(record["texto"]),
                confidence=0.9,
                metrics={"signal_count": 3},
                proposed_action=core.DELETE_SPAN,
            )
            input_sha = "a" * 64
            row = workflow.candidate_row(
                candidate=candidate,
                record=record,
                record_number=1,
                input_sha256=input_sha,
            )

            def write_rows(path, rows):
                with path.open("w", encoding="utf-8-sig", newline="") as output_file:
                    writer = csv.DictWriter(
                        output_file, fieldnames=workflow.CSV_COLUMNS, lineterminator="\n"
                    )
                    writer.writeheader()
                    writer.writerows(rows)

            unknown_path = root / "unknown.csv"
            unknown_row = dict(row)
            unknown_row["decision"] = "tal vez"
            write_rows(unknown_path, [unknown_row])
            unknown_connection = workflow.connect_sqlite(root / "unknown.sqlite3")
            workflow.initialize_application_database(unknown_connection)
            try:
                with self.assertRaisesRegex(ValueError, "Decision desconocida"):
                    workflow.import_review_csv(
                        unknown_connection, unknown_path, input_sha256=input_sha
                    )
            finally:
                unknown_connection.close()

            duplicate_path = root / "duplicate.csv"
            write_rows(duplicate_path, [row, row])
            duplicate_connection = workflow.connect_sqlite(root / "duplicate.sqlite3")
            workflow.initialize_application_database(duplicate_connection)
            try:
                with self.assertRaisesRegex(ValueError, "duplicado"):
                    workflow.import_review_csv(
                        duplicate_connection, duplicate_path, input_sha256=input_sha
                    )
            finally:
                duplicate_connection.close()

            valid_path = root / "valid.csv"
            write_rows(valid_path, [row])
            valid_connection = workflow.connect_sqlite(root / "valid.sqlite3")
            workflow.initialize_application_database(valid_connection)
            try:
                workflow.import_review_csv(valid_connection, valid_path, input_sha256=input_sha)
                stored = valid_connection.execute(
                    "SELECT * FROM review_candidates"
                ).fetchone()
                stale_record = dict(record)
                stale_record["texto"] += " cambio"
                with self.assertRaisesRegex(ValueError, "obsoleto"):
                    workflow.verify_candidate_row(
                        stored, stale_record, 1, input_sha
                    )
            finally:
                valid_connection.close()


if __name__ == "__main__":
    unittest.main()

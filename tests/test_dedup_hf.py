from __future__ import annotations

from contextlib import closing
import importlib.util
import json
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
STEP = ROOT / "Procesamiento" / "4 - Dedup interna"
sys.path.insert(0, str(STEP))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


workflow = load_module("dedup_step4_cloud_tests", STEP / "4_Dedup_interna.py")
hf_runner = load_module("dedup_hf_tests", STEP / "hf" / "dedup_hf.py")


def record(number: int) -> dict:
    text = " ".join(f"palabra{index}" for index in range(120))
    return {
        "id": f"registro-{number}",
        "texto": text,
        "base_de_datos_origen": "CoWeSe" if number == 1 else "MMedC",
        "procesamiento": ["Paso 3: Eliminacion de ruido"],
    }


def write_input(path: Path) -> None:
    path.write_text(
        "".join(json.dumps(record(number), ensure_ascii=False) + "\n" for number in (1, 2)),
        encoding="utf-8",
    )


class DedupCloudTests(unittest.TestCase):
    def test_repository_root_supports_flat_hf_mount(self):
        mounted = PurePosixPath("/app/4_Dedup_interna.py")
        local = PurePosixPath("/repo/Procesamiento/4 - Dedup interna/4_Dedup_interna.py")
        self.assertEqual(workflow.repository_root(mounted), PurePosixPath("/app"))
        self.assertEqual(workflow.repository_root(local), PurePosixPath("/repo"))
    def test_resume_accepts_same_input_from_a_different_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "entrada-windows.jsonl"
            second = root / "entrada-linux.jsonl"
            output = root / "output"
            state = root / "state"
            write_input(first)
            args = [
                "--mode", "audit", "--output-dir", str(output),
                "--state-dir", str(state), "--max-records", "2",
                "--progress-every", "0", "--heartbeat-seconds", "0",
            ]
            self.assertEqual(workflow.main([*args, "--input-file", str(first)]), 0)
            first.rename(second)
            self.assertEqual(
                workflow.main([*args, "--input-file", str(second), "--resume"]),
                0,
            )
            with closing(sqlite3.connect(state / "dedup_estado.sqlite3")) as connection:
                metadata = dict(connection.execute("SELECT key,value FROM meta"))
            identity = json.loads(metadata["input_identity"])
            self.assertEqual(identity["version"], 2)
            self.assertNotIn("mtime_ns", identity)
            self.assertEqual(json.loads(metadata["input_path"]), str(second.resolve()))

    def test_hf_runner_restores_a_consistent_remote_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "input.jsonl"
            output = root / "bucket" / "runs" / "pilot"
            checkpoints = root / "bucket" / "checkpoints" / "pilot"
            scratch = root / "scratch"
            write_input(input_path)
            args = [
                "--input-file", str(input_path),
                "--output-dir", str(output),
                "--checkpoint-dir", str(checkpoints),
                "--scratch-dir", str(scratch),
                "--max-records", "2",
                "--batch-size", "1",
                "--progress-every", "0",
                "--heartbeat-seconds", "0",
                "--checkpoint-seconds", "3600",
                "--resume",
            ]
            self.assertEqual(hf_runner.run(args), 0)
            latest = checkpoints / "latest.sqlite3"
            self.assertTrue(latest.is_file())
            self.assertTrue((checkpoints / "latest.json").is_file())
            self.assertFalse(any(path.name.startswith(".latest") for path in checkpoints.iterdir()))
            with closing(sqlite3.connect(latest)) as connection:
                self.assertEqual(connection.execute("PRAGMA quick_check").fetchone()[0], "ok")
                phase = json.loads(connection.execute(
                    "SELECT value FROM meta WHERE key='phase'"
                ).fetchone()[0])
            self.assertEqual(phase, "exported")

            shutil.rmtree(scratch)
            self.assertEqual(hf_runner.run(args), 0)
            self.assertTrue((scratch / "state" / "dedup_estado.sqlite3").is_file())
            status = json.loads((output / "run.json").read_text(encoding="utf-8"))
            self.assertEqual(status["status"], "completed")


if __name__ == "__main__":
    unittest.main()

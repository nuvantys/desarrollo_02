from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from contifico_pg_backfill import DEFAULT_DB_NAME, open_connection, pg_config_from_env
from export_dashboard_data import base_metadata, build_technical, filters_available, json_default


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = ROOT_DIR / "dashboard" / "data"
DEFAULT_REPORT_PATH = ROOT_DIR / "final_report.md"
API_STATE_LOCK = threading.Lock()
UTC = dt.timezone.utc


def iso_now() -> str:
    return dt.datetime.now(UTC).isoformat(timespec="seconds")


def read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def redact_text(text: str) -> str:
    redacted = text
    for secret_name in ("CONTIFICO_AUTHORIZATION", "PGPASSWORD"):
        secret = os.getenv(secret_name)
        if secret:
            redacted = redacted.replace(secret, f"[redacted:{secret_name.lower()}]")
    return redacted.strip()


class RefreshManager:
    def __init__(self, db_name: str, data_dir: Path, report_path: Path, base_url: str, max_workers: int) -> None:
        self.db_name = db_name
        self.data_dir = data_dir
        self.report_path = report_path
        self.base_url = base_url
        self.max_workers = max_workers
        self.current_job: dict[str, Any] | None = None
        self.last_job: dict[str, Any] | None = None

    def _read_technical_snapshot(self) -> dict[str, Any]:
        technical_path = self.data_dir / "technical.json"
        snapshot = read_json_file(technical_path)
        if snapshot is not None:
            return snapshot
        config = pg_config_from_env(self.db_name)
        with open_connection(config, config.db_name) as conn:
            meta = base_metadata(conn)
            filters = filters_available(conn)
            return build_technical(conn, meta, filters)

    def _status_payload(self) -> dict[str, Any]:
        snapshot = self._read_technical_snapshot()
        with API_STATE_LOCK:
            current_job = dict(self.current_job) if self.current_job else None
            last_job = dict(self.last_job) if self.last_job else None
        return {
            "api_available": True,
            "runtime": {
                "current_job": current_job,
                "last_job": last_job,
            },
            "technical": snapshot,
        }

    def get_status(self) -> dict[str, Any]:
        return self._status_payload()

    def get_job(self, job_id: str) -> dict[str, Any]:
        with API_STATE_LOCK:
            for candidate in (self.current_job, self.last_job):
                if candidate and candidate.get("job_id") == job_id:
                    return {
                        "api_available": True,
                        "job": dict(candidate),
                    }
        raise KeyError(job_id)

    def reload_status(self) -> dict[str, Any]:
        return self._status_payload()

    def start_refresh(self) -> dict[str, Any]:
        with API_STATE_LOCK:
            if self.current_job and self.current_job.get("status") == "running":
                raise RuntimeError("Ya existe una actualizacion en curso")
            job_id = dt.datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            self.current_job = {
                "job_id": job_id,
                "status": "running",
                "scope": "refresh_plus_snapshot",
                "stage": "extrayendo",
                "started_at": iso_now(),
                "finished_at": None,
                "duration_seconds": None,
                "message": "Inicializando actualizacion rapida de Contifico.",
                "error_text": None,
                "logs": [],
            }
            worker = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
            worker.start()
            return dict(self.current_job)

    def _append_log(self, job_id: str, line: str) -> None:
        clean = redact_text(line)
        if not clean:
            return
        with API_STATE_LOCK:
            if not self.current_job or self.current_job.get("job_id") != job_id:
                return
            logs = self.current_job.setdefault("logs", [])
            logs.append(clean)
            if len(logs) > 80:
                del logs[:-80]

    def _update_job(self, job_id: str, **changes: Any) -> None:
        with API_STATE_LOCK:
            if not self.current_job or self.current_job.get("job_id") != job_id:
                return
            self.current_job.update(changes)

    def _finish_job(self, job_id: str, status: str, message: str, error_text: str | None = None) -> None:
        with API_STATE_LOCK:
            if not self.current_job or self.current_job.get("job_id") != job_id:
                return
            started_at = dt.datetime.fromisoformat(self.current_job["started_at"])
            finished_at = dt.datetime.now(UTC)
            self.current_job.update(
                {
                    "status": status,
                    "stage": "finalizado" if status == "success" else "error",
                    "finished_at": finished_at.isoformat(timespec="seconds"),
                    "duration_seconds": int((finished_at - started_at).total_seconds()),
                    "message": message,
                    "error_text": error_text,
                }
            )
            self.last_job = dict(self.current_job)
            self.current_job = None

    def _stage_from_line(self, line: str, current_stage: str) -> tuple[str, str]:
        lowered = line.lower()
        if "validating api access" in lowered:
            return "extrayendo", "Validando acceso a Contifico."
        if "ensuring postgresql schema" in lowered or "truncating previous backfill data" in lowered:
            return "normalizando", "Preparando esquema y limpiando el backfill previo."
        if "loading " in lowered and " into postgresql" in lowered:
            return "cargando PostgreSQL", line.strip()
        if "refreshing watermarks and reporting views" in lowered:
            return "cargando PostgreSQL", "Refrescando watermarks y vistas de reporte."
        if "final report generated" in lowered:
            return current_stage, "Backfill finalizado. Preparando regeneracion del snapshot."
        return current_stage, line.strip()

    def _run_process(self, job_id: str, command: list[str], cwd: Path, initial_stage: str, initial_message: str) -> None:
        self._update_job(job_id, stage=initial_stage, message=initial_message)
        env = os.environ.copy()
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        current_stage = initial_stage
        assert process.stdout is not None
        for line in process.stdout:
            self._append_log(job_id, line)
            current_stage, message = self._stage_from_line(line, current_stage)
            self._update_job(job_id, stage=current_stage, message=message)
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f"El proceso fallo con codigo {return_code}: {' '.join(command)}")

    def _run_job(self, job_id: str) -> None:
        python_executable = sys.executable
        try:
            backfill_command = [
                python_executable,
                str(ROOT_DIR / "contifico_pg_backfill.py"),
                "--mode",
                "refresh",
                "--db-name",
                self.db_name,
                "--report-out",
                str(self.report_path),
                "--base-url",
                self.base_url,
                "--max-workers",
                str(self.max_workers),
            ]
            export_command = [
                python_executable,
                str(ROOT_DIR / "export_dashboard_data.py"),
                "--db-name",
                self.db_name,
                "--out-dir",
                str(self.data_dir),
            ]
            self._run_process(job_id, backfill_command, ROOT_DIR, "extrayendo", "Iniciando refresh rapido en PostgreSQL.")
            self._run_process(job_id, export_command, ROOT_DIR, "regenerando snapshot", "Regenerando snapshot analitico para el dashboard.")
            self._finish_job(job_id, "success", "Actualizacion finalizada correctamente.")
        except Exception as exc:
            self._append_log(job_id, str(exc))
            self._finish_job(job_id, "error", "La actualizacion tecnica fallo.", redact_text(str(exc)))


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2, default=json_default).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()
    handler.wfile.write(body)


def build_handler(manager: RefreshManager):
    class LocalDashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/technical/status":
                    json_response(self, HTTPStatus.OK, manager.get_status())
                    return
                if parsed.path.startswith("/api/technical/refresh/"):
                    job_id = parsed.path.rsplit("/", 1)[-1]
                    json_response(self, HTTPStatus.OK, manager.get_job(job_id))
                    return
                json_response(self, HTTPStatus.NOT_FOUND, {"error": "Ruta no encontrada"})
            except KeyError:
                json_response(self, HTTPStatus.NOT_FOUND, {"error": "Job no encontrado"})
            except Exception as exc:
                json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": redact_text(str(exc))})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/technical/refresh":
                    json_response(self, HTTPStatus.ACCEPTED, {"job": manager.start_refresh()})
                    return
                if parsed.path == "/api/technical/reload":
                    json_response(self, HTTPStatus.OK, manager.reload_status())
                    return
                json_response(self, HTTPStatus.NOT_FOUND, {"error": "Ruta no encontrada"})
            except RuntimeError as exc:
                json_response(self, HTTPStatus.CONFLICT, {"error": redact_text(str(exc)), "runtime": manager.get_status()["runtime"]})
            except Exception as exc:
                json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": redact_text(str(exc))})

    return LocalDashboardHandler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local API for the Contifico technical dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8130)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--report-out", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--base-url", default="https://api.contifico.com")
    parser.add_argument("--max-workers", type=int, default=6)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manager = RefreshManager(
        db_name=args.db_name,
        data_dir=Path(args.data_dir).resolve(),
        report_path=Path(args.report_out).resolve(),
        base_url=args.base_url,
        max_workers=args.max_workers,
    )
    server = ThreadingHTTPServer((args.host, args.port), build_handler(manager))
    print(f"Local dashboard API running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

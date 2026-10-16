"""Local server for the dashboard: runs the dashboard Lambda's code on this machine.

On AWS the dashboard API is the Lambda in ``backend/talent-dashboard``. This
script serves the same routes from the same code, for local development:
``LAKE_ROOT`` is the local lake by default, or an ``s3://`` URI to read the
data the cloud pipeline wrote. Run ``job.py`` first: it writes the dashboard
data (``serving/dashboard/``) next to Gold.

    .venv/bin/python dashboard/api.py        # http://127.0.0.1:8510/api/talent-dashboard/summary
"""

from __future__ import annotations

import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

JOB_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = JOB_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend" / "talent-dashboard"))
import function  # noqa: E402  - the dashboard Lambda

log = logging.getLogger("dashboard")


def make_handler(store) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - name required by BaseHTTPRequestHandler
            try:
                status, body = function.route(store, self.path)
            except Exception as exc:  # noqa: BLE001 - report any read error to the page
                log.exception("request failed: %s", self.path)
                status, body = 500, function._json({"error": f"{type(exc).__name__}: {exc}"})
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            log.info("%s %s", self.address_string(), fmt % args)

    return Handler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = function.open_store(os.environ.get("LAKE_ROOT") or str(JOB_DIR / "lake"))
    port = int(os.environ.get("DASHBOARD_PORT", "8510"))
    log.info("serving dashboard data from %s", store.root)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(store))
    log.info("API on http://127.0.0.1:%d/api/talent-dashboard/source", port)
    server.serve_forever()


if __name__ == "__main__":
    main()

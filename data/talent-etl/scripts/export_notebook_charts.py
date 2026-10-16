"""Copy the insights notebook's charts into the React dashboard.

Reads the executed notebook (``notebooks/talent_insights.ipynb``), takes every
chart under each "## Qn." section, and writes them to
``frontend/public/charts/`` as ``q1-1.png``, ``q1-2.png``, ... plus
``charts.json`` with each chart's titles (used as captions and alt text).

Rerun after re-executing the notebook, then redeploy the frontend:

    .venv/bin/python scripts/export_notebook_charts.py
"""

import base64
import json
import re
import sys
from pathlib import Path

JOB_DIR = Path(__file__).resolve().parent.parent
NOTEBOOK = JOB_DIR / "notebooks" / "talent_insights.ipynb"
TARGET = JOB_DIR.parent.parent / "frontend" / "public" / "charts"
TITLE_RE = re.compile(r'titled\(\s*\w+\s*,\s*"([^"]+)"')


def main() -> int:
    nb = json.loads(NOTEBOOK.read_text())
    TARGET.mkdir(parents=True, exist_ok=True)
    for old in TARGET.glob("q*-*.png"):
        old.unlink()

    question, charts = None, {}
    for cell in nb["cells"]:
        source = "".join(cell["source"])
        if cell["cell_type"] == "markdown":
            heading = re.match(r"^## (Q\d)\.", source)
            question = heading.group(1).lower() if heading else (None if source.startswith("## ") else question)
            continue
        if not question:
            continue
        for output in cell.get("outputs", []):
            png = output.get("data", {}).get("image/png")
            if not png:
                continue
            items = charts.setdefault(question, [])
            name = f"{question}-{len(items) + 1}.png"
            (TARGET / name).write_bytes(base64.b64decode(png))
            items.append({"file": f"charts/{name}", "titles": TITLE_RE.findall(source)})

    if not charts:
        print(f"no charts found in {NOTEBOOK} - run the notebook first", file=sys.stderr)
        return 1
    (TARGET / "charts.json").write_text(json.dumps(charts, indent=2) + "\n")
    print(f"wrote {sum(len(v) for v in charts.values())} charts for {', '.join(charts)} to {TARGET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

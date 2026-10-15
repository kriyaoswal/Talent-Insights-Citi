"""Smoke test: the insights notebook runs top to bottom against the Gold layer.

Skipped when there is no Gold layer yet (run job.py first) or the
"talent-etl" Jupyter kernel is not registered (see README, Notebook).
"""

from pathlib import Path

import pytest

nbformat = pytest.importorskip("nbformat")
nbclient = pytest.importorskip("nbclient")
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel  # noqa: E402

JOB_DIR = Path(__file__).resolve().parents[1]
NOTEBOOK = JOB_DIR / "notebooks" / "talent_insights.ipynb"


def test_notebook_executes_without_errors():
    if not (JOB_DIR / "lake" / "gold").exists():
        pytest.skip("no Gold layer under lake/ - run job.py first")
    try:
        KernelSpecManager().get_kernel_spec("talent-etl")
    except NoSuchKernel:
        pytest.skip("Jupyter kernel 'talent-etl' not registered")

    nb = nbformat.read(NOTEBOOK, as_version=4)
    nbclient.NotebookClient(
        nb, kernel_name="talent-etl", timeout=600, resources={"metadata": {"path": str(NOTEBOOK.parent)}}
    ).execute()  # raises CellExecutionError on the first failing cell

    outputs = [o for c in nb.cells if c.cell_type == "code" for o in c.get("outputs", [])]
    warnings = [o["text"].strip() for o in outputs if o.get("name") == "stderr"]
    assert not warnings, f"notebook printed warnings: {warnings}"
    charts = sum("image/png" in o.get("data", {}) for o in outputs)
    text = "\n".join(o.get("data", {}).get("text/markdown", "") for o in outputs)
    assert charts >= 14
    for question in ("Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7"):
        assert f"| {question} |" in text, f"executive summary is missing {question}"

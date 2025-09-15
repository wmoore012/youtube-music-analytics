import json
import os
from pathlib import Path

import nbformat

from tools.run_notebooks import execute_notebook


def _make_min_notebook(path: Path) -> None:
    nb = nbformat.v4.new_notebook()
    nb.cells = [
        nbformat.v4.new_markdown_cell("# Smoke Test Notebook"),
        nbformat.v4.new_code_cell("x = 1 + 1\nprint(x)"),
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        nbformat.write(nb, fh)


def test_execute_notebook_writes_outputs(tmp_path):
    nb_path = tmp_path / "mini.ipynb"
    _make_min_notebook(nb_path)

    out_dir = tmp_path / "executed"
    os.environ["NOTEBOOK_EXECUTE"] = "0"  # disable kernel in sandbox
    res = execute_notebook(str(nb_path), str(out_dir))

    assert Path(res["executed_path"]).exists()
    assert Path(res["summary_path"]).exists()

    # Basic sanity on executed output
    with open(res["executed_path"], "r", encoding="utf-8") as fh:
        nb2 = nbformat.read(fh, as_version=4)
    # Even without execution, the executed notebook should be valid JSON
    assert any(c.cell_type == "code" for c in nb2.cells)

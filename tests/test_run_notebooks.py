import json
import os
from pathlib import Path

import nbformat
import pytest

from tests.test_notebook_execution_robust import RobustNotebookTester
from tools.development.run_notebooks import execute_notebook


def _make_min_notebook(path: Path) -> None:
    nb = nbformat.v4.new_notebook()
    nb.cells = [
        nbformat.v4.new_markdown_cell("# Smoke Test Notebook"),
        nbformat.v4.new_code_cell("x = 1 + 1\nprint(x)"),
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf - 8") as fh:
        nbformat.write(nb, fh)


def test_execute_notebook_writes_outputs(tmp_path):
    """Basic smoke test for notebook execution."""
    nb_path = tmp_path / "mini.ipynb"
    _make_min_notebook(nb_path)

    out_dir = tmp_path / "executed"
    os.environ["NOTEBOOK_EXECUTE"] = "0"  # disable kernel in sandbox
    res = execute_notebook(str(nb_path), str(out_dir))

    assert Path(res["executed_path"]).exists()
    assert Path(res["summary_path"]).exists()

    # Basic sanity on executed output
    with open(res["executed_path"], "r", encoding="utf - 8") as fh:
        nb2 = nbformat.read(fh, as_version=4)
    # Even without execution, the executed notebook should be valid JSON
    assert any(c.cell_type == "code" for c in nb2.cells)


def test_robust_notebook_execution_integration():
    """Test integration with robust notebook execution system."""
    # Find actual notebooks to test
    notebooks_dir = Path("notebooks")

    if not notebooks_dir.exists():
        pytest.skip("No notebooks directory found")

    # Find current notebooks (not executed versions)
    current_notebooks = [
        f
        for f in notebooks_dir.glob("*.ipynb")
        if not f.name.endswith("-executed.ipynb") and not f.name.startswith(".") and "archive" not in str(f)
    ]

    if not current_notebooks:
        pytest.skip("No current notebooks found to test")

    # Test with robust tester
    tester = RobustNotebookTester(timeout=120)  # Shorter timeout for tests

    # Test at least one notebook
    test_notebook = current_notebooks[0]
    print(f"\n🧪 Testing notebook execution: {test_notebook.name}")

    try:
        results = tester.execute_notebook_with_validation(str(test_notebook))

        # Assert basic execution success
        assert results["execution_successful"], f"Notebook execution failed: {test_notebook.name}"

        # Assert reasonable execution time
        assert results["execution_time"] < 300, f"Notebook took too long: {results['execution_time']:.2f}s"

        # Assert validation passed
        assert results["validation_results"].is_valid, f"Validation failed: {results['validation_results'].errors}"

        # Assert outputs are readable
        assert results["readability_results"].is_valid, f"Readability failed: {results['readability_results'].errors}"

        print(f"✅ {test_notebook.name} executed successfully in {results['execution_time']:.2f}s")

        # Check for charts if present
        chart_count = results["chart_results"].metadata.get("total_charts", 0)
        if chart_count > 0:
            print(f"📊 Generated {chart_count} charts")

            # Assert charts are valid
            assert results["chart_results"].is_valid, f"Chart validation failed: {results['chart_results'].errors}"

            # Check interactivity
            interactivity_rate = results["chart_results"].metadata.get("interactivity_rate", 0)
            assert interactivity_rate > 0.5, f"Charts not sufficiently interactive: {interactivity_rate:.1%}"

    except Exception as e:
        pytest.fail(f"Robust notebook testing failed for {test_notebook.name}: {str(e)}")


def test_notebook_outputs_are_readable():
    """Test that notebook outputs are actually readable and meaningful."""
    notebooks_dir = Path("notebooks")

    if not notebooks_dir.exists():
        pytest.skip("No notebooks directory found")

    # Find executed notebooks
    executed_notebooks = list(notebooks_dir.glob("executed/*-executed.ipynb"))

    if not executed_notebooks:
        pytest.skip("No executed notebooks found to test readability")

    # Test readability of at least one executed notebook
    test_notebook = executed_notebooks[0]
    print(f"\n📖 Testing readability: {test_notebook.name}")

    # Load executed notebook
    with open(test_notebook, "r", encoding="utf - 8") as f:
        nb = nbformat.read(f, as_version=4)

    # Check for meaningful outputs
    meaningful_outputs = 0
    total_code_cells = 0

    for cell in nb.cells:
        if cell.cell_type == "code":
            total_code_cells += 1

            if hasattr(cell, "outputs") and cell.outputs:
                for output in cell.outputs:
                    if output.output_type in ["execute_result", "display_data"]:
                        if "data" in output:
                            # Check for HTML tables (DataFrames)
                            if "text / html" in output.data:
                                html_content = output.data["text / html"]
                                if "<table" in html_content and len(html_content) > 100:
                                    meaningful_outputs += 1

                            # Check for charts
                            elif "application / vnd.plotly.v1 + json" in output.data:
                                meaningful_outputs += 1

                            # Check for meaningful text output
                            elif "text / plain" in output.data:
                                text_content = output.data["text / plain"]
                                if len(text_content.strip()) > 20:  # More than just simple values
                                    meaningful_outputs += 1

    print(f"📊 Found {meaningful_outputs} meaningful outputs in {total_code_cells} code cells")

    # Assert we have meaningful outputs
    if total_code_cells > 0:
        output_ratio = meaningful_outputs / total_code_cells
        assert (
            output_ratio > 0.3
        ), f"Too few meaningful outputs: {output_ratio:.1%} ({meaningful_outputs}/{total_code_cells})"

    print(f"✅ Notebook outputs are readable: {output_ratio:.1%} meaningful output ratio")


def test_notebooks_produce_interactive_charts():
    """Test that notebooks produce interactive charts as expected."""
    notebooks_dir = Path("notebooks")

    if not notebooks_dir.exists():
        pytest.skip("No notebooks directory found")

    # Find executed notebooks
    executed_notebooks = list(notebooks_dir.glob("executed/*-executed.ipynb"))

    if not executed_notebooks:
        pytest.skip("No executed notebooks found to test charts")

    chart_notebooks = []

    for notebook_path in executed_notebooks:
        # Load notebook
        with open(notebook_path, "r", encoding="utf - 8") as f:
            nb = nbformat.read(f, as_version=4)

        # Check for charts
        has_charts = False
        interactive_charts = 0
        total_charts = 0

        for cell in nb.cells:
            if cell.cell_type == "code" and hasattr(cell, "outputs"):
                for output in cell.outputs:
                    if output.output_type == "display_data" and "data" in output:
                        # Check for Plotly charts
                        if "application / vnd.plotly.v1 + json" in output.data:
                            has_charts = True
                            total_charts += 1

                            try:
                                plotly_data = json.loads(output.data["application / vnd.plotly.v1 + json"])
                                # Check for interactive features
                                if "layout" in plotly_data:
                                    interactive_charts += 1
                            except json.JSONDecodeError:
                                pass

                        # Check for Altair charts
                        elif "application / vnd.vegalite.v4 + json" in output.data:
                            has_charts = True
                            total_charts += 1
                            interactive_charts += 1  # Altair is interactive by default

        if has_charts:
            chart_notebooks.append(
                {
                    "notebook": notebook_path.name,
                    "total_charts": total_charts,
                    "interactive_charts": interactive_charts,
                    "interactivity_rate": interactive_charts / total_charts if total_charts > 0 else 0,
                }
            )

    if not chart_notebooks:
        pytest.skip("No notebooks with charts found")

    print(f"\n📊 Found {len(chart_notebooks)} notebooks with charts:")

    total_charts_all = sum(nb["total_charts"] for nb in chart_notebooks)
    total_interactive_all = sum(nb["interactive_charts"] for nb in chart_notebooks)

    for nb_info in chart_notebooks:
        print(
            f"  {nb_info['notebook']}: {nb_info['interactive_charts']
                }/{nb_info['total_charts']} interactive ({nb_info['interactivity_rate']:.1%})"
        )

    # Assert overall interactivity
    overall_interactivity = total_interactive_all / total_charts_all if total_charts_all > 0 else 0

    assert (
        overall_interactivity > 0.8
    ), f"Charts not sufficiently interactive: {overall_interactivity:.1%} ({total_interactive_all}/{total_charts_all})"

    print(f"✅ Charts are sufficiently interactive: {overall_interactivity:.1%} overall rate")

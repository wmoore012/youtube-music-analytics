import json
from pathlib import Path

from tools.docs.doc_archiver import DocArchiver


def test_categorization_basic(tmp_path: Path, monkeypatch):
    # Create fake repo structure
    repo = tmp_path / "repo"
    (repo / "docs").mkdir(parents=True)
    config = {
        "core_docs": ["README.md"],
        "retention_days": 7,
        "archive_root": "docs / archive",
        "category_patterns": {"reports": ["report"], "generated": ["ci_report"]},
    }
    cfg_path = repo / "docs" / "doc_archive_config.json"
    cfg_path.write_text(json.dumps(config))

    # Create docs
    readme = repo / "README.md"
    readme.write_text("Core doc")
    report = repo / "data_REPORT.md"
    report.write_text("Some report")
    gen = repo / "ci_report_status.md"
    gen.write_text("Generated artifact")

    # Monkeypatch REPO_ROOT in module
    import tools.docs.doc_archiver as da

    da.REPO_ROOT = repo

    archiver = DocArchiver(cfg_path)
    records = archiver.build_records()
    cats = {r.rel_path: r.category for r in records}
    assert cats["README.md"] == "core"
    assert cats["data_REPORT.md"] == "reports"
    assert cats["ci_report_status.md"] == "generated"

    # Generated should auto - archive
    gen_rec = [r for r in records if r.rel_path == "ci_report_status.md"][0]
    assert gen_rec.should_archive is True

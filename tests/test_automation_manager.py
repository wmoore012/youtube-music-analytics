"""Tests for the automation manager cron configuration generator."""

from pathlib import Path
import re

import pytest

from scripts.automation_manager import AutomationManager


@pytest.fixture()
def automation_config(tmp_path, monkeypatch):
    """Create a temporary automation schedule configuration for testing."""

    config = tmp_path / "schedule.yml"
    config.write_text(
        """
test_schedule:
  description: "Example schedule for testing"
  processes:
    - name: "sample_task"
      command: "python manage.py sample"
      schedule: "0 0 * * *"
      description: "Runs the sample task"
"""
    )

    monkeypatch.setenv("CRON_EMAIL", "ops@example.com")
    monkeypatch.setenv("CRON_SHELL", "/bin / zsh")
    monkeypatch.setenv("CRON_PATH", "/usr / bin:/bin")

    return AutomationManager(config_path=str(config))


def test_generate_cron_config_includes_environment_and_valid_entries(automation_config):
    """Generated cron configuration should contain env header and valid entries."""

    cron_text = automation_config.generate_cron_config("test_schedule", dry_run=True)

    lines = [line for line in cron_text.splitlines() if line.strip()]

    # Ensure environment header lines are present
    assert "SHELL=/bin / zsh" in lines
    assert "PATH=/usr / bin:/bin" in lines
    assert "MAILTO=ops@example.com" in lines

    # Extract cron job lines (exclude comments and environment definitions)
    command_lines = [
        line
        for line in lines
        if not line.startswith("#")
        and not line.startswith("SHELL=")
        and not line.startswith("PATH=")
        and not line.startswith("MAILTO=")
    ]

    assert command_lines, "Expected at least one cron command entry"

    cwd = str(Path.cwd().absolute())

    for entry in command_lines:
        # Cron entries should begin with five timing fields
        assert re.match(r"^\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+", entry)
        assert f"cd {cwd}" in entry
        assert ">> logs / automation.log 2>&1" in entry

#!/usr / bin / env python3
"""
Repository Switcher - Smart Git Remote Management
===============================================

Utility for managing git remotes across staging and public deployments.
It keeps file inclusion rules consistent and generates helper scripts for
publishing builds.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Dict, List, Optional


class RepositorySwitcher:
    """Smart repository management with environment - specific configurations."""

    def __init__(self):
        self.config_file = Path(".kiro / settings / repo_config.json")
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        self.load_config()

    def load_config(self):
        """Load repository configuration."""
        default_config = {
            "repositories": {
                "public": {
                    "name": "origin",
                    "url": "https://github.com / wmoore012 / youtube - music - analytics.git",
                    "description": "Public portfolio repository",
                    "excluded_files": [
                        ".env*",
                        "*.db",
                        "*.log",
                        "data/",
                        "logs/",
                        "models/",
                        "reports/",
                        "*.csv",
                        "*.jsonl",
                        "benchmarks.json",
                        "coverage.json",
                        "system_health_dashboard.json",
                        "*_validation_report.json",
                        "*_analysis_report.json",
                    ],
                    "required_files": [
                        "README.md",
                        "docs/",
                        "src/",
                        "tests/",
                        "scripts/",
                        ".github / workflows/",
                    ],
                },
                "staging": {
                    "name": "staging",
                    "url": "https://github.com / wmoore012 / staging_yt_analytics.git",
                    "description": "Private staging repository - full codebase",
                    "excluded_files": [],
                    "required_files": ["*"],
                },
            },
            "current_target": "public",
            "auto_switch_rules": {
                "development": "staging",
                "testing": "staging",
                "production": "public",
                "portfolio": "public",
            },
        }

        if self.config_file.exists():
            with open(self.config_file, "r") as f:
                self.config = json.load(f)
        else:
            self.config = default_config
            self.save_config()

    def save_config(self):
        """Save repository configuration."""
        config_dir = self.config_file.parent
        dir_mode = os.stat(config_dir).st_mode
        if dir_mode & 0o222 == 0:
            raise PermissionError(f"Cannot write configuration to {config_dir}: directory is not writable")
        if self.config_file.exists():
            file_mode = os.stat(self.config_file).st_mode
            if file_mode & 0o222 == 0:
                raise PermissionError(f"Cannot update configuration file: {self.config_file} is not writable")
        with open(self.config_file, "w") as f:
            json.dump(self.config, f, indent=2)

    def list_repositories(self):
        """List all configured repositories."""
        print("🔄 CONFIGURED REPOSITORIES:")
        print("=" * 50)

        for repo_id, repo_config in self.config["repositories"].items():
            current = "👉 CURRENT" if repo_id == self.config["current_target"] else ""
            print(f"\n📁 {repo_id.upper()} {current}")
            print(f"   Name: {repo_config['name']}")
            print(f"   URL: {repo_config['url']}")
            print(f"   Description: {repo_config['description']}")
            print(f"   Excluded Files: {len(repo_config['excluded_files'])} patterns")

            # Check if remote exists
            if not self._is_git_repository():
                print(f"   Status: ⚠️  Not in git repository")
            else:
                try:
                    result = subprocess.run(
                        ["git", "remote", "get - url", repo_config["name"]], capture_output=True, text=True, check=True
                    )
                    print(f"   Status: ✅ Remote configured")
                except subprocess.CalledProcessError:
                    print(f"   Status: ❌ Remote not configured")

    def switch_target(self, target: str):
        """Switch to a different repository target."""
        if target not in self.config["repositories"]:
            print(f"❌ Unknown repository target: {target}")
            print(f"Available targets: {list(self.config['repositories'].keys())}")
            return False

        old_target = self.config["current_target"]
        self.config["current_target"] = target
        self.save_config()

        repo_config = self.config["repositories"][target]

        print(f"🔄 SWITCHING REPOSITORY TARGET")
        print(f"From: {old_target} → To: {target}")
        print(f"Target: {repo_config['description']}")
        print(f"Remote: {repo_config['name']} ({repo_config['url']})")

        # Ensure remote is configured
        self._ensure_remote_configured(target)

        # Show what will be excluded / included
        self._show_file_impact(target)

        print(f"\n✅ Repository target switched to: {target}")
        print(f"💡 Use 'git push {repo_config['name']} main' to deploy")

        return True

    def _ensure_remote_configured(self, target: str):
        """Ensure git remote is properly configured."""
        repo_config = self.config["repositories"][target]
        remote_name = repo_config["name"]
        remote_url = repo_config["url"]

        # Check if we're in a git repository
        if not self._is_git_repository():
            print(f"⚠️  Not in a git repository - skipping remote configuration")
            return

        try:
            # Check if remote exists
            result = subprocess.run(
                ["git", "remote", "get - url", remote_name], capture_output=True, text=True, check=True
            )

            if result.stdout.strip() != remote_url:
                # Update remote URL
                subprocess.run(["git", "remote", "set - url", remote_name, remote_url], check=True)
                print(f"🔧 Updated remote '{remote_name}' URL")

        except subprocess.CalledProcessError:
            try:
                # Add remote
                subprocess.run(["git", "remote", "add", remote_name, remote_url], check=True)
                print(f"➕ Added remote '{remote_name}': {remote_url}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to add remote '{remote_name}': {e}")

    def _is_git_repository(self) -> bool:
        """Check if current directory is a git repository."""
        try:
            subprocess.run(["git", "rev - parse", "--git - dir"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def _show_file_impact(self, target: str):
        """Show what files will be excluded / included for this target."""
        repo_config = self.config["repositories"][target]
        excluded_patterns = repo_config["excluded_files"]

        if not excluded_patterns:
            print(f"\n📁 FILE INCLUSION: All files will be included (private repo)")
            return

        print(f"\n📁 FILE EXCLUSION RULES:")
        print("The following patterns will be EXCLUDED from commits:")
        for pattern in excluded_patterns:
            print(f"   🚫 {pattern}")

        # Check for existing files that match exclusion patterns
        excluded_files = []
        for pattern in excluded_patterns:
            try:
                result = subprocess.run(["find", ".", "-name", pattern, "-type", "f"], capture_output=True, text=True)
                if result.stdout.strip():
                    excluded_files.extend(result.stdout.strip().split("\n"))
            except Exception:
                pass

        if excluded_files:
            print(f"\n⚠️  CURRENTLY TRACKED FILES THAT WILL BE EXCLUDED:")
            for file in excluded_files[:10]:  # Show first 10
                print(f"   📄 {file}")
            if len(excluded_files) > 10:
                print(f"   ... and {len(excluded_files) - 10} more files")

    def create_deployment_script(self, target: str):
        """Create a deployment script for the target repository."""
        if target not in self.config["repositories"]:
            print(f"❌ Unknown repository target: {target}")
            return False

        repo_config = self.config["repositories"][target]
        script_name = f"deploy_to_{target}.sh"

        script_content = f"""#!/bin / bash
# Auto - generated deployment script for {target} repository
# Generated by Repository Switcher

set -e

echo "🚀 DEPLOYING TO {target.upper()} REPOSITORY"
echo "Target: {repo_config['description']}"
echo "Remote: {repo_config['name']} ({repo_config['url']})"

# Switch repository target
python scripts / repo_switcher.py switch {target}

# Check git status
echo "📊 Checking git status..."
git status

# Add files (respecting exclusion rules)
echo "📁 Adding files..."
git add .

# Show what will be committed
echo "📋 Files to be committed:"
git diff --cached --name - only

# Commit with timestamp
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
git commit -m "deploy: Deploy to {target} repository at $TIMESTAMP

🎯 Deployment Target: {target}
📁 Repository: {repo_config['description']}
🔗 Remote: {repo_config['name']}
⏰ Timestamp: $TIMESTAMP

Generated by scripts / repo_switcher.py"

# Push to target repository
echo "🚀 Pushing to {repo_config['name']}..."
git push {repo_config['name']} main

echo "✅ Successfully deployed to {target} repository!"
"""

        with open(script_name, "w") as f:
            f.write(script_content)

        # Make script executable
        os.chmod(script_name, 0o755)

        print(f"✅ Created deployment script: {script_name}")
        print(f"💡 Run with: ./{script_name}")

        return True

    def status(self):
        """Show current repository status and configuration."""
        current = self.config["current_target"]
        repo_config = self.config["repositories"][current]

        print("📊 REPOSITORY SWITCHER STATUS")
        print("=" * 40)
        print(f"Current Target: {current}")
        print(f"Description: {repo_config['description']}")
        print(f"Remote Name: {repo_config['name']}")
        print(f"Remote URL: {repo_config['url']}")
        print(f"Excluded Patterns: {len(repo_config['excluded_files'])}")

        # Show git remotes
        print(f"\n🔗 GIT REMOTES:")
        if not self._is_git_repository():
            print("   ⚠️  Not in a git repository")
        else:
            try:
                result = subprocess.run(["git", "remote", "-v"], capture_output=True, text=True, check=True)
                if result.stdout.strip():
                    for line in result.stdout.strip().split("\n"):
                        if line:
                            print(f"   {line}")
                else:
                    print("   📭 No remotes configured")
            except subprocess.CalledProcessError:
                print("   ❌ Error reading git remotes")

        # Show current branch
        if self._is_git_repository():
            try:
                result = subprocess.run(["git", "branch", "--show - current"],
                                        capture_output=True, text=True, check=True)
                current_branch = result.stdout.strip()
                print(f"\n🌿 Current Branch: {current_branch}")
            except subprocess.CalledProcessError:
                print(f"\n🌿 Current Branch: Unknown")
        else:
            print(f"\n🌿 Current Branch: Not in git repository")


def main():
    """Main CLI interface."""
    switcher = RepositorySwitcher()

    if len(sys.argv) < 2:
        print("🔄 REPOSITORY SWITCHER")
        print("=" * 30)
        print("Usage:")
        print("  python scripts / repo_switcher.py list")
        print("  python scripts / repo_switcher.py switch <target>")
        print("  python scripts / repo_switcher.py status")
        print("  python scripts / repo_switcher.py deploy <target>")
        print("")
        print("Available targets: public, staging")
        return

    command = sys.argv[1]

    if command == "list":
        switcher.list_repositories()

    elif command == "switch":
        if len(sys.argv) < 3:
            print("❌ Please specify target: public or staging")
            return
        target = sys.argv[2]
        switcher.switch_target(target)

    elif command == "status":
        switcher.status()

    elif command == "deploy":
        if len(sys.argv) < 3:
            print("❌ Please specify target: public or staging")
            return
        target = sys.argv[2]
        switcher.create_deployment_script(target)

    else:
        print(f"❌ Unknown command: {command}")
        print("Available commands: list, switch, status, deploy")


if __name__ == "__main__":
    main()

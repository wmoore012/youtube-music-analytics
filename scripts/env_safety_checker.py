#!/usr / bin / env python3
"""
Environment Safety Checker-Repository Security Validation
=========================================================

Ensures sensitive files (.env, credentials, etc.) are only committed to staging repository.
Critical security check for preventing accidental exposure of secrets.

Built by Grammy-nominated producer + M.S. Data Science student.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


class EnvironmentSafetyChecker:
    """Safety checker for environment files and sensitive data."""

    def __init__(self):
        self.config_file = Path(".kiro / settings / repo_config.json")
        self.sensitive_patterns = {
            ".env*",
            "*.key",
            "*.pem",
            "credentials/*",
            "secrets/*",
            "api_keys/*",
            "tokens/*",
            "*.log",
            "*.sql",
            "datasets/*",
            "models/*",
            "*.csv",
            "*.json",
            "notebooks/*.ipynb",
        }
        self.load_config()

    def load_config(self):
        """Load repository configuration."""
        if self.config_file.exists():
            with open(self.config_file, "r") as f:
                self.config = json.load(f)
        else:
            print("❌ Repository configuration not found!")
            print("💡 Run: python scripts / repo_switcher.py status")
            sys.exit(1)

    def get_current_target(self) -> str:
        """Get current repository target."""
        return self.config.get("current_target", "unknown")

    def get_staged_files(self) -> List[str]:
        """Get list of files staged for commit."""
        try:
            result = subprocess.run(
                ["git", "diff", "--cached", "--name-only"], capture_output=True, text=True, check=True
            )
            return result.stdout.strip().split("\n") if result.stdout.strip() else []
        except subprocess.CalledProcessError:
            return []

    def check_gitignore_status(self) -> Dict[str, bool]:
        """Check if sensitive patterns are in .gitignore."""
        gitignore_path = Path(".gitignore")
        if not gitignore_path.exists():
            return {}

        gitignore_content = gitignore_path.read_text()

        status = {}
        for pattern in self.sensitive_patterns:
            # Check if pattern is commented out (not ignored)
            __pattern_escaped = pattern.replace("*", r"\*").replace(".", r"\.")  # noqa: F841
            is_ignored = f"\n{pattern}\n" in gitignore_content or gitignore_content.startswith(f"{pattern}\n")
            is_commented = f"# {pattern}" in gitignore_content

            status[pattern] = is_ignored and not is_commented

        return status

    def find_sensitive_files(self, files: List[str]) -> Dict[str, List[str]]:
        """Find sensitive files in the given list."""
        import fnmatch

        sensitive_files = {}

        for pattern in self.sensitive_patterns:
            matches = []
            for file in files:
                if fnmatch.fnmatch(file, pattern) or fnmatch.fnmatch(f"/{file}", f"/{pattern}"):
                    matches.append(file)

            if matches:
                sensitive_files[pattern] = matches

        return sensitive_files

    def validate_commit_safety(self) -> bool:
        """Validate that it's safe to commit sensitive files."""
        current_target = self.get_current_target()
        staged_files = self.get_staged_files()

        if not staged_files:
            print("✅ No files staged for commit")
            return True

        sensitive_files = self.find_sensitive_files(staged_files)

        if not sensitive_files:
            print("✅ No sensitive files detected in staged changes")
            return True

        print(f"\n🔍 ENVIRONMENT SAFETY CHECK")
        print(f"Current Repository Target: {current_target}")
        print(f"Staged Files: {len(staged_files)}")

        # Show sensitive files found
        print(f"\n⚠️  SENSITIVE FILES DETECTED:")
        for pattern, files in sensitive_files.items():
            print(f"   📁 {pattern}:")
            for file in files:
                print(f"      📄 {file}")

        # Check if staging repository
        if current_target == "staging":
            print(f"\n✅ SAFE: Staging repository allows sensitive files")
            print(f"   🔒 Private repository: https://github.com / wmoore012 / staging_yt_analytics.git")
            print(f"   🤖 Agent collaboration: Full access to development files")
            return True

        # Unsafe for public repository
        print(f"\n🚨 UNSAFE: Public repository detected!")
        print(f"   📢 Current target: {current_target}")
        print(f"   ⚠️  Sensitive files would be publicly exposed")

        print(f"\n🔧 REQUIRED ACTION:")
        print(f"   1. Switch to staging repository:")
        print(f"      python scripts / repo_switcher.py switch staging")
        print(f"   2. Then commit your changes:")
        print(f"      git commit -m 'your message'")
        print(f"   3. Push to staging:")
        print(f"      git push staging main")

        return False

    def check_gitignore_consistency(self) -> bool:
        """Check if .gitignore is consistent with repository target."""
        current_target = self.get_current_target()
        gitignore_status = self.check_gitignore_status()

        print(f"\n📋 GITIGNORE CONSISTENCY CHECK")
        print(f"Repository Target: {current_target}")

        if current_target == "staging":
            # Staging should allow sensitive files (patterns should be commented out)
            blocked_patterns = [pattern for pattern, is_ignored in gitignore_status.items() if is_ignored]

            if blocked_patterns:
                print(f"\n⚠️  WARNING: Staging repo has blocked patterns in .gitignore:")
                for pattern in blocked_patterns:
                    print(f"   🚫 {pattern}")
                print(f"\n💡 Consider commenting out these patterns for full agent access")
                return False
            else:
                print(f"✅ Staging .gitignore allows sensitive files for agent collaboration")
                return True

        else:
            # Public repo should block sensitive files
            allowed_patterns = [pattern for pattern, is_ignored in gitignore_status.items() if not is_ignored]

            if allowed_patterns:
                print(f"\n🚨 CRITICAL: Public repo allows sensitive patterns:")
                for pattern in allowed_patterns:
                    print(f"   ⚠️  {pattern}")
                print(f"\n🔧 Switch to staging or update .gitignore to block these patterns")
                return False
            else:
                print(f"✅ Public .gitignore properly blocks sensitive files")
                return True

    def generate_safety_report(self) -> Dict:
        """Generate comprehensive safety report."""
        current_target = self.get_current_target()
        staged_files = self.get_staged_files()
        sensitive_files = self.find_sensitive_files(staged_files)
        gitignore_status = self.check_gitignore_status()

        return {
            "timestamp": subprocess.run(["date"], capture_output=True, text=True).stdout.strip(),
            "repository_target": current_target,
            "staged_files_count": len(staged_files),
            "sensitive_files_detected": len(sensitive_files),
            "sensitive_patterns_found": list(sensitive_files.keys()),
            "gitignore_blocks_sensitive": sum(1 for blocked in gitignore_status.values() if blocked),
            "commit_safety_status": "SAFE" if current_target == "staging" else "UNSAFE",
            "agent_collaboration_ready": current_target == "staging",
            "recommendations": self._generate_recommendations(current_target, sensitive_files),
        }

    def _generate_recommendations(self, target: str, sensitive_files: Dict) -> List[str]:
        """Generate safety recommendations."""
        recommendations = []

        if sensitive_files and target != "staging":
            recommendations.append("Switch to staging repository before committing sensitive files")
            recommendations.append("Use: python scripts / repo_switcher.py switch staging")

        if target == "staging":
            recommendations.append("Staging repository active-full agent collaboration enabled")
            recommendations.append("All development files accessible for agent assistance")

        if target == "public":
            recommendations.append("Public repository active-ensure no sensitive data in commits")
            recommendations.append("Use staging repository for development with sensitive data")

        return recommendations


def main():
    """Main CLI interface."""
    checker = EnvironmentSafetyChecker()

    if len(sys.argv) > 1 and sys.argv[1] == "report":
        # Generate detailed report
        report = checker.generate_safety_report()
        print(json.dumps(report, indent=2))
        return

    # Standard safety check
    print("🔒 ENVIRONMENT SAFETY CHECKER")
    print("=" * 40)

    # Check commit safety
    commit_safe = checker.validate_commit_safety()

    # Check .gitignore consistency
    gitignore_consistent = checker.check_gitignore_consistency()

    # Overall result
    if commit_safe and gitignore_consistent:
        print(f"\n✅ ALL SAFETY CHECKS PASSED")
        print(f"🚀 Safe to proceed with commit")
        sys.exit(0)
    else:
        print(f"\n❌ SAFETY CHECKS FAILED")
        print(f"🛑 Do not proceed with commit")
        sys.exit(1)


if __name__ == "__main__":
    main()

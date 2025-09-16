#!/usr/bin/env python3
"""
Automation Manager - Explicit User Control Over Scheduled Tasks

This script provides transparent, user-controlled automation for the
YouTube Music Analytics platform. Built with Grammy-level reliability
and M.S. Data Science rigor.

Key Principles:
- No hidden processes or automatic startup
- Complete user visibility and control
- Easy enable/disable for all automation
- Comprehensive logging and monitoring
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml


class AutomationManager:
    """
    Manages automated processes with explicit user control.

    Designed for music industry professionals who need reliable,
    transparent automation without surprises.
    """

    def __init__(self, config_path: str = "config/automation/schedule_templates.yml"):
        self.config_path = Path(config_path)
        self.cron_backup_path = Path("config/automation/cron_backup.txt")
        self.active_config_path = Path("config/automation/active_schedule.yml")
        self.log_path = Path("logs/automation.log")

        # Ensure directories exist
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.active_config_path.parent.mkdir(parents=True, exist_ok=True)

        self.load_config()

    def load_config(self) -> None:
        """Load automation configuration templates."""
        try:
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            print(f"❌ Configuration file not found: {self.config_path}")
            print("Run 'make setup' to create default configuration")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"❌ Error parsing configuration: {e}")
            sys.exit(1)

    def log_action(self, action: str, details: str = "") -> None:
        """Log automation actions with timestamp."""
        timestamp = datetime.now().isoformat()
        log_entry = f"{timestamp} - {action}"
        if details:
            log_entry += f" - {details}"

        with open(self.log_path, "a") as f:
            f.write(log_entry + "\n")

        print(f"📝 {log_entry}")

    def list_schedules(self) -> None:
        """List available automation schedules."""
        print("🤖 Available Automation Schedules")
        print("=" * 50)

        for schedule_name, schedule_config in self.config.items():
            if schedule_name in ["management", "user_control", "notifications", "logging"]:
                continue

            print(f"\n📋 {schedule_name.upper()}")
            print(f"   Description: {schedule_config.get('description', 'No description')}")
            print(f"   Frequency: {schedule_config.get('frequency', 'Unknown')}")

            processes = schedule_config.get("processes", [])
            print(f"   Processes: {len(processes)}")

            for process in processes[:3]:  # Show first 3 processes
                print(f"     • {process['name']}: {process['description']}")

            if len(processes) > 3:
                print(f"     ... and {len(processes) - 3} more")

    def generate_cron_config(self, schedule_name: str, dry_run: bool = False) -> str:
        """Generate CRON configuration from template."""
        if schedule_name not in self.config:
            print(f"❌ Schedule '{schedule_name}' not found")
            self.list_schedules()
            return ""

        schedule = self.config[schedule_name]
        processes = schedule.get("processes", [])

        if not processes:
            print(f"❌ No processes defined for schedule '{schedule_name}'")
            return ""

        # Get current working directory for absolute paths
        current_dir = Path.cwd().absolute()

        cron_lines = []
        cron_lines.append(f"# YouTube Music Analytics - {schedule_name.title()} Schedule")
        cron_lines.append(f"# Generated on {datetime.now().isoformat()}")
        cron_lines.append(f"# Schedule: {schedule.get('description', 'No description')}")
        cron_lines.append("")

        for process in processes:
            name = process["name"]
            command = process["command"]
            schedule_time = process["schedule"]
            description = process.get("description", "No description")

            # Add comment with description
            cron_lines.append(f"# {description}")

            # Add user control information if available
            user_control = process.get("user_control")
            if user_control:
                cron_lines.append(f"# User Control: {user_control}")

            # Create full command with proper paths and logging
            full_command = f"cd {current_dir} && {command} >> logs/automation.log 2>&1"

            # Add CRON line
            cron_lines.append(f"{schedule_time} {full_command}")
            cron_lines.append("")

        cron_config = "\n".join(cron_lines)

        if dry_run:
            print("🔍 Generated CRON Configuration (Dry Run):")
            print("=" * 60)
            print(cron_config)
            print("=" * 60)
            print("\n💡 To apply this configuration:")
            print(f"   python scripts/automation_manager.py apply-cron {schedule_name}")
        else:
            # Save configuration for review
            config_file = Path(f"config/automation/generated_{schedule_name}_cron.txt")
            with open(config_file, "w") as f:
                f.write(cron_config)

            print(f"✅ CRON configuration generated: {config_file}")
            print("\n📋 Review the configuration before applying:")
            print(f"   cat {config_file}")
            print("\n🚀 Apply the configuration:")
            print(f"   python scripts/automation_manager.py apply-cron {schedule_name}")

        return cron_config

    def backup_current_cron(self) -> bool:
        """Backup current CRON configuration."""
        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)

            if result.returncode == 0:
                with open(self.cron_backup_path, "w") as f:
                    f.write(result.stdout)

                self.log_action("CRON_BACKUP", f"Saved to {self.cron_backup_path}")
                return True
            else:
                self.log_action("CRON_BACKUP_FAILED", "No existing crontab or access denied")
                return False

        except Exception as e:
            self.log_action("CRON_BACKUP_ERROR", str(e))
            return False

    def apply_cron_schedule(self, schedule_name: str, force: bool = False) -> bool:
        """Apply generated CRON schedule to system."""
        config_file = Path(f"config/automation/generated_{schedule_name}_cron.txt")

        if not config_file.exists():
            print(f"❌ Generated configuration not found: {config_file}")
            print(f"Run: python scripts/automation_manager.py generate-cron {schedule_name}")
            return False

        # Show what will be applied
        print("🔍 CRON Configuration to Apply:")
        print("=" * 50)
        with open(config_file, "r") as f:
            print(f.read())
        print("=" * 50)

        if not force:
            response = input("\n❓ Apply this CRON configuration? (y/N): ")
            if response.lower() != "y":
                print("❌ CRON application cancelled by user")
                return False

        # Backup current CRON
        print("💾 Backing up current CRON configuration...")
        self.backup_current_cron()

        # Apply new CRON configuration
        try:
            with open(config_file, "r") as f:
                cron_content = f.read()

            # Apply via crontab command
            process = subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE, text=True)
            process.communicate(input=cron_content)

            if process.returncode == 0:
                # Save active configuration
                with open(self.active_config_path, "w") as f:
                    yaml.dump(
                        {
                            "schedule_name": schedule_name,
                            "applied_at": datetime.now().isoformat(),
                            "config_file": str(config_file),
                        },
                        f,
                    )

                self.log_action("CRON_APPLIED", f"Schedule: {schedule_name}")
                print(f"✅ CRON schedule '{schedule_name}' applied successfully")
                print("\n📊 Monitor automation with:")
                print("   python scripts/automation_manager.py status")
                return True
            else:
                self.log_action("CRON_APPLY_FAILED", f"Return code: {process.returncode}")
                print("❌ Failed to apply CRON configuration")
                return False

        except Exception as e:
            self.log_action("CRON_APPLY_ERROR", str(e))
            print(f"❌ Error applying CRON configuration: {e}")
            return False

    def disable_automation(self) -> bool:
        """Disable all automation by clearing CRON."""
        print("⚠️ Disabling All Automation")
        print("This will remove all scheduled tasks from CRON")

        response = input("❓ Continue? (y/N): ")
        if response.lower() != "y":
            print("❌ Automation disable cancelled by user")
            return False

        # Backup current CRON
        self.backup_current_cron()

        try:
            # Clear CRON by applying empty configuration
            process = subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE, text=True)
            process.communicate(input="# All automation disabled by user\n")

            if process.returncode == 0:
                self.log_action("AUTOMATION_DISABLED", "All CRON jobs removed")
                print("✅ All automation disabled")
                print("\n🔄 To re-enable automation:")
                print("   python scripts/automation_manager.py restore-cron")
                return True
            else:
                print("❌ Failed to disable automation")
                return False

        except Exception as e:
            self.log_action("DISABLE_ERROR", str(e))
            print(f"❌ Error disabling automation: {e}")
            return False

    def restore_cron(self) -> bool:
        """Restore CRON from backup."""
        if not self.cron_backup_path.exists():
            print("❌ No CRON backup found")
            print("Cannot restore previous configuration")
            return False

        print("🔄 Restoring CRON from backup...")

        try:
            with open(self.cron_backup_path, "r") as f:
                backup_content = f.read()

            print("📋 Backup CRON Configuration:")
            print("=" * 40)
            print(backup_content)
            print("=" * 40)

            response = input("\n❓ Restore this configuration? (y/N): ")
            if response.lower() != "y":
                print("❌ CRON restore cancelled by user")
                return False

            # Apply backup configuration
            process = subprocess.Popen(["crontab", "-"], stdin=subprocess.PIPE, text=True)
            process.communicate(input=backup_content)

            if process.returncode == 0:
                self.log_action("CRON_RESTORED", "From backup")
                print("✅ CRON configuration restored from backup")
                return True
            else:
                print("❌ Failed to restore CRON configuration")
                return False

        except Exception as e:
            self.log_action("RESTORE_ERROR", str(e))
            print(f"❌ Error restoring CRON: {e}")
            return False

    def show_status(self) -> None:
        """Show current automation status."""
        print("📊 Automation Status")
        print("=" * 30)

        # Check if active configuration exists
        if self.active_config_path.exists():
            with open(self.active_config_path, "r") as f:
                active_config = yaml.safe_load(f)

            print(f"✅ Active Schedule: {active_config['schedule_name']}")
            print(f"📅 Applied At: {active_config['applied_at']}")
        else:
            print("❌ No active automation configuration")

        # Show current CRON jobs
        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)

            if result.returncode == 0 and result.stdout.strip():
                print("\n📋 Current CRON Jobs:")
                print("-" * 30)
                print(result.stdout)
            else:
                print("\n📋 No CRON jobs currently scheduled")

        except Exception as e:
            print(f"\n❌ Error checking CRON status: {e}")

        # Show recent log entries
        if self.log_path.exists():
            print("\n📝 Recent Automation Log (last 10 entries):")
            print("-" * 50)
            try:
                with open(self.log_path, "r") as f:
                    lines = f.readlines()
                    for line in lines[-10:]:
                        print(line.strip())
            except Exception as e:
                print(f"Error reading log: {e}")

    def test_schedule(self, schedule_name: str) -> None:
        """Test a schedule configuration without applying it."""
        print(f"🧪 Testing Schedule: {schedule_name}")
        print("=" * 40)

        if schedule_name not in self.config:
            print(f"❌ Schedule '{schedule_name}' not found")
            self.list_schedules()
            return

        schedule = self.config[schedule_name]
        processes = schedule.get("processes", [])

        print(f"📋 Schedule Description: {schedule.get('description', 'No description')}")
        print(f"🔄 Frequency: {schedule.get('frequency', 'Unknown')}")
        print(f"📊 Total Processes: {len(processes)}")
        print()

        for i, process in enumerate(processes, 1):
            print(f"🔍 Process {i}: {process['name']}")
            print(f"   Command: {process['command']}")
            print(f"   Schedule: {process['schedule']}")
            print(f"   Description: {process.get('description', 'No description')}")

            # Test command syntax (dry run)
            command = process["command"]
            if command.startswith("make "):
                make_target = command.split(" ", 1)[1]
                print(f"   ✅ Make target: {make_target}")
            elif command.startswith("python "):
                script_path = command.split(" ", 1)[1].split(" ")[0]
                if Path(script_path).exists():
                    print(f"   ✅ Script exists: {script_path}")
                else:
                    print(f"   ⚠️ Script not found: {script_path}")

            user_control = process.get("user_control")
            if user_control:
                print(f"   🎛️ User Control: {user_control}")

            print()

        print("💡 To apply this schedule:")
        print(f"   python scripts/automation_manager.py generate-cron {schedule_name}")
        print(f"   python scripts/automation_manager.py apply-cron {schedule_name}")


def main():
    """Main function with comprehensive argument parsing."""
    parser = argparse.ArgumentParser(
        description="YouTube Music Analytics - Automation Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available schedules
  python scripts/automation_manager.py list

  # Test a schedule configuration
  python scripts/automation_manager.py test standard

  # Generate CRON configuration
  python scripts/automation_manager.py generate-cron standard

  # Apply CRON configuration
  python scripts/automation_manager.py apply-cron standard

  # Check automation status
  python scripts/automation_manager.py status

  # Disable all automation
  python scripts/automation_manager.py disable

  # Restore from backup
  python scripts/automation_manager.py restore-cron
        """,
    )

    parser.add_argument(
        "action",
        choices=["list", "test", "generate-cron", "apply-cron", "status", "disable", "restore-cron"],
        help="Action to perform",
    )

    parser.add_argument("schedule", nargs="?", help="Schedule name (required for test, generate-cron, apply-cron)")

    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")

    parser.add_argument("--force", action="store_true", help="Skip confirmation prompts")

    args = parser.parse_args()

    # Initialize automation manager
    manager = AutomationManager()

    # Execute requested action
    if args.action == "list":
        manager.list_schedules()

    elif args.action == "test":
        if not args.schedule:
            print("❌ Schedule name required for test action")
            parser.print_help()
            sys.exit(1)
        manager.test_schedule(args.schedule)

    elif args.action == "generate-cron":
        if not args.schedule:
            print("❌ Schedule name required for generate-cron action")
            parser.print_help()
            sys.exit(1)
        manager.generate_cron_config(args.schedule, dry_run=args.dry_run)

    elif args.action == "apply-cron":
        if not args.schedule:
            print("❌ Schedule name required for apply-cron action")
            parser.print_help()
            sys.exit(1)
        manager.apply_cron_schedule(args.schedule, force=args.force)

    elif args.action == "status":
        manager.show_status()

    elif args.action == "disable":
        manager.disable_automation()

    elif args.action == "restore-cron":
        manager.restore_cron()


if __name__ == "__main__":
    main()

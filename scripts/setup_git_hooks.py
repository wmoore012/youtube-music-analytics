#!/usr/bin/env python3
"""
🔧 Git Hooks Setup
=================

Sets up git hooks for automatic quality validation.
"""

import os
import sys
import stat
from pathlib import Path

def setup_pre_commit_hook():
    """Set up the pre-commit hook."""
    
    repo_root = Path(__file__).parent.parent
    hooks_dir = repo_root / ".git" / "hooks"
    
    if not hooks_dir.exists():
        print("❌ .git/hooks directory not found. Are you in a git repository?")
        return False
    
    # Create pre-commit hook
    hook_path = hooks_dir / "pre-commit"
    hook_content = f"""#!/usr/bin/env python3
# Auto-generated pre-commit hook
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.pre_commit_hook import main
sys.exit(main())
"""
    
    with open(hook_path, 'w') as f:
        f.write(hook_content)
    
    # Make executable
    hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)
    
    print(f"✅ Pre-commit hook installed: {hook_path}")
    return True

def main():
    """Main setup function."""
    
    print("🔧 SETTING UP GIT HOOKS")
    print("=" * 30)
    
    if setup_pre_commit_hook():
        print("\n🎉 Git hooks setup complete!")
        print("✅ Pre-commit validation will run automatically")
        print("\n💡 To bypass validation (not recommended):")
        print("   git commit --no-verify")
    else:
        print("\n❌ Git hooks setup failed")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
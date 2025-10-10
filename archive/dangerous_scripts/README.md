# Archived Dangerous Linting Scripts

These scripts have been archived because they use dangerous patterns that can break code:

## Problems with these scripts:

1. **Regex-based code modification**: Uses string replacement to modify Python code, which can break syntax
2. **Mass # noqa insertion**: Hides real code quality issues instead of fixing them
3. **Whole-repository rewrites**: Creates noisy diffs and makes it hard to review changes
4. **Context manager breakage**: Can turn `with open() as f:` into `with open() as _:` breaking functionality
5. **Double-underscore variables**: Creates confusing variable names like `__var_name`

## Safe alternatives:

- Use `safe_professional_linting.py` for proper tooling-based approach
- Use `ruff --fix` for safe automatic fixes
- Use `black` for code formatting
- Create baselines for gradual improvement instead of mass # noqa

## Key principles:

- Never use regex to modify Python code
- Use proper AST-aware tools (ruff, black, autoflake)
- Apply fixes incrementally with proper review
- Maintain test coverage throughout the process

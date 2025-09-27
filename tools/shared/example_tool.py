"""
Example tool demonstrating usage of the ToolBase class and shared utilities.

This serves as a template for creating new tools that follow the standardized patterns.
"""

from typing import List

from tools.shared.common import ToolBase, ToolConfig, register_tool


class ExampleTool(ToolBase):
    """Example tool showing proper usage of ToolBase."""

    def __init__(self):
        super().__init__(name="example - tool", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return []  # This example tool doesn't require any env vars

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="example - tool",
            version="1.0.0",
            description="Example tool demonstrating ToolBase usage patterns",
            dependencies=["python>=3.8"],
            environment_vars=[],
            usage_examples=["python tools / shared / example_tool.py", "python -m tools.shared.example_tool"],
            category="development",
        )

    def run(self) -> None:
        """Main execution method."""
        self.log_progress("Starting example tool execution")

        try:
            # Example of configuration usage
            debug_mode = self.get_config_value("DEBUG", default="false").lower() == "true"

            if debug_mode:
                self.log_progress("Debug mode enabled", level="DEBUG")

            # Example of input validation
            test_value = "example"
            self.validate_input(
                test_value, lambda x: isinstance(x, str) and len(x) > 0, "Test value must be a non - empty string"
            )

            self.log_progress("Example tool completed successfully")

        except Exception as e:
            self.handle_error(e, "main execution")


def main():
    """Main entry point for the example tool."""
    with ExampleTool() as tool:
        tool.run()


if __name__ == "__main__":
    main()

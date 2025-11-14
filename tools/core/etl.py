#!/usr/bin/env python3
"""
Unified ETL Tool-Consolidated ETL Pipeline Management

This tool consolidates multiple ETL scripts into a single, configurable interface:
- Focused ETL for core data processing
- Comprehensive ETL with all data sources
- Channel-specific ETL operations
- Notebook execution integration

Usage:
    python tools / core / etl.py --mode focused
    python tools / core / etl.py --mode comprehensive
    python tools / core / etl.py --channels "artist1,artist2"
    python tools / core / etl.py --with-notebooks
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import ToolBase, ToolConfig, register_tool


class UnifiedETL(ToolBase):
    """
    Unified ETL tool consolidating multiple ETL operations.

    This tool provides a single interface for all ETL operations,
    replacing multiple scattered ETL scripts with a clean, unified approach.
    """

    def __init__(self):
        super().__init__(name="unified-etl", version="1.0.0")
        register_tool(self.get_tool_config())

        # Import ETL modules after path setup
        from tools.core.sentiment_analysis import process_sentiment_analysis
        from web.etl_helpers import get_engine

        self.get_engine = get_engine
        self.process_sentiment_analysis = process_sentiment_analysis

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return ["DATABASE_URL", "YOUTUBE_API_KEY"]

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="unified-etl",
            version="1.0.0",
            description="Unified ETL tool consolidating multiple ETL operations",
            dependencies=["sqlalchemy", "pandas", "python-dotenv", "requests"],
            environment_vars=["DATABASE_URL", "YOUTUBE_API_KEY"],
            usage_examples=[
                "python tools / core / etl.py --mode focused",
                "python tools / core / etl.py --mode comprehensive",
                "python tools / core / etl.py --channels 'artist1,artist2'",
                "python tools / core / etl.py --with-notebooks",
            ],
            category="core",
        )

    def run_focused_etl(self) -> None:
        """
        Run focused ETL for core data processing.

        This mode processes essential data quickly:
        - Sentiment analysis for new comments
        - Data quality validation
        - Core metrics updates
        """
        self.log_progress("Starting focused ETL pipeline")

        try:
            # Import focused ETL logic
            from tools.core.run_focused_etl import main as run_focused_main

            self.log_progress("Executing focused ETL operations")
            run_focused_main()

            self.log_progress("Focused ETL completed successfully")

        except Exception as e:
            self.handle_error(e, "focused ETL execution")

    def run_comprehensive_etl(self) -> None:
        """
        Run comprehensive ETL with all data sources.

        This mode processes all available data:
        - Full YouTube data extraction
        - Complete sentiment analysis
        - All metrics and analytics
        - Data quality validation
        """
        self.log_progress("Starting comprehensive ETL pipeline")

        try:
            # Import comprehensive ETL logic
            from tools.core.run_comprehensive_etl import main as run_comprehensive_main

            self.log_progress("Executing comprehensive ETL operations")
            run_comprehensive_main()

            self.log_progress("Comprehensive ETL completed successfully")

        except Exception as e:
            self.handle_error(e, "comprehensive ETL execution")

    def run_channel_specific_etl(self, channels: List[str]) -> None:
        """
        Run ETL for specific channels only.

        Args:
            channels: List of channel names to process
        """
        self.log_progress(f"Starting channel-specific ETL for: {', '.join(channels)}")

        try:
            # Import channel-specific ETL logic
            # Set channels in environment for the script
            import os

            from tools.core.run_channels_from_env import main as run_channels_main

            original_channels = os.environ.get("CHANNELS", "")
            os.environ["CHANNELS"] = ",".join(channels)

            try:
                self.log_progress("Executing channel-specific ETL operations")
                run_channels_main()
            finally:
                # Restore original environment
                if original_channels:
                    os.environ["CHANNELS"] = original_channels
                elif "CHANNELS" in os.environ:
                    del os.environ["CHANNELS"]

            self.log_progress("Channel-specific ETL completed successfully")

        except Exception as e:
            self.handle_error(e, "channel-specific ETL execution")

    def run_etl_with_notebooks(self, mode: str = "focused") -> None:
        """
        Run ETL and execute notebooks.

        Args:
            mode: ETL mode to run ("focused" or "comprehensive")
        """
        self.log_progress(f"Starting ETL with notebooks (mode: {mode})")

        try:
            # Run ETL first
            if mode == "focused":
                self.run_focused_etl()
            elif mode == "comprehensive":
                self.run_comprehensive_etl()
            else:
                raise ValueError(f"Invalid ETL mode: {mode}")

            # Then run notebooks
            self.log_progress("Executing notebooks after ETL")
            from tools.development.run_notebooks import main as run_notebooks_main

            run_notebooks_main()

            self.log_progress("ETL with notebooks completed successfully")

        except Exception as e:
            self.handle_error(e, "ETL with notebooks execution")

    def run_production_pipeline(self) -> None:
        """
        Run production pipeline with full validation.

        This mode includes:
        - Pre-flight checks
        - Comprehensive ETL
        - Data validation
        - Notebook execution
        - Post-processing validation
        """
        self.log_progress("Starting production pipeline")

        try:
            # Import production pipeline logic
            from tools.core.run_production_pipeline import main as run_production_main

            self.log_progress("Executing production pipeline")
            run_production_main()

            self.log_progress("Production pipeline completed successfully")

        except Exception as e:
            self.handle_error(e, "production pipeline execution")

    def validate_data_quality(self) -> None:
        """Run data quality validation checks."""
        self.log_progress("Running data quality validation")

        try:
            from tools.core.data_quality_validator import main as validate_main

            self.log_progress("Executing data quality checks")
            validate_main()

            self.log_progress("Data quality validation completed")

        except Exception as e:
            self.handle_error(e, "data quality validation")

    def run(
        self,
        mode: str = "focused",
        channels: Optional[List[str]] = None,
        with_notebooks: bool = False,
        production: bool = False,
        validate_only: bool = False,
    ) -> None:
        """
        Main execution method with configurable options.

        Args:
            mode: ETL mode ("focused", "comprehensive")
            channels: Specific channels to process (overrides mode)
            with_notebooks: Whether to execute notebooks after ETL
            production: Whether to run full production pipeline
            validate_only: Whether to only run data quality validation
        """
        self.log_progress("Starting unified ETL execution")

        try:
            if validate_only:
                self.validate_data_quality()
            elif production:
                self.run_production_pipeline()
            elif channels:
                self.run_channel_specific_etl(channels)
                if with_notebooks:
                    from tools.development.run_notebooks import main as run_notebooks_main

                    run_notebooks_main()
            elif with_notebooks:
                self.run_etl_with_notebooks(mode)
            elif mode == "focused":
                self.run_focused_etl()
            elif mode == "comprehensive":
                self.run_comprehensive_etl()
            else:
                raise ValueError(f"Invalid ETL mode: {mode}")

            self.log_progress("Unified ETL execution completed successfully")

        except Exception as e:
            self.handle_error(e, "unified ETL execution")


def main():
    """Main entry point for the unified ETL tool."""
    parser = argparse.ArgumentParser(
        description="Unified ETL Tool-Consolidated ETL Pipeline Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode focused                    # Run focused ETL (default)
  %(prog)s --mode comprehensive              # Run comprehensive ETL
  %(prog)s --channels "artist1,artist2"      # Run ETL for specific channels
  %(prog)s --mode focused --with-notebooks   # Run ETL and execute notebooks
  %(prog)s --production                      # Run full production pipeline
  %(prog)s --validate-only                   # Run data quality validation only
        """,
    )

    parser.add_argument(
        "--mode", choices=["focused", "comprehensive"], default="focused", help="ETL mode to run (default: focused)"
    )

    parser.add_argument("--channels", type=str, help="Comma-separated list of channels to process (overrides mode)")

    parser.add_argument("--with-notebooks", action="store_true", help="Execute notebooks after ETL completion")

    parser.add_argument("--production", action="store_true", help="Run full production pipeline with validation")

    parser.add_argument("--validate-only", action="store_true", help="Run data quality validation only")

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Parse channels if provided
    channels = None
    if args.channels:
        channels = [ch.strip() for ch in args.channels.split(",")]

    # Create and run ETL tool
    with UnifiedETL() as etl_tool:
        etl_tool.run(
            mode=args.mode,
            channels=channels,
            with_notebooks=args.with_notebooks,
            production=args.production,
            validate_only=args.validate_only,
        )


if __name__ == "__main__":
    main()

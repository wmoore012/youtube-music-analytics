from __future__ import annotations

class QuotaTracker:
    """
    Track YouTube API quota usage to avoid exceeding daily limits.

    Attributes:
        units (int): Current quota units used
        max_units (int): Maximum quota units allowed (0 = no limit)
    """

    def __init__(self, max_units: int = 0):
        """
        Initialize the quota tracker.

        Args:
            max_units (int): Maximum quota units allowed (0 = no limit)
        """
        self.units = 0
        self.max_units = max_units

    def check_quota(self, required_units: int = 1) -> bool:
        """
        Check if there's enough quota available.

        Args:
            required_units (int): Units required for the next operation

        Returns:
            bool: True if enough quota is available, False otherwise
        """
        if self.max_units <= 0:  # No limit
            return True
        return (self.units + required_units) <= self.max_units

    def increment(self, units: int = 1) -> None:
        """
        Increment the quota usage.

        Args:
            units (int): Units to add to the current usage
        """
        self.units += units

    def get_usage_str(self) -> str:
        """
        Get a string representation of the current quota usage.

        Returns:
            str: Quota usage string
        """
        if self.max_units <= 0:
            return f"{self.units} units"
        return f"{self.units}/{self.max_units} units"


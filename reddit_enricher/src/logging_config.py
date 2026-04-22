import logging
import sys


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and emojis for human-friendly logging."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    EMOJIS = {
        "DEBUG": "🔵 DEBUG",
        "INFO": "✅ INFO",
        "WARNING": "⚠️  WARN",
        "ERROR": "❌ ERROR",
        "CRITICAL": "🚨 CRIT",
    }

    def format(self, record):
        # Add emoji and color to level name
        level_emoji = self.EMOJIS.get(record.levelname, record.levelname)
        level_color = self.COLORS.get(record.levelname, "")

        # Format message with better readability
        if hasattr(record, "getMessage"):
            message = record.getMessage()
        else:
            message = record.msg

        # Extract module name cleanly
        module = record.name.split(".")[-1]

        # Format the log line
        timestamp = self.formatTime(record, "%H:%M:%S")
        colored_level = f"{level_color}{level_emoji}{self.RESET}"

        log_line = f"{timestamp} {colored_level} [{module}] {message}"

        return log_line


def setup_logging(level=logging.INFO):
    """Configure logging with colored output."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler with custom formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    formatter = ColoredFormatter()
    console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)

    return root_logger

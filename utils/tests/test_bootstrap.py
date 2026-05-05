import logging

from release_service_utils.bootstrap import setup_logger


def test_setup_logger():
    """Test that the logger is set up correctly."""
    setup_logger(level=logging.DEBUG)
    logger = setup_logger.__globals__["logging"].getLogger()
    assert logger.level == logging.DEBUG

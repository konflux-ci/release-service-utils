import logging
import logging.config

logconfig = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"simple": {"format": "%(asctime)s - %(levelname)s - %(message)s"}},
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {"sbom": {"level": "DEBUG"}},
    "root": {"level": "WARNING", "handlers": ["stdout"]},
}


def setup_sbom_logger():
    logging.config.dictConfig(config=logconfig)


def get_sbom_logger():
    return logging.getLogger("sbom")

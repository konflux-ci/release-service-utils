import logging
import logging.config

logconfig = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"simple": {"format": "%(asctime)s - %(levelname)s - %(message)s"}},
    "handlers": {
        "stderr": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stderr",
        }
    },
    "loggers": {"sbom": {"level": "DEBUG"}},
    "root": {"level": "WARNING", "handlers": ["stderr"]},
}


def setup_sbom_logger() -> None:
    logging.config.dictConfig(config=logconfig)


def get_sbom_logger() -> logging.Logger:
    return logging.getLogger("sbom")

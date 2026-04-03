import logging
import os
import sys


class NetFleetLogger:
    _loggers = {}

    @classmethod
    def get_logger(
        cls,
        component_name: str
    ) -> logging.Logger:
        if component_name in cls._loggers:
            return cls._loggers[component_name]

        logger = logging.getLogger(component_name)
        logger.setLevel(
            getattr(
                logging,
                os.getenv("LOG_LEVEL", "INFO")
            )
        )

        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                fmt=(
                    "%(asctime)s | %(levelname)-8s | "
                    "%(name)-20s | %(message)s"
                ),
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        cls._loggers[component_name] = logger
        return logger


def get_logger(component_name: str) -> logging.Logger:
    return NetFleetLogger.get_logger(component_name)

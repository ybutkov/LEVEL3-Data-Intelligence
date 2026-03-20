from src.config.config_properties import get_ConfigProperties
import logging
import sys
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

def project_relative(path):
    try:
        return str(Path(path).resolve().relative_to(PROJECT_ROOT))
    except Exception:
        return path
    
class SpringColorFormatter(logging.Formatter):

    RESET = "\033[0m"

    COLORS = {
        logging.DEBUG: "\033[36m",     # cyan
        logging.INFO: "\033[32m",      # green
        logging.WARNING: "\033[93m",   # yellow
        logging.ERROR: "\033[31m",     # red
        logging.CRITICAL: "\033[41m",  # red background
    }

    FILE_COLOR = "\033[35m"  # magenta

    # def shorten_module(self, name: str) -> str:
    #     if len(name) < 15:
    #         return name
    #     parts = name.split(".")
    #     if len(parts) <= 1:
    #         return name
    #     return ".".join(p[0] for p in parts[:-1]) + "." + parts[-1]
    
    def shorten_path(self, path: str) -> str:
        if len(path) < 35:
            return path
        p = Path(path)
        parts = list(p.parts)
        if len(parts) <= 1:
            return p.name
        folders = parts[:-1]
        file = parts[-1]
        short_folders = [f[0] for f in folders]
        return ".".join(short_folders + [file])
    
    def format(self, record: logging.LogRecord) -> str:

        level_color = self.COLORS.get(record.levelno, self.RESET)
        # TODO : we add msecs to all types of datefmt. Fix formatting to avoid this
        time = f"{self.formatTime(record, self.datefmt)}.{record.msecs:03.0f}"
        level = f"{level_color}{record.levelname:<8}{self.RESET}"
        relative_path= project_relative(record.pathname)
        path = self.shorten_path(relative_path)
        file = f"{self.FILE_COLOR}{path:<35}{self.RESET}"
        line = f"{record.lineno:<4}"
        message = record.getMessage()
        return (
            f"{time} | "
            f"{level} | "
            f"{file}:{line} | "
            f"{message}"
        )

class JSONFormatter(logging.Formatter):
   def format(self, record: logging.LogRecord) -> str:
       log_record = {
           'timestamp': self.formatTime(record, self.datefmt),
           'level': record.levelname,
           'message': record.getMessage(),
           'logger': record.name,
           'line': record.lineno,
       }
       return json.dumps(log_record)

def get_logger(name: str = "lufthansa_ingestion") -> logging.Logger:
    logger = logging.getLogger(name)
    configProperties = get_ConfigProperties()

    if logger.hasHandlers():
        return logger
    if configProperties is None:
        level = "INFO"
        datefmt = "%Y-%m-%dT%H:%M:%S"
    else:
        level = configProperties.logger.level.upper()
        datefmt = configProperties.logger.datetime_format
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
   
    handler.setLevel(level)

    formatter = SpringColorFormatter(datefmt=datefmt)
    # formatter = logging.Formatter(
    #     "%(asctime)-23s | %(levelname)-8s | %(filename)-30s: %(lineno)-4d | %(message)s"
    # )
    # formatter = JSONFormatter(datefmt='%Y-%m-%dT%H:%M:%S')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False
    logger.info(f"Logger configured with level {level}")
    return logger
    
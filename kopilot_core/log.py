import logging
import logging.handlers
from pathlib import Path
from typing import List

VERBOSE_FORMAT = "%(levelname)s %(asctime)s %(name)s %(process)d %(thread)d %(message)s"


def configure_logging(
    components: List[str],
    log_dir: str,
    level: str = "INFO",
    rotation_bytes: int = 10 * 1024 * 1024,
    rotation_backups: int = 5,
):
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(VERBOSE_FORMAT)

    def make_file_handler(filename: str, file_level):
        h = logging.handlers.RotatingFileHandler(
            log_path / filename,
            maxBytes=rotation_bytes,
            backupCount=rotation_backups,
            encoding="utf-8",
        )
        h.setLevel(file_level)
        h.setFormatter(formatter)
        return h
    
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)

    errors_handler = make_file_handler("errors.log", logging.ERROR)

    root_file = make_file_handler("main.log", level)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(root_file)
    root.addHandler(errors_handler)

    for name in components:
        comp_file = make_file_handler(f"{name}.log", level)
        comp = logging.getLogger(name)
        comp.setLevel(level)
        comp.handlers.clear()
        comp.addHandler(console)
        comp.addHandler(comp_file)
        comp.addHandler(errors_handler)
        comp.propagate = False
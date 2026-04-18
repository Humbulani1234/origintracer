import logging
from pathlib import Path

logger = logging.getLogger("origintracer")


def setup_file_logging(
    debug: bool = False, log_dir: str = "logs"
) -> None:
    if any(
        isinstance(h, logging.FileHandler)
        for h in logger.handlers
    ):
        return

    path = Path(log_dir)
    path.mkdir(exist_ok=True)

    file_handler = logging.FileHandler(path / "origintracer.log")
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
        )
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
        )
    )

    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

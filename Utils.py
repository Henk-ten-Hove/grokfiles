# utils.py
import os
import logging
import socket
from datetime import datetime, timezone

def setup_logging(log_file: str, debug: bool = False):
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ]
    )

def check_network() -> bool:
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=2)
        return True
    except OSError:
        return False

def validate_code(file_path: str):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            compile(f.read(), file_path, "exec")
    except Exception as e:
        logging.error(f"Code validation failed for {file_path}: {e}")
        raise

def archive_files(data_dir: str, archive_dir: str, retention_days: int):
    now = datetime.now(timezone.utc)
    for filename in os.listdir(data_dir):
        if not filename.endswith(".jsonl.gz"):
            continue
        file_path = os.path.join(data_dir, filename)
        try:
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)
            age_days = (now - file_mtime).days
            if age_days >= retention_days:
                archive_path = os.path.join(archive_dir, filename)
                os.makedirs(archive_dir, exist_ok=True)
                os.rename(file_path, archive_path)
                logging.info(f"Archived {filename} to {archive_path}")
        except Exception as e:
            logging.error(f"Error archiving {filename}: {e}")
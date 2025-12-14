# config.py
from pathlib import Path

# === CẤU HÌNH CHUNG ===
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
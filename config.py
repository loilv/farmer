# config.py
import os
from pathlib import Path
from decimal import Decimal

# === CẤU HÌNH CHUNG ===
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# === THÔNG SỐ GIAO DỊCH ===
MIN_PROFIT = Decimal('0.35')
FEE_RATE = Decimal('0.0008')
START_USDT = 20
TEST_MODE = True

# === WEBSOCKET ===
RECONNECT_DELAY = 5

# === ASYNCIO ===
SCAN_DELAY = 0.05
ORDER_DELAY = 0.2
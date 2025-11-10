import sys
import os

# Thêm đường dẫn để import các module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config_manager import Config
from core.bot import CandlePatternScannerBot
from core.binance_client import BinanceOrderWatcher


def main():
    """Hàm chính khởi chạy bot"""
    print("🚀 Khởi động Candle Pattern Scanner Bot...")
    
    try:
        # Tải cấu hình
        config = Config('config/config.yaml')
        
        # Khởi tạo và chạy bot
        bot = CandlePatternScannerBot(config)
        bot.start()

        
    except Exception as e:
        raise
        print(f"❌ Lỗi khởi động bot: {e}")
        # sys.exit(1)


if __name__ == "__main__":
    main()
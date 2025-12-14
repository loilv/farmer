# main.py
import logging
from core.bot_pro import BotPro
from dotenv import load_dotenv
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    load_dotenv()
    
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")
    demo = os.getenv("DEMO", "false").lower() in ("true", "1", "yes")
    
    if not api_key or not api_secret:
        logger.error("Thiếu API Key! Tạo file .env với API_KEY và API_SECRET")
        return

    bot = BotPro(api_key, api_secret, demo=demo)
    bot.start()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Dừng bot...")
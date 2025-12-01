# main.py
import asyncio
from core.bot_pro import BotPro
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    if not os.getenv("API_KEY") or not os.getenv("API_SECRET"):
        print("Thiếu API Key! Tạo file .env")
        return

    bot = BotPro(os.getenv("API_KEY"), os.getenv("API_SECRET"))
    bot.start()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDừng bot...")
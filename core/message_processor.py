import queue
import time
import logging


class MessageProcessor:
    def __init__(self, config, pattern_scanner):
        self.config = config
        self.pattern_scanner = pattern_scanner
        self.running = False

    def start(self):
        """Bắt đầu xử lý message"""
        self.running = True
        logging.info("🔄 Bắt đầu xử lý message queue...")
        
        while self.running:
            try:
                message = self.pattern_scanner.message_queue.get(timeout=1)
                self.pattern_scanner.process_message(message)
            except queue.Empty:
                continue
            except Exception as e:
                logging.info(f"Lỗi xử lý queue: {e}")

    def stop(self):
        """Dừng xử lý message"""
        self.running = False
        logging.info("✅ Đã dừng message processor")
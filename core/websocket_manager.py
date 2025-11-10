import websocket
import threading
import time
import logging
import ssl
from .order_manager import OrderBinanceManager


class WebSocketManager:
    def __init__(self, config, pattern_scanner):
        self.config = config
        self.pattern_scanner = pattern_scanner
        self.websocket_connections = {}
        self.user_data_websocket = None
        self.running = False
        self.binance = OrderBinanceManager(config)

    def create_websocket_url(self, symbols):
        """Tạo URL WebSocket cho nhiều symbols"""
        if len(symbols) == 1:
            return f"{self.config.websocket_base_url}/ws/{symbols[0].lower()}@kline_{self.config.timeframe}"
        else:
            streams = [f"{symbol.lower()}@kline_{self.config.timeframe}" for symbol in symbols]
            streams_param = "/".join(streams)
            return f"{self.config.websocket_base_url}/stream?streams={streams_param}"

    def start_websocket_connection(self, symbols_batch):
        """Khởi động kết nối WebSocket"""
        if not symbols_batch:
            return

        ws_url = self.create_websocket_url(symbols_batch)
        connection_id = f"conn_{len(self.websocket_connections)}"

        logging.info(f"🔗 Kết nối WebSocket {connection_id} với {len(symbols_batch)} symbols")

        def on_message(ws, message):
            self.pattern_scanner.message_queue.put(message)

        def on_error(ws, error):
            logging.info(f"WebSocket error ({connection_id}): {error}")

        def on_close(ws, close_status_code, close_msg):
            logging.warning(f"WebSocket connection closed ({connection_id})")
            if self.running:
                time.sleep(self.config.reconnect_delay)
                self.start_websocket_connection(symbols_batch)

        def on_open(ws):
            logging.info(f"✅ WebSocket connected ({connection_id}) for {len(symbols_batch)} symbols")

        ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )

        self.websocket_connections[connection_id] = {
            'ws': ws,
            'symbols': symbols_batch
        }

        def run_websocket():
            while self.running:
                try:
                    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
                except Exception as e:
                    logging.info(f"WebSocket exception ({connection_id}): {e}")
                    time.sleep(self.config.reconnect_delay)

        ws_thread = threading.Thread(target=run_websocket, daemon=True)
        ws_thread.start()

    def start_connections(self):
        """Bắt đầu tất cả kết nối WebSocket"""
        self.running = True

        # Khởi động các kết nối market data
        all_symbols = list(self.pattern_scanner.symbols_data.keys())

        # Chia nhỏ symbols
        batch_size = 50
        for i in range(0, len(all_symbols), batch_size):
            batch_symbols = all_symbols[i:i + batch_size]
            logging.info(f"Khởi động connection với {len(batch_symbols)} symbols")
            self.start_websocket_connection(batch_symbols)
            time.sleep(1)

    def stop(self):
        """Dừng tất cả kết nối WebSocket"""
        self.running = False
        for conn_data in self.websocket_connections.values():
            try:
                conn_data['ws'].close()
            except:
                pass
        if self.user_data_websocket:
            try:
                self.user_data_websocket.close()
            except:
                pass
        logging.info("✅ Đã đóng tất cả WebSocket connections")
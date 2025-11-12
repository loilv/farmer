import threading
import time
import signal
import logging
from utils.logger import setup_logging
from .binance_client import BinanceOrderWatcher
from .order_manager import OrderBinanceManager
import queue


class CandlePatternScannerBot:
    def __init__(self, config):
        self.config = config
        self.running = False
        self.message_queue = queue.Queue()
        # Khởi tạo các component
        self.setup_logging()
        self.binance_watcher = BinanceOrderWatcher(config)
        self.symbol_scanner = {}
        self.order_manager = OrderBinanceManager(config)
        self.position = {}
        self.get_position()
        self.last_time = time.time()
        self.trailing_stop = {}

    def get_position(self):
        positions = self.binance_watcher.client.futures_position_information()
        for p in positions:
            self.position[p["symbol"]] = p
        logging.info(f"Currenct position: {self.position}")

    def setup_logging(self):
        """Thiết lập hệ thống logging"""
        setup_logging(self.config)

    def get_symbol_stream(self):
        symbols = self.symbol_scanner.keys()
        return [f'{s.lower()}@kline_{self.config.timeframe}' for s in symbols]

    def remove_non_ascii_symbols(self, symbols):
        import re
        return [s for s in symbols if re.match(r'^[A-Za-z0-9_]+$', s)]

    def get_signal_symbol_stream(self):
        data = self.binance_watcher.get_top_strong_movers()
        symbols = self.remove_non_ascii_symbols(data)
        logging.info(f"Symbols: {symbols}")
        return [f'{s.lower()}@kline_{self.config.signal_time_frame}' for s in symbols]

    def start(self):
        """Bắt đầu bot"""
        self.running = True

        # Xử lý tín hiệu dừng
        def signal_handler(sig, frame):
            logging.info("🛑 Nhận tín hiệu dừng...")
            self.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logging.info("✅ Bot đã khởi động, đang chờ tín hiệu...")

        self.binance_watcher.twm.start()
        self.binance_watcher.twm.start_futures_user_socket(callback=self._handle_user_stream)
        signal_streams = self.get_signal_symbol_stream()

        self.binance_watcher.twm.start_futures_multiplex_socket(
            callback=self._handle_multi_signal_kline, streams=signal_streams)
        self.binance_watcher.twm.start_futures_multiplex_socket(
            callback=self._handle_mark_price,
            streams=['!markPrice@arr']
        )
        threading.Thread(target=self.binance_watcher.twm.join, daemon=True).start()
        logging.info("🚀 WebSocket user stream đã khởi chạy...")

        try:
            while self.running:
                self._handle_multi_kline_order_queue()
        except KeyboardInterrupt:
            pass

    def _handle_mark_price(self, msg):

        activate_profit = 0.5
        stop_loss = -0.15

        for coin in (d for d in msg['data'] if d['s'] in self.position):
            symbol = coin['s']
            mark_price = float(coin['p'])
            pos = self.position.get(symbol)
            if not pos:
                continue

            entry = float(pos['entryPrice'])
            amt = float(pos['positionAmt'])
            trailing = self.trailing_stop.get(symbol, {})
            sl = trailing.get('sl', 0)

            if amt == 0:
                continue

            pnl = round((mark_price - entry) * amt, 2)
            print(f"✅ {symbol} lãi {pnl} USDT")

            if pnl > 0 and pnl >= 0.15:
                result = "💸 WIN"
                logging.info(f"{result} {symbol} | PNL: {pnl} USDT")
                side = 'BUY' if amt > 0 else 'SELL'
                self.binance_watcher.close_position(
                    symbol=symbol
                )
                self.position[symbol] = {}
                if symbol in self.trailing_stop:
                    self.trailing_stop.pop(symbol, None)
                continue

            if pnl < 0 and pnl <= stop_loss:
                result = "LOSS ❌"
                logging.info(f"{result} {symbol} | PNL: {pnl} USDT")
                self.binance_watcher.close_position(
                    symbol=symbol
                )

                self.position[symbol] = {}

                # self.position.pop(symbol, None)
                continue

    def _handle_user_stream(self, msg):
        """Xử lý sự kiện WebSocket từ user stream"""
        if msg['e'] == 'ORDER_TRADE_UPDATE':
            data = msg['o']
            symbol = data['s']
            order_id = int(data['i'])
            status = data['X']
            execution_type = data['x']
            side = data['S']
            quantity = float(data['q'])

            # Khi lệnh entry khớp
            if status == 'FILLED' and execution_type == 'TRADE':
                logging.info(f"✅ Entry {symbol} đã khớp hoàn toàn (OrderID: {order_id})")
                logging.info(f"✅ MSG data: {data})")
                entry_price = float(data['ap'])
                if data['R']:
                    if data['ot'] == 'TAKE_PROFIT_MARKET':
                        self.position.pop(symbol, None)
                    return

                if data['o'] == 'LIMIT':
                    try:
                        side = 'SELL' if data['S'] == 'BUY' else 'BUY'
                        p_side = data['ps']

                        capital = 0.5
                        leverage = 20
                        expected_profit = 0.15

                        position_value = capital * leverage
                        target_pct = expected_profit / position_value

                        mark_price = float(self.binance_watcher.client.futures_mark_price(symbol=symbol)['markPrice'])
                        logging.info(f"📌 Current Mark Price: {mark_price}")

                        if side == "BUY":  # LONG -> TP phải cao hơn mark price
                            tp_price = min(entry_price * (1 - target_pct), mark_price * (1 - target_pct))  # +0.2%
                        else:  # SELL -> TP phải thấp hơn mark price
                            tp_price = max(entry_price * (1 + target_pct), mark_price * (1 + target_pct))  # -0.2%

                        price = self.binance_watcher._format_price(symbol, tp_price * 0.998 if side == "SELL" else tp_price * 1.002)
                        tp_price = self.binance_watcher._format_price(symbol, tp_price)
                        quantity = self.binance_watcher._format_quantity(symbol, abs(quantity))

                        logging.info(f"🎯 Setting TP: {tp_price} ({target_pct * 100:.2f}%)")

                        self.binance_watcher.client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            positionSide=p_side,
                            type="TAKE_PROFIT",
                            price=price,
                            stopPrice=tp_price,
                            quantity=abs(quantity),
                            workingType="MARK_PRICE"
                        )
                        logging.info(f"✅ TP Order placed @ {tp_price}")
                    except Exception as e:
                        logging.error(f'❌ TP ERROR: {str(e)}')

                self.get_position()

    def _handle_multi_signal_kline(self, data):
        self.message_queue.put(data)

    def _handle_multi_kline_order_queue(self):
        data = self.message_queue.get()
        now = time.time()
        if now - self.last_time >= 300:
            logging.info(f"Live: {data}")
            self.last_time = now

        if 'data' not in data:
            return

        kline = data['data']['k']
        symbol = data['data']['s']

        open_price = round(float(kline['o']), 5)
        close_price = round(float(kline['c']), 5)
        h_price = round(float(kline['h']), 5)
        l_price = round(float(kline['l']), 5)
        percentage_change = round(((close_price - open_price) / open_price) * 100, 2)
        percentage_h = round(((h_price - close_price) / h_price) * 100, 2)
        percentage_l = round(((close_price - l_price) / l_price) * 100, 2)

        precent_sell = 0
        precent_buy = 0

        if percentage_change > 0:
            precent_sell = round((close_price - open_price) / (h_price - open_price) * 100) if (h_price - open_price) > 0 else 0

        if percentage_change < 0:
            precent_buy = round((open_price - close_price) / (open_price - l_price) * 100) if (open_price - l_price) > 0 else 0

        # print(f'Check tín hiệu {symbol} | open: {open_price} | close: {close_price} | h: {h_price} | l: {l_price} | body: {percentage_change}% | ratio_sell: {precent_sell}% | ratio_buy: {precent_buy}%')

        if abs(percentage_change) < 3:
            return

        print(
            f'Check tín hiệu {symbol} | open: {open_price} | close: {close_price} | h: {h_price} | l: {l_price} | body: {percentage_change}% | ratio_sell: {precent_sell}% | ratio_buy: {precent_buy}%')

        if 99 < abs(precent_sell) <= 100 or 99 < abs(precent_buy) <= 100:
            side = "BUY" if percentage_change > 0 else "SELL"
            if not self.can_order(symbol, side):
                return

            adjust = 1.0005 if side == "BUY" else 0.9995
            entry_price = close_price * adjust

            qty = self.order_manager.calculate_position_size(symbol, entry_price)

            logging.info(f"[ENTRY] Cùng chiều: {side} {symbol} | Qty: {qty} | Price: {entry_price:.5f}")

            self.position[symbol] = {}
            self.trailing_stop[symbol] = {"counter": True}

            self.binance_watcher.create_entry_order(
                symbol, side, round(entry_price, 5), qty
            )
            return

        if 80 < abs(precent_sell) <= 90 or 80 < abs(precent_buy) <= 90:
            side = "SELL" if percentage_change > 0 else "BUY"
            if not self.can_order(symbol, side):
                return

            adjust = 0.9995 if side == "BUY" else 1.0005
            entry_price = close_price * adjust

            qty = self.order_manager.calculate_position_size(symbol, entry_price)

            logging.info(f"[ENTRY] Ngược chiều: {side} {symbol} | Qty: {qty} | Price: {entry_price:.5f}")

            self.position[symbol] = {}
            self.trailing_stop[symbol] = {"counter": True}

            self.binance_watcher.create_entry_order(
                symbol, side, round(entry_price, 5), qty
            )
            return

        # t_open = kline['t'] / 1000
        # now = time.time()
        # candle_duration = now - t_open

        # ✅ Exit: khi nến đóng
        if kline['x'] and symbol in self.position:
            pos = self.position.get(symbol, False)
            amt = float(pos.get('positionAmt', 0))
            if amt != 0:
                side = 'BUY' if amt > 0 else 'SELL'
                entry_price = float(pos['entryPrice'])

                if side == 'BUY':
                    pnl = round((close_price - entry_price) * amt, 2)
                else:
                    pnl = round((entry_price - close_price) * amt, 2)

                if pnl > 0 and side == 'BUY' or pnl < 0 and side == 'SELL':
                    result = "💸 WIN"
                    logging.info(f"{result} {symbol} | PNL: {abs(pnl)} USDT | Side: {side}")
                elif pnl < 0 and side == 'BUY' or pnl > 0 and side == 'SELL':
                    result = "LOSS ❌"
                    logging.info(f"{result} {symbol} | PNL: {pnl} USDT | Side: {side}")

                self.binance_watcher.close_position(symbol=symbol)

            if symbol in self.position:
                self.binance_watcher.client.futures_cancel_all_open_orders(symbol=symbol)

            self.position.pop(symbol, None)
            self.trailing_stop.pop(symbol, None)

            return

    def can_order(self, symbol, type):

        if len(self.position.keys()) >= 2:
            return False

        if symbol in self.position:
            return False

        curr_position = self.position.get(symbol)
        if not curr_position:
            return True

        # self.binance_watcher.client.futures_cancel_all_open_orders(symbol=symbol)
        if type == 'BUY' and float(curr_position.get('positionAmt', 0)) >= 0:
            return False
        if type == 'SELL' and float(curr_position.get('positionAmt', 0)) <= 0:
            return False
        return True

    def stop(self):
        """Dừng bot"""
        logging.info("⏹️ Đang dừng scanner...")
        self.binance_watcher.twm.stop()
        self.running = False

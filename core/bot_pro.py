import logging
import signal
import threading
import time
from collections import deque
from typing import Optional, Dict, Any, Tuple

from binance import ThreadedWebsocketManager
from binance.enums import *

from .binance_core import BinanceCore

logger = logging.getLogger(__name__)


class BotPro:
    def __init__(self, api_key: str, secret_key: str, demo: bool = False):
        self.api_key = api_key
        self.secret_key = secret_key
        self.running = True
        self.demo = demo if isinstance(demo, bool) else str(demo).lower() in ('true', '1', 'yes')
        self.binance = BinanceCore(self.api_key, self.secret_key, self.demo)

        self.trade = {
            'usdt': 0.5,
            'leverage': 20,
        }

        self.signal = {
            'timeframe_signal': "1m",
            'timeframe_check': "1h",
            'oc_signal': 1,
            'oc_signal_realtime': 1.5,
            'oc_check_min': 6,
            'kline_signal': 3,
            'kline_check': 3,
        }

        self.risk = {
            'tp': (self.trade['usdt'] * 1.5),
            'sl': -1 * (self.trade['usdt'] * 1.5),
        }

        self.cache = {
            'klines_check_ttl': 60,
        }

        self.ws = {
            'reconnect_delay': 5,
            'max_reconnect_attempts': 10,
        }

        self.dca = {
            'profit_threshold': 0.2,
            'profit_multiplier': 2,
        }

        symbols = self.get_top_coins()
        self.all_symbols_signal = {sym: deque(maxlen=self.signal['kline_signal']) for sym in symbols}
        self.klines_check_cache: Dict[str, Any] = {}  # Cache klines timeframe_check
        self.klines_check_cache_time: Dict[str, float] = {}  # Thời gian cache
        self.last_realtime_signal_candle: Dict[str, int] = {}

        # Queue chứa message WebSocket
        self.twm = ThreadedWebsocketManager(
            api_key=self.api_key,
            api_secret=self.secret_key,
            testnet=self.demo,
            max_queue_size=50000
        )

        self.orders = dict()
        self.tp_orders = dict()
        self.sl_orders = dict()
        self.dca_done = dict()
        self.dca_orders = dict()
        self.get_current_orders()

    def _clear_symbol_state(self, symbol: str) -> None:
        self.tp_orders.pop(symbol, None)
        self.sl_orders.pop(symbol, None)
        self.orders.pop(symbol, None)
        self.dca_done.pop(symbol, None)
        self.dca_orders.pop(symbol, None)
        self.all_symbols_signal[symbol] = deque(maxlen=self.signal['kline_signal'])

    # ------------------------------------------
    # START BOT
    # ------------------------------------------
    def start(self):
        # Bắt tín hiệu stop
        def signal_handler(sig, frame):
            logger.info("🛑 STOP signal nhận...")
            self.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        reconnect_count = 0
        while self.running:
            try:
                self._start_websocket()
                reconnect_count = 0
                
                # Main loop giữ bot chạy
                while self.running:
                    time.sleep(0.1)
                    
            except Exception as e:
                reconnect_count += 1
                logger.error(f"WebSocket lỗi: {e}")
                
                if reconnect_count >= self.ws['max_reconnect_attempts']:
                    logger.error(f"Đã thử kết nối lại {self.ws['max_reconnect_attempts']} lần. Dừng bot.")
                    self.running = False
                    break
                
                logger.info(f"Đang kết nối lại... (lần {reconnect_count}/{self.ws['max_reconnect_attempts']})")
                self._cleanup_websocket()
                time.sleep(self.ws['reconnect_delay'])

    def _start_websocket(self):
        """Khởi tạo và bắt đầu WebSocket"""
        self.twm = ThreadedWebsocketManager(
            api_key=self.api_key,
            api_secret=self.secret_key,
            testnet=self.demo,
            max_queue_size=50000
        )
        self.twm.start()
        
        streams = self.get_top_coins()
        socket_streams = [f"{c.lower()}@kline_{self.signal['timeframe_signal']}" for c in streams]

        # Hàm chia batch
        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        # Tách mỗi batch 50
        batches = list(chunk_list(socket_streams, 50))
        for batch in batches:
            self.twm.start_futures_multiplex_socket(
                callback=self._handle_multi_signal_kline, streams=batch
            )
            logger.info(f'Đang theo dõi {batch}')
        
        # Theo dõi khớp lệnh
        self.twm.start_futures_user_socket(callback=self._handle_user_stream)
        threading.Thread(target=self.twm.join, daemon=True).start()
        logger.info("WebSocket đã kết nối thành công")

    def _cleanup_websocket(self):
        """Dọn dẹp WebSocket trước khi kết nối lại"""
        try:
            if self.twm:
                self.twm.stop()
        except Exception as e:
            logger.warning(f"Lỗi khi dừng WebSocket: {e}")

    def get_current_orders(self):
        orders = self.binance.get_all_positions()
        self.orders = dict()
        for order in orders:
            self.orders[order["symbol"]] = order
        logger.info(f"Current orders: {self.orders}")
        return self.orders

    def _handle_user_stream(self, msg):
        if msg['e'] == 'ORDER_TRADE_UPDATE':
            data = msg['o']
            symbol = data['s']
            order_id = int(data['i'])
            status = data['X']
            execution_type = data['x']
            order_type = data['ot']

            # Khi lệnh entry khớp
            if status == 'FILLED' and execution_type == 'TRADE':
                logger.info(f"Entry {symbol} đã khớp hoàn toàn (OrderID: {order_id}) lệnh {data['o']}")
                logger.info(f"MSG data: {data}")

                had_position = symbol in self.orders

                dca_order_id = self.dca_orders.get(symbol)
                if dca_order_id is not None and order_id == int(dca_order_id):
                    try:
                        self.get_current_orders()
                        position = self.orders.get(symbol)
                        if not position:
                            self.dca_orders.pop(symbol, None)
                            return

                        try:
                            qty = float(position.get('positionAmt', 0))
                            entry_be = float(position.get('breakEvenPrice', 0))
                        except (ValueError, TypeError):
                            self.dca_orders.pop(symbol, None)
                            return

                        if qty == 0 or entry_be == 0:
                            self.dca_orders.pop(symbol, None)
                            return

                        stop_side = "SELL" if qty > 0 else "BUY"

                        try:
                            if symbol in self.sl_orders:
                                self.binance.cancel_order(symbol, self.sl_orders[symbol])
                                self.sl_orders.pop(symbol, None)
                        except Exception as e:
                            logger.warning(f"Lỗi hủy SL cũ trước khi đặt hòa vốn {symbol}: {e}")

                        order = self.binance.create_order_stop_loss_be(
                            symbol=symbol,
                            side=stop_side,
                            price=entry_be,
                            quantity=abs(qty)
                        )

                        if order:
                            self.sl_orders[symbol] = order
                    finally:
                        self.dca_orders.pop(symbol, None)

                is_reduce_only = bool(data.get('R')) or bool(data.get('reduceOnly'))
                is_close_position = bool(data.get('cp')) or bool(data.get('closePosition'))
                should_clear = (
                    order_type in ('TAKE_PROFIT_MARKET', 'STOP_MARKET', 'TAKE_PROFIT', 'STOP')
                    or (order_type == 'MARKET' and (is_reduce_only or is_close_position))
                )

                if should_clear:
                    self.binance.clear_order(symbol)
                    self._clear_symbol_state(symbol)

                self.get_current_orders()

                if had_position and symbol not in self.orders:
                    self.binance.clear_order(symbol)
                    self._clear_symbol_state(symbol)


    def _handle_multi_signal_kline(self, msg: Dict[str, Any]) -> None:
        """Xử lý dữ liệu kline từ WebSocket"""
        data = msg.get("data", {})
        if not data:
            return
            
        symbol = data.get("s")
        kline = data.get("k")
        if not kline or not symbol:
            return
            
        try:
            close_price = float(kline.get("c", 0))
            max_price = float(kline.get("h", 0))
            low_price = float(kline.get("l", 0))
        except (ValueError, TypeError):
            return
        self.handle_pnl(symbol, close_price, max_price, low_price)

        try:
            open_price = float(kline.get("o", 0))
            candle_start = int(kline.get("t", 0))
        except (ValueError, TypeError):
            open_price = 0
            candle_start = 0

        if (
            open_price > 0
            and candle_start > 0
            and symbol not in self.orders
            and symbol in self.all_symbols_signal
        ):
            last_candle = self.last_realtime_signal_candle.get(symbol)
            if last_candle != candle_start:
                oc_signal_pct = ((close_price - open_price) / open_price) * 100
                if abs(oc_signal_pct) >= self.signal['oc_signal_realtime']:
                    result = self._check_realtime_signal(symbol, close_price, oc_signal_pct)
                    if result:
                        self.last_realtime_signal_candle[symbol] = candle_start
                        self._place_entry_order(symbol, result[0], result[1], result[2])

        if kline.get("x"):
            if symbol in self.orders:
                try:
                    orders = self.binance.get_limit_orders(symbol=symbol)
                    for order in orders:
                        self.binance.cancel_order(symbol, order)
                        self.orders.pop(symbol, None)
                    self.get_current_orders()
                except Exception as e:
                    logger.warning(f"Lỗi hủy lệnh {symbol}: {e}")

        

    def _get_check_open_price(self, symbol: str) -> Optional[float]:
        now = time.time()
        if symbol in self.klines_check_cache and (now - self.klines_check_cache_time.get(symbol, 0)) < self.cache['klines_check_ttl']:
            klines = self.klines_check_cache[symbol]
        else:
            klines = self.binance.get_klines(symbol, self.signal['timeframe_check'], 3)
            if klines:
                self.klines_check_cache[symbol] = klines
                self.klines_check_cache_time[symbol] = now

        if not klines:
            return None

        try:
            open_check = float(klines[0][1])
        except (ValueError, TypeError, IndexError):
            return None

        if open_check == 0:
            return None
        return open_check


    def _check_realtime_signal(self, symbol: str, close_price: float, oc_signal_pct: float) -> Optional[Tuple[float, float, str]]:
        open_check_price = self._get_check_open_price(symbol)
        if open_check_price is None:
            return None

        check_change_pct = ((close_price - open_check_price) / open_check_price) * 100
        logger.info(
            f"{symbol} | {self.signal['timeframe_signal']} OC(now): {oc_signal_pct:.2f}% | {self.signal['timeframe_check']} OpenΔ: {check_change_pct:.2f}%"
        )

        if (
            oc_signal_pct >= self.signal['oc_signal_realtime']
            and check_change_pct <= -self.signal['oc_check_min']
        ):
            return close_price, abs(oc_signal_pct), 'BUY'

        if (
            oc_signal_pct <= -self.signal['oc_signal_realtime']
            and check_change_pct >= self.signal['oc_check_min']
        ):
            return close_price, abs(oc_signal_pct), 'SELL'

        return None


    def _place_entry_order(self, symbol: str, price: float, change: float, side: str) -> None:
        logger.info(f"Signal {side} {symbol} | Change: {change:.2f}%")

        quantity = (self.trade['usdt'] * self.trade['leverage']) / abs(price)
        if not self.binance.can_make_order(symbol):
            return

        order = self.binance.create_order(
            symbol=symbol,
            side=side,
            entry_price=abs(price),
            quantity=quantity,
            order_type=FUTURE_ORDER_TYPE_LIMIT,
        )

        if order:
            self.orders[symbol] = order


    def handle_pnl(self, symbol: str, close_price: float, max_price: float, low_price: float) -> None:
        """Xử lý PNL và đặt TP/SL"""
        
        if symbol not in self.orders:
            return

        position = self.orders[symbol]
        try:
            qty = float(position.get('positionAmt', 0))
            entry = float(position.get('entryPrice', 0))
        except (ValueError, TypeError):
            return
            
        if qty == 0 or entry == 0:
            return

        if qty < 0:
            pnl = (entry - close_price) * abs(qty)
        else:
            pnl = (close_price - entry) * abs(qty)

        pnl_color = "\033[92m" if pnl > 0 else ("\033[91m" if pnl < 0 else "")
        pnl_reset = "\033[0m" if pnl_color else ""
        logger.info(f'{pnl_color}{symbol} PNL: {round(pnl, 2)}{pnl_reset}')
        offset = 0.001

        if (
            pnl > 0
            and pnl >= self.dca['profit_threshold']
            and not self.dca_done.get(symbol)
        ):
            try:
                if qty < 0:
                    dca_side = "SELL"
                else:
                    dca_side = "BUY"

                dca_usdt = self.trade['usdt'] * self.dca['profit_multiplier']
                dca_qty = (dca_usdt * self.trade['leverage']) / abs(close_price)

                order = self.binance.create_order(
                    symbol=symbol,
                    side=dca_side,
                    entry_price=abs(close_price),
                    quantity=dca_qty,
                    order_type=FUTURE_ORDER_TYPE_MARKET,
                )

                if order:
                    self.dca_done[symbol] = True
                    if isinstance(order, dict) and order.get('orderId') is not None:
                        self.dca_orders[symbol] = int(order.get('orderId'))
            except Exception as e:
                logger.warning(f"Lỗi DCA {symbol}: {e}")

        if pnl > 0 and pnl >= self.risk['tp']:
            if symbol not in self.tp_orders:
                if qty < 0:
                    side = "BUY"
                    price = close_price * (1 - offset)
                else:
                    side = "SELL"
                    price = close_price * (1 + offset)

                order = self.binance.create_order_take_profit(
                    symbol=symbol,
                    side=side,
                    price=price,
                    quantity=abs(qty)
                )

                if order:
                    self.tp_orders[symbol] = order

        if pnl < 0 and pnl <= self.risk['sl']:
            if symbol not in self.sl_orders:
                if qty < 0:
                    side = "BUY"
                    price = close_price * (1 + offset)
                else:
                    side = "SELL"
                    price = close_price * (1 - offset)

                logger.info(f'{symbol} price SL: {price}, {close_price}')
                order = self.binance.create_order_stop_loss(
                    symbol=symbol,
                    side=side,
                    price=price,
                    quantity=abs(qty)
                )
                if order:
                    self.sl_orders[symbol] = order

    # ------------------------------------------
    def stop(self):
        logger.info("Đang tắt bot...")
        self.running = False
        
        # Dừng WebSocket nhanh
        try:
            if self.twm:
                self.twm.stop()
        except Exception as e:
            logger.warning(f"Lỗi khi dừng WebSocket: {e}")
        
        logger.info("Bot dừng hoàn toàn.")

    # ------------------------------------------
    # Chọn coin để listen
    # ------------------------------------------
    def get_top_coins(self):
        return self.binance.get_top_volatile_liquid_symbols()

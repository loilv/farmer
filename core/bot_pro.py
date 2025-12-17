import logging
import signal
import threading
import time
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
            'timeframe_check': "15m",
            'oc_signal_realtime': 1,
            'oc_check_min': 8,
        }

        self.risk = {
            'tp': self.trade['usdt'] * 0.5,
            'sl': -self.trade['usdt'] * 1,
            'max_active': 4
        }

        self.cache = {
            'klines_check_ttl': 60,
        }

        self.ws = {
            'reconnect_delay': 5,
            'max_reconnect_attempts': 10,
        }

        self.cooldown = {
            'after_win': 3600,  # 1h cooldown sau khi win
            'bypass_oc_check_min': 13,  # Bypass cooldown nếu oc_check >= ±13%
            'bypass_oc_signal': 3,  # Bypass cooldown nếu oc_signal >= ±3%
            'bypass_max_times': 1,  # Số lần bypass tối đa trong 1 cooldown
        }

        self.symbols = self.get_top_coins()
        self.klines_check_cache: Dict[str, Any] = {}
        self.klines_check_cache_time: Dict[str, float] = {}
        self.last_realtime_signal_candle: Dict[str, int] = {}

        self.twm = ThreadedWebsocketManager(
            api_key=self.api_key,
            api_secret=self.secret_key,
            testnet=self.demo,
            max_queue_size=50000
        )

        self.orders = dict()
        self.tp_orders = dict()
        self.sl_orders = dict()
        self.last_win_time: Dict[str, float] = {}  # Thời gian win cuối của symbol
        self.bypass_count: Dict[str, int] = {}  # Số lần đã bypass cho symbol
        self.get_current_orders()

    def _clear_symbol_state(self, symbol: str) -> None:
        self.tp_orders.pop(symbol, None)
        self.sl_orders.pop(symbol, None)
        self.orders.pop(symbol, None)

    def start(self):
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
        
        socket_streams = [f"{s.lower()}@kline_{self.signal['timeframe_signal']}" for s in self.symbols]

        batches = [socket_streams[i:i+50] for i in range(0, len(socket_streams), 50)]
        for batch in batches:
            self.twm.start_futures_multiplex_socket(
                callback=self._handle_multi_signal_kline, streams=batch
            )
            logger.info(f'Đang theo dõi {len(batch)} symbols')

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
        if msg.get('e') != 'ORDER_TRADE_UPDATE':
            return

        data = msg['o']
        symbol = data['s']
        status = data['X']
        execution_type = data['x']
        order_type = data['ot']

        if status == 'FILLED' and execution_type == 'TRADE':
            logger.info(f"{symbol} khớp lệnh {order_type} | reduceOnly={data.get('R')} | closePosition={data.get('cp')}")
            had_position = symbol in self.orders

            is_reduce_only = data.get('R')
            is_close_position = data.get('cp')
            is_tp = order_type in ('TAKE_PROFIT_MARKET', 'TAKE_PROFIT')
            is_sl = order_type in ('STOP_MARKET', 'STOP')
            should_clear = (
                is_tp or is_sl
                or (order_type == 'MARKET' and (is_reduce_only or is_close_position))
            )

            if is_tp or should_clear:
                self.last_win_time[symbol] = time.time()
                self.bypass_count.pop(symbol, None)  # Reset bypass cho cooldown mới
                logger.info(f"{symbol} WIN - cooldown 1h")

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
        except (ValueError, TypeError):
            return

        self.handle_pnl(symbol, close_price)

        try:
            open_price = float(kline.get("o", 0))
            candle_start = int(kline.get("t", 0))
        except (ValueError, TypeError):
            open_price = 0
            candle_start = 0

        # Kiểm tra cooldown sau khi win
        is_in_cooldown = False
        if symbol in self.last_win_time:
            elapsed = time.time() - self.last_win_time[symbol]
            remaining = self.cooldown['after_win'] - elapsed
            if remaining > 0:
                is_in_cooldown = True

        if (
            open_price > 0
            and candle_start > 0
            and symbol not in self.orders
        ):
            last_candle = self.last_realtime_signal_candle.get(symbol)
            if last_candle != candle_start:
                oc_signal_pct = ((close_price - open_price) / open_price) * 100
                if abs(oc_signal_pct) >= self.signal['oc_signal_realtime']:
                    # Nếu đang cooldown, kiểm tra tín hiệu mạnh để bypass
                    if is_in_cooldown:
                        kline_data = self._get_check_kline_data(symbol)
                        if kline_data:
                            open_check_price, _ = kline_data
                            check_change_pct = ((close_price - open_check_price) / open_check_price) * 100
                            # Bypass cooldown nếu tín hiệu mạnh
                            if abs(check_change_pct) >= self.cooldown['bypass_oc_check_min'] and abs(oc_signal_pct) >= self.cooldown['bypass_oc_signal']:
                                # Kiểm tra số lần bypass đã dùng
                                used_count = self.bypass_count.get(symbol, 0)
                                if used_count < self.cooldown['bypass_max_times']:
                                    logger.info(f"{symbol} BYPASS COOLDOWN ({used_count + 1}/{self.cooldown['bypass_max_times']}) | OC: {oc_signal_pct:.2f}% | Check: {check_change_pct:.2f}%")
                                    result = self._check_realtime_signal(symbol, close_price, oc_signal_pct)
                                    if result:
                                        self.bypass_count[symbol] = used_count + 1
                                        self.last_realtime_signal_candle[symbol] = candle_start
                                        self._place_entry_order(symbol, result[0], result[1], result[2])
                        return
                    
                    result = self._check_realtime_signal(symbol, close_price, oc_signal_pct)
                    if result:
                        self.last_realtime_signal_candle[symbol] = candle_start
                        self._place_entry_order(symbol, result[0], result[1], result[2])

        if kline.get("x") and symbol in self.orders:
            try:
                limit_orders = self.binance.get_limit_orders(symbol=symbol)
                if limit_orders:
                    # Có lệnh LIMIT chưa khớp → hủy tất cả orders và clear state
                    logger.info(f"{symbol} có {len(limit_orders)} lệnh LIMIT chưa khớp, hủy tất cả")
                    self.binance.clear_order(symbol)
                    self._clear_symbol_state(symbol)
                self.get_current_orders()
            except Exception as e:
                logger.warning(f"Lỗi hủy lệnh {symbol}: {e}")

    def _get_check_kline_data(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Trả về (open_price, body) của nến timeframe_check"""
        now = time.time()
        if symbol in self.klines_check_cache and (now - self.klines_check_cache_time.get(symbol, 0)) < self.cache['klines_check_ttl']:
            klines = self.klines_check_cache[symbol]
        else:
            klines = self.binance.get_klines(symbol, self.signal['timeframe_check'], 2)
            if klines:
                self.klines_check_cache[symbol] = klines
                self.klines_check_cache_time[symbol] = now

        if not klines:
            return None

        try:
            open_check = float(klines[0][1])
            close_check = float(klines[0][4])
        except (ValueError, TypeError, IndexError):
            return None

        if open_check == 0:
            return None
        
        body = abs(close_check - open_check)
        return open_check, body


    def _check_realtime_signal(self, symbol: str, close_price: float, oc_signal_pct: float) -> Optional[Tuple[float, float, str]]:
        """Trả về (price, change, side) nếu có signal"""
        kline_data = self._get_check_kline_data(symbol)
        if kline_data is None:
            return None

        open_check_price, _ = kline_data
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
        if len(self.orders) >= self.risk['max_active']:
            return

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


    def handle_pnl(self, symbol: str, close_price: float) -> None:
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
        offset = 0.0005

        if pnl >= self.risk['tp']:
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

        if pnl <= self.risk['sl']:
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

    def stop(self):
        logger.info("Đang tắt bot...")
        self.running = False
        
        try:
            if self.twm:
                self.twm.stop()
        except Exception as e:
            logger.warning(f"Lỗi khi dừng WebSocket: {e}")
        
        logger.info("Bot dừng hoàn toàn.")

    def get_top_coins(self):
        return self.binance.get_top_volatile_liquid_symbols()

import logging
import queue
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
        self.timeframe_signal = "5m"
        self.timeframe_check = "30m"
        self.usdt = 0.5
        self.leverage = 20
        self.oc_signal = 1
        self.oc_signal_realtime = 1.3
        self.oc_check_min = 8
        self.kline_signal = 3
        self.kline_check = 3
        self.tp = (self.usdt * 0.6)
        self.sl = -1 * (self.usdt * 2)
        self.tp_stop = (self.usdt * 0.4)
        self.atr_period = 14
        self.atr_mult = 1.5
        symbols = self.get_top_coins()
        self.all_symbols_signal = {sym: deque(maxlen=self.kline_signal) for sym in symbols}
        self.klines_check_cache: Dict[str, Any] = {}  # Cache klines timeframe_check
        self.klines_check_cache_time: Dict[str, float] = {}  # Thời gian cache
        self.klines_check_cache_ttl = 60  # Cache 60 giây
        self.trailing_stop = dict()
        self.last_realtime_signal_candle: Dict[str, int] = {}
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 10

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
        self.tp_part_orders = dict()
        self.hedge_orders = dict()
        self.get_current_orders()

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
                
                if reconnect_count >= self.max_reconnect_attempts:
                    logger.error(f"Đã thử kết nối lại {self.max_reconnect_attempts} lần. Dừng bot.")
                    self.running = False
                    break
                
                logger.info(f"Đang kết nối lại... (lần {reconnect_count}/{self.max_reconnect_attempts})")
                self._cleanup_websocket()
                time.sleep(self.reconnect_delay)

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
        socket_streams = [f"{c.lower()}@kline_{self.timeframe_signal}" for c in streams]

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

                if order_type in ('TAKE_PROFIT_MARKET', 'STOP_MARKET', 'MARKET', 'TAKE_PROFIT', 'STOP'):
                    self.tp_orders.pop(symbol, None)
                    self.hedge_orders.pop(symbol, None)
                    self.sl_orders.pop(symbol, None)
                    self.orders.pop(symbol, None)
                    self.binance.clear_order(symbol)
                    self.trailing_stop.pop(symbol, None)
                    self.all_symbols_signal[symbol] = deque(maxlen=self.kline_signal)

                self.get_current_orders()

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
                if abs(oc_signal_pct) >= self.oc_signal_realtime:
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

        

    def _get_check_oc_pct(self, symbol: str) -> Optional[float]:
        now = time.time()
        if symbol in self.klines_check_cache and (now - self.klines_check_cache_time.get(symbol, 0)) < self.klines_check_cache_ttl:
            klines = self.klines_check_cache[symbol]
        else:
            klines = self.binance.get_klines(symbol, self.timeframe_check, self.kline_check)
            if klines:
                self.klines_check_cache[symbol] = klines
                self.klines_check_cache_time[symbol] = now

        if not klines or len(klines) < self.kline_check:
            return None

        try:
            open_check = float(klines[0][1])
            close_check = float(klines[-1][4])
        except (ValueError, TypeError, IndexError):
            return None

        if open_check == 0:
            return None
        return ((close_check - open_check) / open_check) * 100


    def _check_realtime_signal(self, symbol: str, close_price: float, oc_signal_pct: float) -> Optional[Tuple[float, float, str]]:
        oc_check_pct = self._get_check_oc_pct(symbol)
        if oc_check_pct is None:
            return None

        logger.info(f"{symbol} | {self.timeframe_signal} OC(now): {oc_signal_pct:.2f}% | {self.timeframe_check} OC: {oc_check_pct:.2f}%")

        if oc_signal_pct >= self.oc_signal_realtime and oc_check_pct < 0 and oc_check_pct < -self.oc_check_min:
            return close_price, abs(oc_signal_pct), 'BUY'

        if oc_signal_pct <= -self.oc_signal_realtime and oc_check_pct > self.oc_check_min:
            return close_price, abs(oc_signal_pct), 'SELL'

        return None


    def _place_entry_order(self, symbol: str, price: float, change: float, side: str) -> None:
        logger.info(f"Signal {side} {symbol} | Change: {change:.2f}%")

        quantity = (self.usdt * self.leverage) / abs(price)
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


    def _calculate_atr(self, symbol: str, interval: str, period: int) -> Optional[float]:
        """Tính ATR (Average True Range) theo klines đã đóng."""
        if period <= 0:
            return None

        klines = self.binance.get_klines(symbol, interval, period + 1)
        if not klines or len(klines) < period + 1:
            return None

        trs = []
        try:
            prev_close = float(klines[0][4])
            for k in klines[1:]:
                high = float(k[2])
                low = float(k[3])
                close = float(k[4])
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close),
                )
                trs.append(tr)
                prev_close = close
        except (ValueError, TypeError, IndexError):
            return None

        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period


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
        offset = (abs(pnl) / 100)

        if pnl > 0 and pnl >= self.tp_stop:
            atr = self._calculate_atr(symbol, self.timeframe_signal, self.atr_period)
            if atr and atr > 0:
                if qty < 0:
                    stop_side = "BUY"
                    new_stop_price = close_price + (atr * self.atr_mult)
                else:
                    stop_side = "SELL"
                    new_stop_price = close_price - (atr * self.atr_mult)

                prev = self.trailing_stop.get(symbol)
                prev_stop_price = None
                if isinstance(prev, dict):
                    prev_stop_price = prev.get('stop_price')

                improved = False
                if prev_stop_price is None:
                    improved = True
                else:
                    if qty > 0:
                        improved = new_stop_price > float(prev_stop_price)
                    else:
                        improved = new_stop_price < float(prev_stop_price)

                if improved:
                    try:
                        if isinstance(prev, dict):
                            prev_order_id = prev.get('order_id')
                            if prev_order_id is not None:
                                self.binance.cancel_order(symbol, {'orderId': prev_order_id})
                            elif prev.get('order') and isinstance(prev.get('order'), dict) and prev['order'].get('orderId') is not None:
                                self.binance.cancel_order(symbol, prev['order'])
                    except Exception as e:
                        logger.warning(f"Lỗi hủy trailing stop cũ {symbol}: {e}")

                    try:
                        if symbol in self.sl_orders:
                            self.binance.cancel_order(symbol, self.sl_orders[symbol])
                            self.sl_orders.pop(symbol, None)
                    except Exception as e:
                        logger.warning(f"Lỗi hủy SL cũ trước khi đặt trailing {symbol}: {e}")

                    order = self.binance.create_order_stop_loss(
                        symbol=symbol,
                        side=stop_side,
                        price=new_stop_price,
                        quantity=abs(qty)
                    )
                    if order:
                        order_id = None
                        if isinstance(order, dict):
                            order_id = order.get('orderId')
                        self.trailing_stop[symbol] = {
                            'order': order,
                            'order_id': order_id,
                            'stop_price': new_stop_price,
                            'atr': atr,
                            'interval': self.timeframe_signal,
                        }

        if pnl > 0 and pnl >= self.tp:
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

        if pnl < 0 and pnl <= self.sl:
            # Cắt lỗ 1 phần
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

    def check_signal(self, symbol: str) -> Optional[Tuple[float, float, str]]:
        """Check điều kiện vào lệnh dựa trên 3 nến 1m và 3 nến 1h
        
        BUY: 3 nến 1m tăng > 1% VÀ 3 nến 1h giảm < -8%
        SELL: 3 nến 1m giảm < -1% VÀ 3 nến 1h tăng > 8%
        
        Returns:
            Tuple[price, change_pct, side] hoặc None
        """
        klines_signal = self.all_symbols_signal.get(symbol)
        if not klines_signal or len(klines_signal) < self.kline_signal:
            return None

        open_1m = klines_signal[0]['open']
        close_1m = klines_signal[-1]['close']
        oc_1m_pct = ((close_1m - open_1m) / open_1m) * 100

        oc_1h_pct = self._get_check_oc_pct(symbol)
        if oc_1h_pct is None:
            return None

        logger.info(f"{symbol} | 1m OC: {oc_1m_pct:.2f}% | {self.timeframe_check} OC: {oc_1h_pct:.2f}%")

        if oc_1m_pct > self.oc_signal and oc_1h_pct < 0 and oc_1h_pct < -self.oc_check_min:
            return close_1m, abs(oc_1m_pct), 'BUY'

        if oc_1m_pct < -self.oc_signal and oc_1h_pct > self.oc_check_min:
            return close_1m, abs(oc_1m_pct), 'SELL'

        return None

    def _handle_multi_kline_order_queue(self):
        return

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

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List

from binance import Client
from binance.enums import *

logger = logging.getLogger(__name__)


class BinanceCore:
    def __init__(self, api_key: str, secret_key: str, demo: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.client = Client(self.api_key, self.secret_key, demo=demo)
        
        # Cache exchange info để tránh gọi API nhiều lần
        self._exchange_info_cache: Optional[Dict] = None
        self._exchange_info_cache_time: float = 0
        self._cache_ttl: int = 3600  # Cache 15m

    def _get_exchange_info(self) -> Dict:
        """Lấy exchange info với cache"""
        current_time = time.time()
        if self._exchange_info_cache is None or (current_time - self._exchange_info_cache_time) > self._cache_ttl:
            self._exchange_info_cache = self.client.futures_exchange_info()
            self._exchange_info_cache_time = current_time
        return self._exchange_info_cache

    def get_all_positions(self) -> List[Dict]:
        """Lấy tất cả positions đang mở (có qty != 0)"""
        positions = self.client.futures_position_information()
        return [p for p in positions if float(p.get('positionAmt', 0)) != 0]

    def get_position_by_symbol(self, symbol):
        return self.client.futures_position_information(symbol=symbol)

    def get_limit_orders(self, symbol):
        orders = self.client.futures_get_open_orders(symbol=symbol)
        return [o for o in orders if o["type"] == "LIMIT" and (not o['reduceOnly'] or o['closePosition'])]

    def cancel_order(self, symbol, order):
        try:
            order_id = None
            if isinstance(order, dict):
                if order.get('orderId') is not None:
                    order_id = order.get('orderId')
                elif order.get('i') is not None:
                    order_id = order.get('i')
            else:
                order_id = order

            if order_id is None:
                return

            self.client.futures_cancel_order(
                symbol=symbol,
                orderId=order_id
            )
        except Exception as e:
            msg = str(e)
            if '-2011' in msg or 'Unknown order' in msg:
                return
            raise

    def clear_order(self, symbol):
        return self.client.futures_cancel_all_open_orders(symbol=symbol)

    def can_make_order(self, symbol: str) -> bool:
        """Kiểm tra có thể đặt lệnh mới không (không có position đang mở)"""
        self.clear_order(symbol)
        positions = self.get_position_by_symbol(symbol)
        if not positions:
            return True
        # Kiểm tra xem có position nào đang mở không
        for pos in positions:
            if float(pos.get('positionAmt', 0)) != 0:
                return False
        return True

    def get_klines(self, symbol, interval, limit=3):
        """Lấy dữ liệu nến đã đóng theo interval (1m, 1h, ...)
        
        Lấy limit+1 nến rồi bỏ nến cuối (đang chạy) để chỉ lấy nến đã đóng
        """
        try:
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=interval,
                limit=limit + 1
            )
            # Bỏ nến cuối cùng (đang chạy, chưa đóng)
            return klines[:-1] if len(klines) > limit else klines
        except Exception as e:
            logger.error(f"Lỗi lấy klines {symbol} {interval}: {e}")
            return []

    def get_top_volatile_liquid_symbols(self, limit: int = 100) -> List[str]:
        """Lấy danh sách symbol theo khối lượng giao dịch 24h"""
        exchange_info = self._get_exchange_info()
        min_age = datetime.utcnow() - timedelta(days=15)

        futures_symbols = set()
        for s in exchange_info["symbols"]:
            if (s["contractType"] == "PERPETUAL" 
                and s["status"] == "TRADING"
                and s["symbol"].endswith("USDT")):
                onboard_date = datetime.utcfromtimestamp(s.get("onboardDate", 0) / 1000)
                if onboard_date <= min_age:
                    futures_symbols.add(s["symbol"])

        tickers = self.client.futures_ticker()
        filtered = [
            (t["symbol"], float(t["quoteVolume"]))
            for t in tickers if t["symbol"] in futures_symbols
        ]
        filtered.sort(key=lambda x: x[1], reverse=True)

        return [f[0] for f in filtered[:limit]]

    def _format_quantity(self, symbol: str, quantity: float) -> float:
        """Format quantity theo step size của symbol"""
        try:
            exchange_info = self._get_exchange_info()
            if not exchange_info:
                return round(quantity, 3)

            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    filters = symbol_info.get('filters', [])
                    for filter_info in filters:
                        if filter_info['filterType'] == 'LOT_SIZE':
                            step_size = float(filter_info['stepSize'])
                            min_qty = float(filter_info.get('minQty', 0))
                            if quantity < min_qty:
                                quantity = min_qty
                            formatted_qty = round(quantity / step_size) * step_size
                            return round(formatted_qty, 8)

            return round(quantity, 3)

        except Exception as e:
            logger.error(f"Lỗi format quantity {symbol}: {e}")
            return round(quantity, 3)

    def _format_price(self, symbol: str, price: float) -> float:
        """Format price theo tick size của symbol"""
        try:
            exchange_info = self._get_exchange_info()
            if not exchange_info:
                return round(price, 2)

            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    filters = symbol_info.get('filters', [])
                    for filter_info in filters:
                        if filter_info['filterType'] == 'PRICE_FILTER':
                            tick_size = float(filter_info['tickSize'])
                            min_price = float(filter_info.get('minPrice', 0))
                            if price < min_price:
                                price = min_price
                            formatted_price = round(price / tick_size) * tick_size
                            return round(formatted_price, 8)

            return round(price, 2)

        except Exception as e:
            logger.error(f"Lỗi format price {symbol}: {e}")
            return round(price, 2)


    def create_order(self, symbol, side, entry_price, quantity, order_type=FUTURE_ORDER_TYPE_LIMIT):
        """Tạo lệnh entry (LIMIT hoặc MARKET)"""
        try:
            # Đặt leverage trước
            try:
                self.client.futures_change_leverage(symbol=symbol, leverage=20)
            except:
                return

            entry_price = self._format_price(symbol, entry_price)
            quantity = self._format_quantity(symbol, quantity)

            logger.info(f"🟢 Gửi lệnh {order_type} {side} {symbol} tại {entry_price}, số lượng: {quantity}")

            if order_type == FUTURE_ORDER_TYPE_MARKET:
                order = self.client.futures_create_order(
                    symbol=symbol,
                    side=side,
                    type=FUTURE_ORDER_TYPE_MARKET,
                    quantity=quantity,
                )
            else:
                order = self.client.futures_create_order(
                    symbol=symbol,
                    side=side,
                    type=FUTURE_ORDER_TYPE_LIMIT,
                    price=entry_price,
                    quantity=quantity,
                    timeInForce="GTC"
                )

            return order

        except Exception as e:
            logger.error(f"❌ Lỗi tạo lệnh entry {symbol}: {e}")
            return None

    def create_order_take_profit(self, symbol, side, price, quantity):
        try:
            # format
            price = self._format_price(symbol, price)
            quantity = self._format_quantity(symbol, quantity)
            stop_price = self._format_price(symbol, price)

            logger.info(f"TP price: {price}, stop_price: {stop_price}")
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                stopPrice=stop_price,
                reduceOnly=True,
                quantity=quantity,
                workingType="MARK_PRICE",
            )
            logger.info(f'Đặt TP Thành Công: {symbol}')
            return order
        except Exception as e:
            self.close_position(symbol)
            logger.error(f'Đặt TP Lỗi: {e}')
            return None

    def create_order_stop_loss(self, symbol, side, price, quantity):
        try:
            # format
            price = self._format_price(symbol, price)
            quantity = self._format_quantity(symbol, quantity)
            stop_price = self._format_price(symbol, price)

            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=FUTURE_ORDER_TYPE_STOP_MARKET,
                stopPrice=stop_price,
                quantity=quantity,
                reduceOnly=True,
                workingType="MARK_PRICE",
            )
            logger.info(f'Đặt SL Thành Công: {symbol}')
            return order
        except Exception as e:
            self.close_position(symbol)
            logger.error(f'Đặt SL Lỗi: {e}')
            return None

    def close_position(self, symbol):
        """Đóng toàn bộ vị thế của symbol"""
        try:
            # Hủy toàn bộ lệnh chờ (bao gồm trailing stop / TP / SL)
            try:
                self.client.futures_cancel_all_open_orders(symbol=symbol)
            except Exception as e:
                logger.warning(f"⚠️ Lỗi hủy lệnh chờ trước khi đóng vị thế {symbol}: {e}")

            position_info = self.client.futures_position_information(symbol=symbol)
            if not position_info:
                logger.warning(f"⚠️ Không tìm thấy thông tin vị thế cho {symbol}")
                return

            for pos in position_info:
                # position_amt = float(position_info[0]['positionAmt'])
                position_amt = float(pos.get('positionAmt', 0))

                # Nếu có vị thế thì đóng
                if position_amt != 0:
                    side = "SELL" if position_amt > 0 else "BUY"
                    quantity = abs(position_amt)

                    order = self.client.futures_create_order(
                        symbol=symbol,
                        side=side,
                        type="MARKET",
                        quantity=quantity,
                    )

                    logger.info(f"✅ Đã đóng vị thế {symbol} - Side: {side} - Quantity: {quantity}")

                    # Hủy lại lần nữa để đảm bảo không còn lệnh chờ sau khi đã close
                    try:
                        self.client.futures_cancel_all_open_orders(symbol=symbol)
                    except Exception as e:
                        logger.warning(f"⚠️ Lỗi hủy lệnh chờ sau khi đóng vị thế {symbol}: {e}")
                    return order
                else:
                    logger.warning(f"⚠️ Không có vị thế mở để đóng cho {symbol}")

        except Exception as e:
            logger.error(f"❌ Lỗi khi đóng vị thế {symbol}: {e}")

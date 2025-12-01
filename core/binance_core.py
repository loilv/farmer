import logging

from binance import Client
from binance.enums import *


class BinanceCore:
    def __init__(self, api_key, secret_key, demo=True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.client = Client(self.api_key, self.secret_key, demo=demo)

    def get_all_positions(self):
        return self.client.futures_position_information()

    def get_position_by_symbol(self, symbol):
        return self.client.futures_position_information(symbol=symbol)

    def get_limit_orders(self, symbol):
        orders = self.client.futures_get_open_orders(symbol=symbol)
        return [o for o in orders if o["type"] == "LIMIT" and (not o['reduceOnly'] or o['closePosition'])]

    def get_stop_loss_orders(self, symbol):
        orders = self.client.futures_get_open_orders(symbol=symbol)
        return [o for o in orders if o["type"] == "STOP_MARKET"]

    def cancel_order(self, symbol, order):
        self.client.futures_cancel_order(
            symbol=symbol,
            orderId=order["orderId"]
        )

    def clear_order(self, symbol):
        return self.client.futures_cancel_all_open_orders(symbol=symbol)

    def can_make_order(self, symbol):
        self.clear_order(symbol)
        position = self.get_position_by_symbol(symbol)
        if not position:
            return True
        return False

    def get_top_liquid_symbols(self):
        # Lấy toàn bộ dữ liệu 24h của USDT-M Futures
        tickers = self.client.futures_ticker()

        # Lọc symbol chỉ lấy USDT-M (kết thúc bằng USDT)
        usdt_tickers = [
            t for t in tickers
            if t["symbol"].endswith("USDT")
        ]

        # Sort theo thanh khoản (quoteVolume)
        sorted_tickers = sorted(
            usdt_tickers,
            key=lambda t: float(t["quoteVolume"]),
            reverse=True
        )

        # Lấy top 50 symbol
        top_50_symbols = [t["symbol"] for t in sorted_tickers[:100]]

        return top_50_symbols

    def get_top_volatile_liquid_symbols(self, limit=200, min_liquidity=30_000_000):
        # Lấy danh sách symbol Futures PERPETUAL đang hoạt động
        exchange_info = self.client.futures_exchange_info()
        futures_symbols = {
            s["symbol"]
            for s in exchange_info["symbols"]
            if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
        }

        tickers = self.client.futures_ticker()
        filtered = []

        for t in tickers:
            symbol = t["symbol"]

            # Chỉ lấy symbol đang hoạt động trên futures PERPETUAL
            if symbol not in futures_symbols:
                continue

            change_pct = float(t["priceChangePercent"])
            quote_vol = float(t["quoteVolume"])  # thanh khoản 24h USDT

            if quote_vol < min_liquidity:
                continue

            filtered.append((symbol, change_pct, quote_vol))

        # Sắp xếp theo biến động mạnh (abs)
        filtered.sort(key=lambda x: abs(x[1]), reverse=True)

        return [f[0] for f in filtered[:limit]]

    def _format_quantity(self, symbol: str, quantity: float) -> float:
        """Format quantity theo step size của symbol"""
        try:
            exchange_info = self.client.futures_exchange_info()
            if not exchange_info:
                return round(quantity, 3)

            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    filters = symbol_info.get('filters', [])
                    for filter_info in filters:
                        if filter_info['filterType'] == 'LOT_SIZE':
                            step_size = float(filter_info['stepSize'])
                            # Đảm bảo quantity không nhỏ hơn minQty
                            min_qty = float(filter_info.get('minQty', 0))
                            if quantity < min_qty:
                                quantity = min_qty
                            formatted_qty = round(quantity / step_size) * step_size
                            return round(formatted_qty, 8)

            return round(quantity, 3)

        except Exception as e:
            logging.error(f"Lỗi format quantity {symbol}: {e}")
            return round(quantity, 3)

    def _format_price(self, symbol: str, price: float) -> float:
        """Format price theo tick size của symbol"""
        try:
            exchange_info = self.client.futures_exchange_info()
            if not exchange_info:
                return round(price, 2)

            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    filters = symbol_info.get('filters', [])
                    for filter_info in filters:
                        if filter_info['filterType'] == 'PRICE_FILTER':
                            tick_size = float(filter_info['tickSize'])
                            # Đảm bảo price không nhỏ hơn minPrice
                            min_price = float(filter_info.get('minPrice', 0))
                            if price < min_price:
                                price = min_price
                            formatted_price = round(price / tick_size) * tick_size
                            return round(formatted_price, 8)

            return round(price, 2)

        except Exception as e:
            logging.error(f"Lỗi format price {symbol}: {e}")
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

            print(f"🟢 Gửi lệnh {order_type} {side} {symbol} tại {entry_price}, số lượng: {quantity}")

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
            print(f"❌ Lỗi tạo lệnh entry {symbol}: {e}")
            return None

    def create_order_take_profit(self, symbol, side, price, quantity):
        try:
            # format
            price = self._format_price(symbol, price)
            quantity = self._format_quantity(symbol, quantity)
            stop_price = self._format_price(symbol, price * 1.001)

            print(price, stop_price)
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                stopPrice=stop_price,
                reduceOnly=True,
                # price=price,
                quantity=quantity,
                workingType="MARK_PRICE",
            )
            print(f'Đặt TP Thành Công: {symbol} !!!!!!')
            return order
        except Exception as e:
            self.close_position(symbol)
            print(f'Đặt TP Lỗi: {e}')
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
                # price=price,
                quantity=quantity,
                reduceOnly=True,
                workingType="MARK_PRICE",
            )
            print(f'Đặt SL Thành Công: {symbol} !!!!!!')
            return order
        except Exception as e:
            self.close_position(symbol)
            print(f'Đặt SL Lỗi: {e}')
            return None

    def create_stop_loss_be(self, symbol, new_stop, max_price, low_price):
        positions = self.get_position_by_symbol(symbol)
        position = positions[-1] if positions else {}
        qty = float(position.get('positionAmt', 0))
        price = float(position.get('breakEvenPrice', 0))
        offset = (new_stop / 10) - 0.005
        try:
            if qty > 0:
                side = 'SELL'
                price = price * (1 + offset)
                price = max(price, low_price)
            else:  # Short
                side = 'BUY'
                price = price * (1 - offset)
                price = max(price, max_price)

            price = self._format_price(symbol, price)
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type=FUTURE_ORDER_TYPE_STOP_MARKET,
                stopPrice=price,
                closePosition=True,
                # quantity=quantity,
                workingType="MARK_PRICE",
            )
            print(f'Đặt SL BE Thành Công: {symbol} !!!!!!')
            return order
        except Exception as e:
            print(f'Đặt SL BE Lỗi: price: {price} : {e}')
            self.close_position(symbol)
            return None

            
    def close_position(self, symbol):
        """Đóng toàn bộ vị thế của symbol"""
        try:
            position_info = self.client.futures_position_information(symbol=symbol)
            if not position_info:
                print(f"⚠️ Không tìm thấy thông tin vị thế cho {symbol}")
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

                    print(f"✅ Đã đóng vị thế {symbol} - Side: {side} - Quantity: {quantity}")
                    self.client.futures_cancel_all_open_orders(symbol=symbol)
                    return order
                else:
                    print(f"⚠️ Không có vị thế mở để đóng cho {symbol}")

        except Exception as e:
            logging.error(f"❌ Lỗi khi đóng vị thế {symbol}: {e}")
            pass




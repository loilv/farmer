import time
from binance import ThreadedWebsocketManager, Client
import logging
from logging.handlers import RotatingFileHandler


class BinanceOrderWatcher:
    def __init__(self, config):
        """Khởi tạo client Binance và WebSocket manager"""
        self.client = Client(config.api_key, config.secret_key, testnet=config.testnet)
        self.twm = ThreadedWebsocketManager(api_key=config.api_key, api_secret=config.secret_key,
                                            testnet=config.testnet, max_queue_size=5000)
        self.active_orders = {}  # symbol -> order_id
        self.config = config
        self.trading_logger = None
        self.setup_trading_logger()
        self.leverage = config.leverage

    def get_new_futures_symbols(self, hours: int = 6, limit: int = 50):
        now = time.time()
        cutoff = now - hours * 3600

        info = self.client.futures_exchange_info()
        symbols_info = info.get("symbols", [])

        new_symbols = []
        other_symbols = []

        for s in symbols_info:
            if s.get("contractType") != "PERPETUAL" or not s["symbol"].endswith("USDT"):
                continue

            onboard = s.get("onboardDate") or s.get("listDate")

            if onboard and (onboard / 1000) >= cutoff:
                new_symbols.append(s["symbol"])
            else:
                other_symbols.append(s["symbol"])

        # Nếu ít hơn limit, thì thêm các symbol còn lại để đủ số lượng
        if len(new_symbols) < limit:
            missing = limit - len(new_symbols)
            new_symbols.extend(other_symbols[:missing])

        return new_symbols[:limit]

    def get_cheap_volatile_futures_symbols(self, price_threshold=5.0, min_volume=10000000, top_n=100):
        """
        Lấy danh sách symbol futures giá dưới 1 USDT có biến động tốt
        Return: List các symbol (string)
        """
        try:

            # Lấy thông tin futures
            exchange_info = self.client.futures_exchange_info()
            symbols = exchange_info['symbols']

            # Lấy TẤT CẢ ticker 24h một lần (tránh rate limit)
            all_tickers = self.client.futures_ticker()
            ticker_dict = {ticker['symbol']: ticker for ticker in all_tickers}

            coin_data = []

            for symbol in symbols:
                if symbol['quoteAsset'] == 'USDT' and symbol['status'] == 'TRADING':
                    symbol_name = symbol['symbol']

                    # Kiểm tra xem symbol có trong ticker không
                    if symbol_name in ticker_dict:
                        ticker = ticker_dict[symbol_name]

                        try:
                            current_price = float(ticker['lastPrice'])
                            volume_24h = float(ticker.get('volume', 0))
                            price_change_percent = abs(float(ticker.get('priceChangePercent', 0)))

                            # Kiểm tra điều kiện
                            if (current_price <= price_threshold and
                                    volume_24h >= min_volume):
                                coin_data.append({
                                    'symbol': symbol_name,
                                    'price': current_price,
                                    'volume_24h': volume_24h,
                                    'price_change_24h%': price_change_percent
                                })

                        except (ValueError, KeyError):
                            continue

            # Sắp xếp theo biến động và volume
            coin_data.sort(key=lambda x: (x['price_change_24h%'], x['volume_24h']), reverse=True)

            # Trả về list symbol
            symbol_list = [coin['symbol'] for coin in coin_data[:top_n]]

            print(f"✅ Tìm thấy {len(symbol_list)} symbols phù hợp")
            return symbol_list

        except Exception as e:
            print(f"❌ Lỗi: {e}")
            return []

    def get_top_strong_movers(self, top_n=100, pump_threshold=4.5, dump_threshold=-4.5, min_volume_usdt=50_000_000):
        """
        Lấy top coin Pump/Dump mạnh nhất trong 24h (Futures USDT-M PERPETUAL)

        Trả về 1 danh sách symbol duy nhất
        - Ưu tiên coin biến động lớn nhất
        """
        try:
            # Lấy symbol hợp lệ đang giao dịch
            exchange_info = self.client.futures_exchange_info()
            valid_symbols = {
                s["symbol"]
                for s in exchange_info["symbols"]
                if (
                        s.get("contractType") == "PERPETUAL"
                        and s.get("quoteAsset") == "USDT"
                        and s.get("status") == "TRADING"
                )
            }

            tickers = self.client.futures_ticker()
            movers = []

            for t in tickers:
                try:
                    symbol = t.get("symbol", "")
                    if symbol not in valid_symbols:
                        continue

                    quote_volume = float(t.get("quoteVolume", 0))
                    price_change = float(t.get("priceChangePercent", 0))

                    # Lọc theo volume tối thiểu
                    if quote_volume < min_volume_usdt:
                        continue

                    # Lọc Pump/Dump
                    if price_change >= pump_threshold or price_change <= dump_threshold:
                        movers.append({
                            "symbol": symbol,
                            "change": price_change
                        })

                except Exception:
                    continue

            # Sort theo độ biến động mạnh nhất (|%|)
            movers.sort(key=lambda x: abs(x["change"]), reverse=True)

            # Lấy top_n symbol
            top_symbols = [x["symbol"] for x in movers[:top_n]]
            return top_symbols

        except Exception as e:
            logging.error(f"Lỗi khi lấy top strong movers: {e}")
            return []

    def get_high_volume_symbols(self, top_n=100, min_volume_usdt=50_000_000):
        """
        Lấy danh sách symbol có volume giao dịch lớn nhất trong 24h
        Chỉ lấy Futures USDT-M PERPETUAL đang giao dịch
        """
        try:
            # Lấy thông tin exchange
            exchange_info = self.client.futures_exchange_info()

            # Lọc symbol futures USDT-M PERPETUAL đang giao dịch
            valid_symbols = set()
            for s in exchange_info['symbols']:
                if (s.get('contractType') == "PERPETUAL" and
                        s.get('quoteAsset') == "USDT" and
                        s.get('status') == "TRADING"):
                    valid_symbols.add(s['symbol'])

            # Lấy ticker 24h
            tickers = self.client.futures_ticker()
            volume_data = []

            for t in tickers:
                symbol = t.get('symbol', '')

                # Chỉ lấy symbol hợp lệ
                if symbol not in valid_symbols:
                    continue

                try:
                    quote_volume = float(t.get('quoteVolume', 0))

                    # Lọc theo volume tối thiểu
                    if quote_volume >= min_volume_usdt:
                        volume_data.append({
                            "symbol": symbol,
                            "quoteVolume": quote_volume
                        })

                except (ValueError, TypeError):
                    continue

            # Sắp xếp theo volume giảm dần và lấy top_n
            volume_data.sort(key=lambda x: x["quoteVolume"], reverse=True)
            top_symbols = [x["symbol"] for x in volume_data[:top_n]]

            logging.info(f"Lấy được {len(top_symbols)} symbol có volume >= {min_volume_usdt:,.0f} USDT")

            return top_symbols

        except Exception as e:
            logging.error(f"Lỗi khi lấy high volume symbols: {e}")
            return []

    def get_most_volatile_symbols(self, top_n=100, min_volume_usdt=1_000_000, min_days_listed=90):
        """
        Chỉ lấy các symbol đang có trên FUTURES USDT-M PERPETUAL,
        đã lên sàn ít nhất 3 tháng, volume đủ lớn, biến động mạnh.
        """
        try:
            # Lấy tất cả symbol futures
            exchange_info = self.client.futures_exchange_info()
            symbol_info_map = {}

            for s in exchange_info['symbols']:
                # Lọc đúng Futures USDT-M PERP
                if s.get('contractType') != "PERPETUAL":
                    continue
                if s.get('quoteAsset') != "USDT":
                    continue
                if s.get('status') != "TRADING":  # Chỉ lấy symbol đang giao dịch
                    continue

                symbol_info_map[s['symbol']] = s.get('onboardDate', 0)

            now = int(time.time() * 1000)
            min_list_time = now - min_days_listed * 24 * 60 * 60 * 1000

            tickers = self.client.futures_ticker()
            processed = []

            for t in tickers:
                symbol = t.get('symbol', '')

                # Chỉ lấy symbol hợp lệ từ futures PERP
                if symbol not in symbol_info_map:
                    continue

                # Lọc theo tuổi đời >= 3 tháng
                onboard_time = symbol_info_map.get(symbol, 0)
                if onboard_time == 0 or onboard_time > min_list_time:
                    continue

                try:
                    price_change_percent = float(t.get('priceChangePercent', 0))
                    volume = float(t.get('volume', 0))
                    last_price = float(t.get('lastPrice', 0))
                    quote_volume = float(t.get('quoteVolume', volume * last_price))

                    # Volume phải đủ lớn và giá hợp lệ
                    if quote_volume < min_volume_usdt or last_price <= 0:
                        continue

                    processed.append({
                        "symbol": symbol,
                        "priceChangePercent": price_change_percent,
                        "quoteVolume": quote_volume
                    })
                except (ValueError, TypeError):
                    continue

            if not processed:
                logging.warning("Không tìm thấy symbol nào đạt tiêu chí")
                return {"gainers": [], "losers": []}

            # Loại coin biến động quá yếu (giảm ngưỡng để có đủ symbol)
            processed = [x for x in processed if abs(x["priceChangePercent"]) > 0.3]

            if not processed:
                logging.warning("Không có symbol nào có biến động > 0.3%")
                return {"gainers": [], "losers": []}

            top_gainers = sorted(processed, key=lambda x: x["priceChangePercent"], reverse=True)[:top_n]
            top_losers = sorted(processed, key=lambda x: x["priceChangePercent"])[:top_n]

            return {
                "gainers": [x['symbol'] for x in top_gainers],
                "losers": [x['symbol'] for x in top_losers]
            }

        except Exception as e:
            logging.error(f"Lỗi khi lấy volatile symbols: {e}")
            return {"gainers": [], "losers": []}

    def setup_trading_logger(self):
        """Thiết lập logger cho trading"""
        self.trading_logger = logging.getLogger('trading')
        self.trading_logger.setLevel(logging.INFO)
        trading_handler = RotatingFileHandler(
            self.config.trading_log_file,
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        trading_handler.setFormatter(formatter)
        self.trading_logger.addHandler(trading_handler)
        self.trading_logger.propagate = False

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

    def close_order(self, symbol, quantity, reverse_side):
        """Đóng lệnh với side ngược lại"""
        try:
            quantity = self._format_quantity(symbol, quantity)
            order = self.client.futures_create_order(
                symbol=symbol,
                side=reverse_side,
                quantity=quantity,
                type="MARKET",
                reduceOnly=True  # Đảm bảo chỉ đóng vị thế
            )
            logging.info(f"✅ Đã đóng lệnh {symbol} - Side: {reverse_side} - Quantity: {quantity}")
            return order
        except Exception as e:
            logging.error(f"❌ Lỗi khi đóng lệnh {symbol}: {e}")
            pass

    def close_position_symbol_tp(self, symbol, position_amt, entry_price, tp_ratio=0.01):
        side = "SELL" if position_amt > 0 else "BUY"
        quantity = abs(position_amt)

        # Giá TP
        limit_price = entry_price * (1.001 if position_amt > 0 else 0.999)
        limit_price = self._format_price(symbol, limit_price)

        order = self.client.futures_create_order(
            symbol=symbol,
            side=side,
            type="TAKE_PROFIT",
            stopPrice=entry_price,
            price=limit_price,
            quantity=quantity,
            timeInForce="GTC",
            workingType="MARK_PRICE",
            reduceOnly=True
        )
        print(entry_price, limit_price)
        logging.info(f"✅ TP {symbol} - {side} - {quantity} tại {entry_price}")

    def close_position_symbol_sl(self, symbol, position_amt, entry_price, sl_ratio=0.005):
        side = "SELL" if position_amt > 0 else "BUY"
        quantity = abs(position_amt)

        # Giá SL
        limit_price = entry_price * (0.999 if position_amt > 0 else 1.001)
        limit_price = self._format_price(symbol, limit_price)

        order = self.client.futures_create_order(
            symbol=symbol,
            side=side,
            type="STOP",
            stopPrice=entry_price,
            price=limit_price,
            quantity=quantity,
            timeInForce="GTC",
            workingType="MARK_PRICE",
            reduceOnly=True
        )
        print(entry_price, limit_price)
        logging.info(f"✅ SL {symbol} - {side} - {quantity} tại {entry_price}")

    def close_position(self, symbol):
        """Đóng toàn bộ vị thế của symbol"""
        try:
            position_info = self.client.futures_position_information(symbol=symbol)
            if not position_info:
                logging.info(f"⚠️ Không tìm thấy thông tin vị thế cho {symbol}")
                return

            for pos in position_info:
                # position_amt = float(position_info[0]['positionAmt'])
                position_amt = float(pos.get('positionAmt', 0))

                # Nếu có vị thế thì đóng
                if position_amt != 0:
                    side = "SELL" if position_amt > 0 else "BUY"
                    positionSide = "LONG" if position_amt > 0 else "SHORT"
                    quantity = abs(position_amt)

                    order = self.client.futures_create_order(
                        symbol=symbol,
                        side=side,
                        type="MARKET",
                        quantity=quantity,
                        positionSide=positionSide
                    )

                    logging.info(f"✅ Đã đóng vị thế {symbol} - Side: {side} - Quantity: {quantity}")
                    self.client.futures_cancel_all_open_orders(symbol=symbol)
                    return order
                else:
                    logging.info(f"⚠️ Không có vị thế mở để đóng cho {symbol}")

        except Exception as e:
            logging.error(f"❌ Lỗi khi đóng vị thế {symbol}: {e}")
            pass

    def create_entry_order(self, symbol, side, entry_price, quantity, order_type="MARKET", pattern=None):
        """Tạo lệnh entry (LIMIT hoặc MARKET)"""
        try:
            # Đặt leverage trước
            try:
                self.client.futures_change_leverage(symbol=symbol, leverage=self.leverage)
            except:
                return

            entry_price = self._format_price(symbol, entry_price)
            quantity = self._format_quantity(symbol, quantity)

            logging.info(f"🟢 Gửi lệnh {order_type} {side} {symbol} tại {entry_price}, số lượng: {quantity}")

            if order_type == "MARKET":
                order = self.client.futures_create_order(
                    symbol=symbol,
                    side=side,
                    type="MARKET",
                    positionSide="LONG" if side == "BUY" else "SHORT",
                    quantity=quantity
                )
            else:
                order = self.client.futures_create_order(
                    symbol=symbol,
                    side=side,
                    type="LIMIT",
                    positionSide="LONG" if side == "BUY" else "SHORT",
                    price=entry_price,
                    quantity=quantity,
                    timeInForce="GTC"
                )

            self.trading_logger.info(
                f"MỞ LỆNH | {symbol} | Side: {side} | "
                f"Price: {entry_price} | Quantity: {quantity} | "
                f"Type: {order_type} | Time: {time.time()}"
            )

            # Lưu thông tin order đang active
            self.active_orders[symbol] = order['orderId']

            return order

        except Exception as e:
            logging.error(f"❌ Lỗi tạo lệnh entry {symbol}: {e}")
            pass

    def x_amount(self, symbol, side, qty):
        """Tăng gấp đôi vị thế (chỉ sử dụng khi cần)"""
        try:
            new_qty = qty * 2.5
            quantity = self._format_quantity(symbol, new_qty)

            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=quantity
            )

            logging.info(f"📈 Đã tăng vị thế {symbol} - Side: {side} - New Quantity: {quantity}")
            return order

        except Exception as e:
            logging.error(f"❌ Lỗi khi tăng vị thế {symbol}: {e}")
            pass

    def close_and_reverse(self, symbol, current_side, current_qty, reorder=False):
        """Đóng lệnh hiện tại và mở lệnh ngược chiều."""
        try:
            # 2️⃣ Đóng lệnh hiện tại
            close_order = self.client.futures_create_order(
                symbol=symbol,
                side=current_side,  # Sử dụng side ngược để đóng
                type="MARKET",
                quantity=abs(current_qty),
                reduceOnly=True
            )

            logging.info(f"🔄 Đã đóng vị thế {symbol} - Side: {current_side} - Quantity: {abs(current_qty)}")

            if reorder:
                # 3️⃣ Mở lệnh ngược lại với số lượng lớn hơn
                new_qty = abs(current_qty) * 2.0  # Giảm hệ số để an toàn
                new_quantity = self._format_quantity(symbol, new_qty)
                reverse_side = "SELL" if current_side == "BUY" else "BUY"  # BUY thành BUY mới, SELL thành SELL mới

                open_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=reverse_side,
                    type="MARKET",
                    quantity=new_quantity
                )

                logging.info(f"🔄 Đã mở vị thế ngược ({reverse_side}) {symbol} - Quantity: {new_quantity}")
                return open_order

            return close_order

        except Exception as e:
            logging.error(f"❌ Lỗi khi đảo chiều {symbol}: {e}")
            pass

    def stop(self):
        """Dừng WebSocket và cleanup"""
        try:
            self.twm.stop()
            logging.info("🛑 Đã dừng WebSocket Binance")
        except Exception as e:
            logging.error(f"Lỗi khi dừng WebSocket: {e}")
            pass
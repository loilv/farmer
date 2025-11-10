import json
import queue
import time
import logging
import requests
from .symbol_data import SymbolData
from trading.calculator import TradingCalculator
from core.order_manager import OrderBinanceManager
from core.binance_client import BinanceOrderWatcher


class PatternScanner:
    def __init__(self, config, virtual_trading):
        self.config = config
        self.virtual_trading = virtual_trading
        self.max_symbols = config.max_symbols
        self.symbols_data = {}
        self.message_queue = queue.Queue()
        self.signals_logger = logging.getLogger('signals')
        self.trading_calculator = TradingCalculator(config)
        self.order_manager = OrderBinanceManager(config)
        self.binance_client = BinanceOrderWatcher(config)

        # Tải symbols
        if config.scan_all_pairs:
            self.load_all_usdt_pairs()
        else:
            for symbol in config.symbols:
                self.symbols_data[symbol] = SymbolData(symbol, config.timeframe)

        logging.info(f"✅ Đã khởi tạo scanner với {len(self.symbols_data)} symbols")

    def load_all_usdt_pairs(self):
        """Tải tất cả các cặp USDT từ Binance"""
        try:
            response = requests.get('https://api.binance.com/api/v3/exchangeInfo', timeout=10)
            data = response.json()

            usdt_pairs = []
            for symbol_info in data['symbols']:
                if symbol_info['status'] == 'TRADING' and symbol_info['quoteAsset'] == 'USDT':
                    usdt_pairs.append(symbol_info['symbol'])

            max_pairs = self.max_symbols
            if len(usdt_pairs) > max_pairs:
                usdt_pairs = usdt_pairs[:max_pairs]

            for symbol in usdt_pairs:
                self.symbols_data[symbol] = SymbolData(symbol, self.config.timeframe)

            logging.info(f"📊 Đã tải {len(usdt_pairs)} cặp USDT")

        except Exception as e:
            logging.info(f"❌ Lỗi tải danh sách cặp: {e}")
            test_pairs = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'DOGEUSDT', 'SOLUSDT']
            for symbol in test_pairs:
                self.symbols_data[symbol] = SymbolData(symbol, self.config.timeframe)
            logging.info(f"📊 Sử dụng {len(test_pairs)} cặp test")

    def calculate_range_and_change(self, open_price, high, low, close):
        range_percent = ((high - low) / low) * 100
        change_percent = ((close - open_price) / open_price) * 100

        return {
            'range_percent': round(range_percent, 2),
            'change_percent': round(change_percent, 2)
        }

    def process_message(self, message):
        """Xử lý tin nhắn từ WebSocket"""
        try:
            data = json.loads(message)
            if 'stream' in data:
                kline_data = data['data']['k']
                symbol = kline_data['s']
            else:
                kline_data = data['k']
                symbol = kline_data['s']

            if symbol in self.symbols_data:
                open_price = float(kline_data['o'])
                close_price = float(kline_data['c'])
                high_price = float(kline_data['h'])
                low_price = float(kline_data['l'])
                start_time = int(kline_data['t'])
                end_time = int(kline_data['T'])

                on = high_price - max(open_price, close_price)
                both = min(open_price, close_price) - low_price
                current_time = int(time.time() * 1000)
                percentage = ((current_time - start_time) / (end_time - start_time)) * 100
                ratio_candlestick = self.calculate_candle_range_percent(high_price, low_price)

                if round(percentage) <= 30:
                    if on > 0 and close_price < open_price:
                        logging.info(f"Tín hiệu: {symbol} | SELL | {close_price} | ở {percentage:.2f}%")

                    elif (on == 0 or on <= 10) and both and round(ratio_candlestick, 1) > 2.5:
                        logging.info(f"Tín hiệu: {symbol} | BUY | {close_price} | ở {percentage:.2f}%")

                    elif (both == 0 or both <= 10) and on and round(ratio_candlestick, 1) > 2.5:
                        logging.info(f"Tín hiệu: {symbol} | SELL | {close_price} | ở {percentage:.2f}%")

                # result = self.handle_signal(open_price, close_price, high_price, low_price, close_price)

                # Xử lý nến đóng
                # if kline_data['x']:
                #     signal = self.detect_single_wick_signal(
                #         open_price=float(kline_data['o']),
                #         close_price=float(kline_data['c']),
                #         high_price=float(kline_data['h']),
                #         low_price=float(kline_data['l']),
                #     )
                #     if signal['signal'] != "NO_TRADE":
                #         logging.info(f"{symbol}: {signal['signal']}: price {kline_data['c']}")
                #         # Tính toán parameters với xử lý lỗi
                #         try:
                #             entry_price = self.trading_calculator.calculate_entry_price_signal(
                #                 float(kline_data['c']),
                #                 signal['signal'])
                #         except Exception as e:
                #             logging.info(f"❌ Lỗi tính toán parameters {symbol}: {e}")
                #             return
                #
                #         # Tính toán quantity cho Binance
                #         quantity = self.order_manager.calculate_position_size(symbol=symbol, current_price=entry_price)
                #
                #         # Tạo lệnh chính trên Binance
                #         self.binance_client.create_entry_order(
                #             symbol=symbol,
                #             side=signal['signal'],
                #             entry_price=entry_price,
                #             quantity=quantity,
                #         )

        except Exception as e:
            logging.info(f"Lỗi xử lý message: {e}")

    def analyze_candle_and_trade(self, open_price, close_price, high_price, low_price, current_price):
        """
        Phân tích nến hiện tại:
        - Xác định nến tăng hay giảm.
        - So sánh giá hiện tại với biên độ nến.
        - Nếu lệch 5% biên độ thì vào lệnh ngược.
        """
        # Tránh lỗi chia 0
        if high_price == low_price:
            return {"signal": "NO_TRADE", "reason": "Nến không có biên độ"}

        candle_range = high_price - low_price
        is_bullish = close_price > open_price
        is_bearish = close_price < open_price

        # Tính tỉ lệ vị trí giá hiện tại trong biên nến
        pos_ratio = (current_price - low_price) / candle_range  # 0 = đáy, 1 = đỉnh

        signal = "NO_TRADE"
        reason = ""

        if is_bullish:  # Nến tăng
            if pos_ratio >= 0.95:  # gần đỉnh
                signal = "SELL"
                reason = f"Giá hiện tại gần đỉnh (>{pos_ratio:.2%}) của nến tăng"
        elif is_bearish:  # Nến giảm
            if pos_ratio <= 0.05:  # gần đáy
                signal = "BUY"
                reason = f"Giá hiện tại gần đáy (<{pos_ratio:.2%}) của nến giảm"
        else:
            reason = "Nến doji (không rõ hướng)"

        return {
            "is_bullish": is_bullish,
            "is_bearish": is_bearish,
            "pos_ratio": round(pos_ratio, 4),
            "signal": signal,
            "reason": reason,
        }

    def detect_single_wick_signal(self, open_price, close_price, high_price, low_price, wick_ratio=1.5):
        """
        Phát hiện nến có đúng 1 râu dài >= 2.5 * thân nến
        - Nến giảm + râu dưới => BUY
        - Nến tăng + râu dưới => SELL
        - Nến giảm + râu trên => SELL
        - Nến tăng + râu trên => BUY
        """
        body = abs(close_price - open_price)
        upper_wick = high_price - max(open_price, close_price)
        lower_wick = min(open_price, close_price) - low_price

        # Tính tỷ lệ
        upper_ratio = upper_wick / body if body > 0 else 0
        lower_ratio = lower_wick / body if body > 0 else 0

        is_bullish = close_price > open_price
        is_bearish = close_price < open_price


        signal = "NO_TRADE"
        reason = ""
        body_percent = abs(close_price - open_price) / open_price * 100
        if round(body_percent, 2) < 0.2:
            return {
                "signal": signal,
            }

        # Chỉ xử lý khi chỉ có 1 râu
        if (upper_wick > 0 and lower_wick == 0) or (lower_wick > 0 and upper_wick == 0):
            # Râu trên dài
            if upper_ratio >= wick_ratio and lower_ratio < wick_ratio:
                if is_bearish:
                    signal = "SELL"
                    reason = f"Nến giảm có râu trên dài ({upper_ratio:.2f}x thân)"
                elif is_bullish:
                    signal = "BUY"
                    reason = f"Nến tăng có râu trên dài ({upper_ratio:.2f}x thân)"

            # Râu dưới dài
            elif lower_ratio >= wick_ratio and upper_ratio < wick_ratio:
                if is_bearish:
                    signal = "SELL"
                    reason = f"Nến giảm có râu dưới dài ({lower_ratio:.2f}x thân)"
                elif is_bullish:
                    signal = "BUY"
                    reason = f"Nến tăng có râu dưới dài ({lower_ratio:.2f}x thân)"
            else:
                reason = "Râu không đủ 2.5x thân nến"
        else:
            reason = "Có 2 râu hoặc không có râu"

        return {
            "open": open_price,
            "close": close_price,
            "high": high_price,
            "low": low_price,
            "body": round(body, 6),
            "upper_wick": round(upper_wick, 6),
            "lower_wick": round(lower_wick, 6),
            "upper_ratio": round(upper_ratio, 2),
            "lower_ratio": round(lower_ratio, 2),
            "signal": signal,
            "reason": reason,
        }

    def process_completed_candle(self, symbol, symbol_data, kline_data):
        """Xử lý khi nến hoàn thành"""
        try:
            # ĐẢM BẢO chuyển đổi kiểu dữ liệu cho tất cả giá trị
            open_price = float(kline_data['o'])
            close_price = float(kline_data['c'])
            high_price = float(kline_data['h'])
            low_price = float(kline_data['l'])
            volume = float(kline_data['v'])  # QUAN TRỌNG: chuyển volume sang float

            symbol_data.update_volume_history(volume)
            symbol_data.update_higher_timeframe_open_price(kline_data['t'], open_price)

            # Phân tích mô hình nến
            pattern = self.analyze_candlestick_patterns(symbol_data, open_price, close_price, high_price, low_price)

            # Lưu nến hiện tại
            symbol_data.prev_candle = (open_price, close_price)

            if pattern and self.config.virtual_trading:
                logging.info(f"🔍 Phát hiện tín hiệu {pattern} trên {symbol}")

                is_valid, validation_message = self.trading_calculator.validate_signal(
                    volume, symbol_data.average_volume, pattern
                )

                if not is_valid:
                    logging.info(f"❌ Tín hiệu {symbol} không hợp lệ: {validation_message}")
                    return

                # Xác định loại lệnh
                if pattern in self.config.enabled_bullish_patterns:
                    order_type = "BUY"
                elif pattern in self.config.enabled_bearish_patterns:
                    order_type = "SELL"
                else:
                    return

                # Tính toán parameters với xử lý lỗi
                try:
                    entry_price = self.trading_calculator.calculate_entry_price(close_price, pattern)
                except Exception as e:
                    logging.info(f"❌ Lỗi tính toán parameters {symbol}: {e}")
                    return

                # Tính toán quantity cho Binance
                quantity = self.order_manager.calculate_position_size(symbol=symbol, current_price=entry_price)

                # Tạo lệnh chính trên Binance
                self.binance_client.create_entry_order(
                    symbol=symbol,
                    side=order_type,
                    entry_price=entry_price,
                    quantity=quantity,
                    pattern=pattern
                )

        except Exception as e:
            logging.info(f"❌ Lỗi xử lý nến {symbol}: {e}")

    def analyze_candlestick_patterns(self, symbol_data, open_price, close_price, high_price, low_price):
        """Phân tích các mô hình nến"""
        try:
            body_size = abs(close_price - open_price)
            upper_shadow = high_price - max(open_price, close_price)
            lower_shadow = min(open_price, close_price) - low_price
            total_range = high_price - low_price

            if total_range == 0:
                return None

            upper_shadow_ratio = upper_shadow / total_range
            body_ratio = body_size / total_range
            lower_shadow_ratio = lower_shadow / total_range

            is_bearish = close_price > open_price
            is_bullish = close_price < open_price
            # enabled_bullish_patterns
            # enabled_bearish_patterns

            # SHOOTING STAR
            if ("SHOOTING_STAR" in self.config.enabled_bearish_patterns and
                    is_bearish and
                    upper_shadow >= body_size * 1.5 and
                    upper_shadow_ratio >= 0.4 and
                    lower_shadow_ratio <= 0.2):
                return "SHOOTING_STAR"



            # BEARISH ENGULFING
            elif ("BEARISH_ENGULFING" in self.config.enabled_bearish_patterns and
                  symbol_data.prev_candle is not None):
                prev_open, prev_close = symbol_data.prev_candle
                if (is_bearish and
                        prev_close > prev_open and
                        open_price > prev_close and
                        close_price < prev_open):
                    return "BEARISH_ENGULFING"

            # HAMMER
            elif ("HAMMER" in self.config.enabled_bullish_patterns and
                  is_bullish and
                  lower_shadow >= body_size * 1.5 and
                  lower_shadow_ratio >= 0.4 and
                  upper_shadow_ratio <= 0.2):
                return "HAMMER"

            # INVERTED HAMMER
            elif ("INVERTED_HAMMER" in self.config.enabled_bullish_patterns and
                  is_bullish and
                  upper_shadow >= body_size * 1.5 and
                  upper_shadow_ratio >= 0.4 and
                  lower_shadow_ratio <= 0.2):
                return "INVERTED_HAMMER"

            # BULLISH ENGULFING
            elif ("BULLISH_ENGULFING" in self.config.enabled_bullish_patterns and
                  symbol_data.prev_candle is not None):
                prev_open, prev_close = symbol_data.prev_candle
                if (is_bullish and
                        prev_close < prev_open and
                        open_price < prev_close and
                        close_price > prev_open):
                    return "BULLISH_ENGULFING"

            # HANGING MAN
            elif ("HANGING_MAN" in self.config.enabled_bullish_patterns and
                  is_bearish and
                  lower_shadow >= body_size * 1.5 and
                  lower_shadow_ratio >= 0.4 and
                  upper_shadow_ratio <= 0.2):
                return "HANGING_MAN"

            return None
        except Exception as e:
            logging.info(f"Lỗi phân tích mô hình nến: {e}")
            return None
import logging
from datetime import datetime
from collections import deque
import pandas as pd
from .rsi import RSI


class CandleAnalyzer:
    def __init__(self):
        # Dictionary để lưu trữ dữ liệu cho từng symbol
        self.symbol_data = {}
        self.rsi_length = 6  # Tùy ý, set length RSI nếu cần điều chỉnh

    def update_candle(self, symbol, candle_data):
        """Cập nhật dữ liệu nến mới cho symbol cụ thể (và history đóng close cho RSI)"""
        if symbol not in self.symbol_data:
            self.symbol_data[symbol] = {
                'candles': deque(maxlen=3),
                'count': 0,
                'close_history': deque(maxlen=50),
            }
        self.symbol_data[symbol]['candles'].append(candle_data)
        self.symbol_data[symbol]['count'] += 1
        self.symbol_data[symbol]['close_history'].append(candle_data['close'])
        if len(self.symbol_data[symbol]['candles']) >= 3:
            return self.analyze_candles(symbol)
        return None

    def set_close_history(self, symbol, close_list):
        """Set full close history cho symbol, chỉ dùng khi khởi tạo startup"""
        if symbol not in self.symbol_data:
            self.symbol_data[symbol] = {
                'candles': deque(maxlen=3),
                'count': 0,
                'close_history': deque(maxlen=50),
            }
        self.symbol_data[symbol]['close_history'] = deque(close_list, maxlen=50)

    def get_symbol_rsi(self, symbol):
        """Trả về giá trị RSI hiện tại cho symbol (RSI cuối chuỗi)"""
        if symbol not in self.symbol_data or len(self.symbol_data[symbol]['close_history']) < self.rsi_length+1:
            return None
        closes = list(self.symbol_data[symbol]['close_history'])
        rsi_series = RSI(length=self.rsi_length).calculate_series(pd.Series(closes))
        # Lấy RSI mới nhất không phải NaN
        rsi_value = rsi_series.dropna()
        if len(rsi_value) > 0:
            return rsi_value.iloc[-1]
        return None

    def analyze_candles(self, symbol):
        """Phân tích logic nến trên 3 cây gần nhất cho symbol cụ thể"""
        if symbol not in self.symbol_data or len(self.symbol_data[symbol]['candles']) < 3:
            return None

        candles = self.symbol_data[symbol]['candles']
        n1, n2, n3 = list(candles)  # n1: cũ nhất, n3: mới nhất

        # Kiểm tra 2 điều kiện chính
        buy = self.signal_buy(symbol, n1, n2, n3)  # Logic 1
        sell = self.signal_sell(symbol, n1, n2, n3)  # Logic 2

        return {
            'symbol': symbol,
            'signal_buy': buy,
            'signal_sell': sell,
            'candles': [n1, n2, n3],
            'timestamp': datetime.now()
        }

    def subtract_nonzero_decimals(self, a: float, b: float) -> int:
        def extract_nonzero_decimal(x: float) -> int:
            decimal = str(x).split('.')[1]  # phần thập phân
            decimal = decimal.rstrip('0')  # bỏ 0 ở cuối nếu có
            filtered = ''.join([c for c in decimal if c != '0'])
            return int(filtered) if filtered else 0

        num_a = extract_nonzero_decimal(a)
        num_b = extract_nonzero_decimal(b)
        return abs(num_b - num_a)

    def signal_buy(self, symbol, n1, n2, n3, rsi=None, tail_ratio=1.5, rsi_th=20, min_price_change=0.002):
        print(f'Check buy {symbol}')

        if not self.is_red_candle(n1):
            return False

        if not self.is_red_candle(n2):
            return False

        if not self.is_red_candle(n3):
            return False

        # RSI - điều chỉnh ngưỡng cho khung ngắn
        rsi = self.get_symbol_rsi(symbol) if not rsi else rsi
        if rsi is None or rsi >= rsi_th:
            return False

        print(f"✅ BUY {symbol} | RSI={rsi:.1f}")
        return True

    def signal_sell(self, symbol, n1, n2, n3, rsi=None, tail_ratio=2.0, rsi_th=55, min_price_change=0.002):
        print(f'Check sell {symbol}')

        if not self.is_green_candle(n1):
            return False

        if not self.is_green_candle(n2):
            return False

        if not self.is_red_candle(n3):
            return False

        # RSI
        rsi = self.get_symbol_rsi(symbol) if not rsi else rsi
        if rsi is None or rsi <= rsi_th:
            return False

        print(
            f"✅ SELL {symbol} | RSI={rsi:.1f}")
        return True

    def has_long_upper_shadow(self, candle):
        """Kiểm tra nến có râu trên dài"""
        if self.is_green_candle(candle):
            # Với nến xanh: râu trên = high - close
            upper_shadow = candle['high'] - candle['close']
            body = candle['close'] - candle['open']
        else:
            # Với nến đỏ: râu trên = high - open
            upper_shadow = candle['high'] - candle['open']
            body = candle['open'] - candle['close']

        # Râu trên được coi là dài khi > 60% thân nến
        if body > 0:  # Tránh chia cho 0
            return upper_shadow > (body * 0.6)
        return upper_shadow > 0

    def is_red_candle(self, candle):
        """Kiểm tra nến đỏ (giá đóng < giá mở)"""
        return candle['close'] < candle['open']

    def is_green_candle(self, candle):
        """Kiểm tra nến xanh (giá đóng > giá mở)"""
        return candle['close'] > candle['open']

    def get_symbol_info(self, symbol):
        """Lấy thông tin về symbol cụ thể"""
        if symbol in self.symbol_data:
            return {
                'candle_count': len(self.symbol_data[symbol]['candles']),
                'total_count': self.symbol_data[symbol]['count']
            }
        return {'candle_count': 0, 'total_count': 0}

    def get_all_symbols(self):
        """Lấy danh sách tất cả symbols đang được theo dõi"""
        return list(self.symbol_data.keys())

    def print_pattern_details(self, result):
        """In chi tiết về các điều kiện pattern"""
        symbol = result.get('symbol', False)
        if not symbol:
            return
        if result['signal_buy']:
            print(f"\n🎯 PHÁT HIỆN TÍN HIỆU BUY - {symbol.upper()} - {datetime.now().strftime('%H:%M:%S')} 🎯")
        elif result['signal_sell']:
            print(
                f"\n🎯 PHÁT HIỆN TÍN HIỆU SELL - {symbol.upper()} - {datetime.now().strftime('%H:%M:%S')} 🎯")

    def get_candle_info(self, candle):
        """Trả về thông tin chi tiết của nến"""
        color = "🟢 XANH" if self.is_green_candle(candle) else "🔴 ĐỎ"
        upper_shadow = candle['high'] - max(candle['open'], candle['close'])
        body = abs(candle['close'] - candle['open'])
        upper_shadow_ratio = (upper_shadow / body) if body > 0 else 0

        shadow = "✅ RÂU TRÊN DÀI" if self.has_long_upper_shadow(candle) else "❌ RÂU TRÊN NGẮN"

        return f"{color} | O:{candle['open']:.4f} H:{candle['high']:.4f} L:{candle['low']:.4f} C:{candle['close']:.4f} | {shadow} ({upper_shadow_ratio:.1%})"

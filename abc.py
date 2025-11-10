import websocket
import json
import threading
import time
from datetime import datetime
from collections import deque

class CandleAnalyzer:
    def __init__(self):
        self.candles = deque(maxlen=3)  # Lưu trữ 3 cây nến gần nhất
        
    def update_candle(self, candle_data):
        """Cập nhật dữ liệu nến mới"""
        self.candles.append(candle_data)
        
        if len(self.candles) == 3:
            self.analyze_candles()
    
    def analyze_candles(self):
        """Phân tích logic nến trên 3 cây gần nhất"""
        if len(self.candles) < 3:
            return
            
        n1, n2, n3 = list(self.candles)  # n1: cũ nhất, n3: mới nhất
        
        # Kiểm tra 2 điều kiện chính
        condition1 = self.pattern_1(n1, n2, n3)  # Logic 1
        condition2 = self.pattern_2(n1, n2, n3)  # Logic 2
        
        # In kết quả phân tích
        if condition1 or condition2:
            print(f"\n🎯 PHÁT HIỆN TÍN HIỆU - {datetime.now().strftime('%H:%M:%S')} 🎯")
            if condition1:
                print("🔴 Tín hiệu 1: Nến1 ĐỎ → Nến2 XANH râu trên dài → Nến3 đóng cửa thấp hơn sàn Nến2")
            if condition2:
                print("🟢 Tín hiệu 2: Nến1 XANH → Nến2 XANH râu trên dài → Nến3 ĐỎ đóng cửa thấp hơn mở cửa Nến2")
            self.print_candle_sequence(n1, n2, n3)
            self.print_pattern_details(condition1, condition2, n1, n2, n3)
    
    def pattern_1(self, n1, n2, n3):
        """
        Logic 1: 
        - Nến 1: ĐỎ
        - Nến 2: XANH có râu trên dài
        - Nến 3: Giá đóng cửa thấp hơn giá sàn của nến 2
        """
        # Kiểm tra nến 1 là đỏ
        if not self.is_red_candle(n1):
            return False
            
        # Kiểm tra nến 2 là xanh và có râu trên dài
        if not self.is_green_candle(n2):
            return False
            
        if not self.has_long_upper_shadow(n2):
            return False
            
        # Kiểm tra nến 3: giá đóng cửa thấp hơn giá sàn nến 2
        if n3['close'] >= n2['low']:
            return False
            
        return True
    
    def pattern_2(self, n1, n2, n3):
        """
        Logic 2:
        - Nến 1: XANH
        - Nến 2: XANH có râu trên dài
        - Nến 3: ĐỎ và giá đóng cửa thấp hơn giá mở cửa của nến 2
        """
        # Kiểm tra nến 1 là xanh
        if not self.is_green_candle(n1):
            return False
            
        # Kiểm tra nến 2 là xanh và có râu trên dài
        if not self.is_green_candle(n2):
            return False
            
        if not self.has_long_upper_shadow(n2):
            return False
            
        # Kiểm tra nến 3 là đỏ và giá đóng cửa thấp hơn giá mở cửa nến 2
        if not self.is_red_candle(n3):
            return False
            
        if n3['close'] >= n2['open']:
            return False
            
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
    
    def print_candle_sequence(self, n1, n2, n3):
        """In thông tin chi tiết về chuỗi 3 nến"""
        print("\n📊 CHUỖI 3 NẾN GẦN NHẤT:")
        print(f"Nến 1: {self.get_candle_info(n1)}")
        print(f"Nến 2: {self.get_candle_info(n2)}")
        print(f"Nến 3: {self.get_candle_info(n3)}")
        print("=" * 70)
    
    def get_candle_info(self, candle):
        """Trả về thông tin chi tiết của nến"""
        color = "🟢 XANH" if self.is_green_candle(candle) else "🔴 ĐỎ"
        upper_shadow = candle['high'] - max(candle['open'], candle['close'])
        body = abs(candle['close'] - candle['open'])
        upper_shadow_ratio = (upper_shadow / body) if body > 0 else 0
        
        shadow = "✅ RÂU TRÊN DÀI" if self.has_long_upper_shadow(candle) else "❌ RÂU TRÊN NGẮN"
        
        return f"{color} | O:{candle['open']:.4f} H:{candle['high']:.4f} L:{candle['low']:.4f} C:{candle['close']:.4f} | {shadow} ({upper_shadow_ratio:.1%})"
    
    def print_pattern_details(self, condition1, condition2, n1, n2, n3):
        """In chi tiết về các điều kiện pattern"""
        print("\n📈 CHI TIẾT PHÂN TÍCH:")
        
        if condition1:
            print("🔴 PATTERN 1:")
            print(f"   ✓ Nến1: ĐỎ (C:{n1['close']:.4f} < O:{n1['open']:.4f})")
            print(f"   ✓ Nến2: XANH + Râu trên dài (C:{n2['close']:.4f} > O:{n2['open']:.4f})")
            print(f"   ✓ Nến3: Đóng cửa {n3['close']:.4f} < Sàn nến2 {n2['low']:.4f}")
            print(f"   📉 Chênh lệch: {n2['low'] - n3['close']:.4f}")
        
        if condition2:
            print("🟢 PATTERN 2:")
            print(f"   ✓ Nến1: XANH (C:{n1['close']:.4f} > O:{n1['open']:.4f})")
            print(f"   ✓ Nến2: XANH + Râu trên dài (C:{n2['close']:.4f} > O:{n2['open']:.4f})")
            print(f"   ✓ Nến3: ĐỎ + Đóng cửa {n3['close']:.4f} < Mở cửa nến2 {n2['open']:.4f}")
            print(f"   📉 Chênh lệch: {n2['open'] - n3['close']:.4f}")

class BinanceWebSocket:
    def __init__(self, symbol="btcusdt"):
        self.symbol = symbol
        self.analyzer = CandleAnalyzer()
        self.candle_count = 0
        
    def on_message(self, ws, message):
        """Xử lý tin nhắn từ websocket"""
        data = json.loads(message)
        
        if 'k' in data:
            kline = data['k']
            if kline['x']:  # Nếu nến đã đóng
                self.process_completed_candle(kline)
    
    def process_completed_candle(self, kline):
        """Xử lý nến đã hoàn thành"""
        candle_data = {
            'open': float(kline['o']),
            'high': float(kline['h']),
            'low': float(kline['l']),
            'close': float(kline['c']),
            'volume': float(kline['v']),
            'start_time': kline['t']
        }
        
        self.candle_count += 1
        current_time = datetime.fromtimestamp(kline['t']/1000)
        
        print(f"\n{'='*50}")
        print(f"Nến #{self.candle_count} - Time: {current_time.strftime('%H:%M:%S')}")
        print(f"{self.get_candle_emoji(candle_data)} O:{candle_data['open']:.4f} H:{candle_data['high']:.4f} L:{candle_data['low']:.4f} C:{candle_data['close']:.4f}")
        
        # Gửi dữ liệu nến cho analyzer
        self.analyzer.update_candle(candle_data)
        
        # Hiển thị số lượng nến đang theo dõi
        print(f"📈 Đang theo dõi: {len(self.analyzer.candles)}/3 nến")
    
    def get_candle_emoji(self, candle):
        """Trả về emoji cho nến"""
        if candle['close'] > candle['open']:
            return "🟢 XANH"
        else:
            return "🔴 ĐỎ"
    
    def on_error(self, ws, error):
        print(f"Lỗi: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        print("Kết nối websocket đã đóng")
    
    def on_open(self, ws):
        print(f"✅ Kết nối websocket đã mở cho {self.symbol.upper()}")
        print("⏳ Đang chờ dữ liệu nến 1m...")
    
    def start(self):
        """Bắt đầu kết nối websocket"""
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_1m"
        
        ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        
        # Chạy websocket trong thread riêng
        def run_ws():
            ws.run_forever()
        
        thread = threading.Thread(target=run_ws)
        thread.daemon = True
        thread.start()

# Sử dụng chương trình
if __name__ == "__main__":
    print("🚀 Bắt đầu theo dõi nến 1m qua WebSocket (3 cây gần nhất)...")
    print("\n📋 2 TÍN HIỆU CHÍNH ĐƯỢC THEO DÕI:")
    print("1. 🔴 PATTERN 1: Nến1(ĐỎ) → Nến2(XANH+râu trên dài) → Nến3(đóng cửa < sàn nến2)")
    print("2. 🟢 PATTERN 2: Nến1(XANH) → Nến2(XANH+râu trên dài) → Nến3(ĐỎ+đóng cửa < mở cửa nến2)")
    print("\n💡 Lưu ý: Cần ít nhất 3 cây nến để bắt đầu phân tích")
    print("-" * 70)
    
    # Có thể thay đổi symbol ở đây
    symbols = ["btcusdt", "ethusdt", "adausdt", "dogeusdt"]
    selected_symbol = "btcusdt"  # Thay đổi symbol tại đây
    
    ws_client = BinanceWebSocket(symbol=selected_symbol)
    ws_client.start()
    
    # Giữ chương trình chạy
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Dừng chương trình...")
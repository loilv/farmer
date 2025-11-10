import logging


class SymbolData:
    def __init__(self, symbol, timeframe):
        self.symbol = symbol
        self.timeframe = timeframe
        self.higher_tf_open_price = None
        self.last_update_time = None
        self.last_signal_time = None
        self.volume_history = []
        self.average_volume = 0
        self.current_price = None
        self.prev_candle = None

    def update_volume_history(self, volume):
        """Cập nhật lịch sử volume với xử lý lỗi"""
        try:
            # Đảm bảo volume là số
            volume_float = float(volume)
            self.volume_history.append(volume_float)

            # Giữ chỉ 20 bản ghi gần nhất
            if len(self.volume_history) > 20:
                self.volume_history.pop(0)

            # Tính volume trung bình
            if self.volume_history:
                self.average_volume = sum(self.volume_history) / len(self.volume_history)
            else:
                self.average_volume = 0  # Đảm bảo luôn có giá trị số

        except (ValueError, TypeError) as e:
            logging.info(f"Lỗi xử lý volume {self.symbol}: {volume} - {e}")
            # Đặt average_volume về 0 để tránh lỗi
            self.average_volume = 0

    def update_higher_timeframe_open_price(self, current_time, open_price):
        pass
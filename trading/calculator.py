import logging


class TradingCalculator:
    def __init__(self, config):
        self.config = config
        self.bearish_patterns = config.enabled_bearish_patterns
        self.bullish_patterns = config.enabled_bullish_patterns

    def calculate_entry_price(self, current_price, pattern, order_type=None):
        """Tính giá entry hợp lý theo hướng mô hình"""
        offset = self.config.entry_price_offset / 100
        if pattern in self.bullish_patterns:
            # Kỳ vọng tăng → chờ giá hồi thấp hơn một chút để buy
            return current_price * (1 - offset)
        elif pattern in self.bearish_patterns:
            # Kỳ vọng giảm → chờ giá hồi cao hơn một chút để sell
            return current_price * (1 + offset)
        else:
            return current_price

    def calculate_entry_price_signal(self, current_price, signal):
        """Tính giá entry hợp lý theo hướng mô hình"""
        offset = self.config.entry_price_offset / 100
        if signal == 'BUY':
            # Kỳ vọng tăng → chờ giá hồi thấp hơn một chút để buy
            return current_price * (1 + offset)
        elif signal == 'SELL':
            # Kỳ vọng giảm → chờ giá hồi cao hơn một chút để sell
            return current_price * (1 - offset)
        else:
            return current_price

    def calculate_stop_loss(self, entry_price, pattern, leverage):
        """Tính SL dựa trên % ROI và đòn bẩy"""
        roi_stop = self.config.stop_loss_percentage / 100
        delta_percent = roi_stop / leverage  # đổi ROI thành % thay đổi giá

        if pattern in self.bearish_patterns:  # Lệnh SELL (Short) -> SL khi giá tăng
            return entry_price * (1 - delta_percent)
        elif pattern in self.bullish_patterns:  # Lệnh BUY (Long) -> SL khi giá giảm
            return entry_price * (1 + delta_percent)

    def calculate_take_profit(self, entry_price, pattern, leverage):
        """Tính TP dựa trên % ROI và đòn bẩy"""
        roi_tp = self.config.take_profit_percentage / 100
        delta_percent = roi_tp / leverage  # đổi ROI thành % thay đổi giá

        if pattern in self.bearish_patterns:  # Lệnh SELL (Short) -> TP khi giá giảm
            return entry_price * (1 + delta_percent)
        elif pattern in self.bullish_patterns:  # Lệnh BUY (Long) -> TP khi giá tăng
            return entry_price * (1 - delta_percent)

    def calculate_position_size(self, entry_price):
        """Tính khối lượng position dựa trên số tiền cố định"""
        try:
            entry_price_float = float(entry_price)
            if entry_price_float <= 0:
                logging.info(f"Giá entry không hợp lệ: {entry_price}")
                return 0

            position_size = self.config.position_size_usdt / entry_price_float
            return round(position_size, 6)  # Làm tròn 6 số thập phân

        except (ValueError, TypeError, ZeroDivisionError) as e:
            logging.info(f"Lỗi tính position size: entry_price={entry_price} - {e}")
            return 0

    def calculate_position_value(self, entry_price, position_size):
        """Tính giá trị position với đòn bẩy"""
        try:
            return float(entry_price) * float(position_size) * self.config.leverage
        except (ValueError, TypeError) as e:
            logging.info(f"Lỗi tính position value: {e}")
            return 0

    def validate_signal(self, volume, average_volume, pattern):
        """Xác thực tín hiệu với xử lý lỗi"""
        try:
            # Đảm bảo volume là số
            volume_float = float(volume)

            # Kiểm tra volume tối thiểu
            if volume_float < self.config.min_volume:
                return False, f"Volume quá thấp ({volume_float:.0f} < {self.config.min_volume})"

            # Kiểm tra volume spike chỉ khi có average_volume hợp lệ
            if average_volume and float(average_volume) > 0:
                try:
                    average_volume_float = float(average_volume)
                    volume_ratio = volume_float / average_volume_float
                    if volume_ratio < self.config.volume_spike_threshold:
                        return False, f"Volume không đủ mạnh (tỷ lệ: {volume_ratio:.2f} < {self.config.volume_spike_threshold})"
                except (TypeError, ValueError) as e:
                    # Nếu không tính được ratio, vẫn chấp nhận tín hiệu
                    pass

            return True, "Tín hiệu hợp lệ"

        except (ValueError, TypeError) as e:
            logging.info(f"Lỗi validate signal: volume={volume}, avg_volume={average_volume} - {e}")
            return False, f"Lỗi xác thực tín hiệu: {e}"
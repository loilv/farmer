import time


class Order:
    def __init__(self, order_id, symbol, order_type, entry_price, stop_loss, take_profit,
                 position_size, timestamp, pattern, leverage, position_amount_usdt):
        self.order_id = order_id
        self.symbol = symbol
        self.order_type = order_type  # "SELL" or "BUY"
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.position_size = position_size
        self.leverage = leverage
        self.position_amount_usdt = position_amount_usdt
        self.pattern = pattern
        self.status = "OPEN"
        self.result = None
        self.close_price = None
        self.close_time = None
        self.open_time = timestamp
        self.pnl_percentage = 0
        self.pnl_usdt = 0
        self.position_value = entry_price * position_size * leverage

    def close_order(self, close_price, result, close_time):
        """Đóng lệnh và tính PnL với đòn bẩy"""
        self.close_price = close_price
        self.result = result
        self.close_time = close_time
        self.status = "CLOSED"

        # Tính PnL phần trăm
        if self.order_type == "SELL":
            price_diff_pct = ((self.entry_price - close_price) / self.entry_price) * 100
        else:  # BUY
            price_diff_pct = ((close_price - self.entry_price) / self.entry_price) * 100

        # Áp dụng đòn bẩy
        self.pnl_percentage = price_diff_pct * self.leverage

        # Tính PnL USDT
        self.pnl_usdt = self.position_amount_usdt * (self.pnl_percentage / 100)

        return self.pnl_usdt
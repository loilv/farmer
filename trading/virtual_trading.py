import uuid
import time
import logging
from logging.handlers import RotatingFileHandler
from .order import Order


class VirtualTrading:
    def __init__(self, config):
        self.config = config
        self.open_orders = {}
        self.closed_orders = []
        self.account_balance = config.account_balance
        self.initial_balance = config.account_balance
        self.trading_logger = None
        self.setup_trading_logger()

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

    def can_open_order(self, symbol):
        """Kiểm tra có thể mở lệnh mới không"""
        if len(self.open_orders) >= self.config.max_open_orders:
            return False, "Đạt số lệnh mở tối đa"
        if symbol in self.open_orders:
            return False, f"Đã có lệnh mở cho {symbol}"
        return True, "Có thể mở lệnh"

    def open_order(self, symbol, order_type, entry_price, stop_loss, take_profit,
                   position_size, pattern, leverage, position_amount_usdt):
        """Mở lệnh giả lập"""
        can_open, message = self.can_open_order(symbol)
        if not can_open:
            return None, message

        order_id = str(uuid.uuid4())[:8]
        timestamp = int(time.time() * 1000)

        order = Order(
            order_id=order_id,
            symbol=symbol,
            order_type=order_type,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            position_size=position_size,
            timestamp=timestamp,
            pattern=pattern,
            leverage=leverage,
            position_amount_usdt=position_amount_usdt
        )

        self.open_orders[symbol] = order

        # Log mở lệnh
        if self.trading_logger:
            self.trading_logger.info(
                f"📈 MỞ LỆNH | ID: {order_id} | {symbol} | {order_type} | {pattern} | "
                f"Entry: {entry_price:.6f} | SL: {stop_loss:.6f} | TP: {take_profit:.6f} | "
                f"Size: {position_size:.6f} | Leverage: {leverage}x | "
                f"Position Amount: ${position_amount_usdt:.2f} | "
                f"Position Value: ${order.position_value:.2f} | "
                f"Balance: ${self.account_balance:.2f}"
            )

        return order, "Lệnh đã mở thành công"

    def check_order_conditions(self, symbol, current_price):
        """Kiểm tra điều kiện đóng lệnh"""
        if symbol not in self.open_orders:
            return None, None

        order = self.open_orders[symbol]

        # Kiểm tra Take Profit
        if order.order_type == "SELL" and current_price <= order.take_profit:
            return "WIN", order.take_profit
        elif order.order_type == "BUY" and current_price >= order.take_profit:
            return "WIN", order.take_profit

        # Kiểm tra Stop Loss
        if order.order_type == "SELL" and current_price >= order.stop_loss:
            return "LOSS", order.stop_loss
        elif order.order_type == "BUY" and current_price <= order.stop_loss:
            return "LOSS", order.stop_loss

        return None, None

    def close_order(self, symbol, current_price, result, close_time=None):
        """Đóng lệnh"""
        if symbol not in self.open_orders:
            return None, "Không tìm thấy lệnh"

        order = self.open_orders[symbol]

        if close_time is None:
            close_time = int(time.time() * 1000)

        # Tính PnL
        pnl_usdt = order.close_order(current_price, result, close_time)

        # Cập nhật số dư
        self.account_balance += pnl_usdt
        # self.total_pnl_usdt += pnl_usdt # This line was removed as per the edit hint

        # Chuyển sang danh sách đã đóng
        self.closed_orders.append(order)
        del self.open_orders[symbol]

        # Log kết quả
        if self.trading_logger:
            status_emoji = "💰" if result == "WIN" else "💸"
            self.trading_logger.info(
                f"{status_emoji} ĐÓNG LỆNH | ID: {order.order_id} | {symbol} | {order.order_type} | {order.pattern} | "
                f"Kết quả: {result} | Giá đóng: {current_price:.6f} | "
                f"Leverage: {order.leverage}x | PnL: {order.pnl_percentage:+.2f}% | "
                f"PnL USDT: {pnl_usdt:+.2f} | Balance: ${self.account_balance:.2f} | "
                f"Thời gian: {(close_time - order.open_time) / 1000:.1f}s"
            )

        return order, f"Lệnh đã đóng - {result}"

    def get_trading_summary(self):
        """Lấy tổng kết trading"""
        total_orders = len(self.closed_orders)
        winning_orders = len([o for o in self.closed_orders if o.result == "WIN"])
        losing_orders = len([o for o in self.closed_orders if o.result == "LOSS"])
        win_rate = (winning_orders / total_orders * 100) if total_orders > 0 else 0

        total_pnl_usdt = sum(order.pnl_usdt for order in self.closed_orders)
        total_percentage = ((self.account_balance - self.initial_balance) / self.initial_balance) * 100

        return {
            'total_orders': total_orders,
            'winning_orders': winning_orders,
            'losing_orders': losing_orders,
            'win_rate': win_rate,
            'total_pnl_usdt': total_pnl_usdt,
            'total_percentage': total_percentage,
            'current_balance': self.account_balance,
            'initial_balance': self.initial_balance,
            'open_orders': len(self.open_orders)
        }
import time
import logging


class OrderMonitor:
    def __init__(self, config, virtual_trading, symbols_data):
        self.config = config
        self.virtual_trading = virtual_trading
        self.symbols_data = symbols_data
        self.running = False

    def start(self):
        """Bắt đầu theo dõi lệnh"""
        self.running = True
        while self.running:
            try:
                time.sleep(30)
                self.monitor_orders()
            except Exception as e:
                logging.info(f"Lỗi monitor: {e}")

    def monitor_orders(self):
        """Theo dõi lệnh"""
        open_orders_count = len(self.virtual_trading.open_orders)
        if open_orders_count > 0:
            logging.info(f"📊 ĐANG THEO DÕI {open_orders_count} LỆNH:")
            total_unrealized = 0

            for symbol, order in self.virtual_trading.open_orders.items():
                current_price = self.symbols_data[symbol].current_price if symbol in self.symbols_data else None
                if current_price:
                    # Tính ROI %
                    if order.order_type == "SELL":
                        roi_pct = ((order.entry_price - current_price) / order.entry_price) * 100 * order.leverage
                    else:  # BUY
                        roi_pct = ((current_price - order.entry_price) / order.entry_price) * 100 * order.leverage

                    # PnL theo vốn gốc
                    pnl_usdt = order.position_amount_usdt * (roi_pct / 100)
                    total_unrealized += pnl_usdt

                    logging.info(
                        f"   {symbol} {order.order_type} | ROI: {roi_pct:+.2f}% | PnL: ${pnl_usdt:+.2f}")

            summary = self.virtual_trading.get_trading_summary()
            logging.info(
                f"📈 TỔNG KẾT | Mở: {open_orders_count} | "
                f"Win Rate: {summary['win_rate']:.1f}% | "
                f"Tổng PnL: ${summary['total_pnl_usdt']:+.2f} | "
                f"Unrealized: ${total_unrealized:+.2f} | "
                f"Balance: ${summary['current_balance']:.2f}"
            )

    def stop(self):
        """Dừng theo dõi lệnh"""
        self.running = False
        logging.info("✅ Đã dừng order monitor")
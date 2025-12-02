import os
from itertools import islice

from binance import Client, ThreadedWebsocketManager
import queue
import threading
import logging
import signal
import time
from .binance_core import BinanceCore
from binance.enums import *
from collections import deque, defaultdict


class BotPro:
    def __init__(self, api_key, secret_key, demo=False):
        self.api_key = api_key
        self.secret_key = secret_key
        self.running = True
        self.demo = False
        self.binance = BinanceCore(self.api_key, self.secret_key, self.demo)
        self.timeframe = "1m"
        self.usdt = 0.5
        self.leverage = 20
        self.oc = 5
        self.kline = 6
        self.tp = (self.usdt * 0.6)
        self.sl = -1 * (self.usdt * 2)
        self.tp_stop = (self.usdt * 0.4)
        symbols = self.get_top_coins()
        self.all_symbols = {sym: deque(maxlen=self.kline) for sym in symbols}
        self.trailing_stop = dict()
        self.worker_count = max(2, min(8, (os.cpu_count() or 4)))
        self.queue_workers = []
        self.symbol_locks = defaultdict(threading.Lock)

        # Queue chứa message WebSocket
        self.message_queue = queue.Queue(maxsize=50000)
        self.twm = ThreadedWebsocketManager(
            api_key=self.api_key,
            api_secret=self.secret_key,
            testnet=self.demo,
            max_queue_size=50000
        )

        self.orders = dict()
        self.tp_orders = dict()
        self.sl_orders = dict()
        self.tp_part_orders = dict()
        self.hedge_orders = dict()
        self.get_current_orders()

    # ------------------------------------------
    # START BOT
    # ------------------------------------------
    def start(self):

        # Bắt tín hiệu stop
        def signal_handler(sig, frame):
            logging.info("🛑 STOP signal nhận...")
            self.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Bắt đầu WebSocket
        self.twm.start()
        streams = self.get_top_coins()
        socket_streams = [f"{c.lower()}@kline_{self.timeframe}" for c in streams]

        # Hàm chia batch
        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        # Tách mỗi batch 50
        batches = list(chunk_list(socket_streams, 50))
        for batch in batches:
            self.twm.start_futures_multiplex_socket(
                callback=self._handle_multi_signal_kline, streams=batch
            )
            print(f'Đang theo dõi {batch}')
        # theo dõi khớp lệnh
        self.twm.start_futures_user_socket(callback=self._handle_user_stream)
        threading.Thread(target=self.twm.join, daemon=True).start()

        self._start_queue_workers()

        # Main loop giữ bot chạy
        while self.running:
            self._handle_multi_kline_order_queue()
            time.sleep(0.005)

    def get_current_orders(self):
        orders = self.binance.get_all_positions()
        for order in orders:
            self.orders[order["symbol"]] = order
        print(self.orders)
        return self.orders

    def _handle_user_stream(self, msg):
        if msg['e'] == 'ORDER_TRADE_UPDATE':
            data = msg['o']
            symbol = data['s']
            order_id = int(data['i'])
            status = data['X']
            execution_type = data['x']
            order_type = data['ot']

            # Khi lệnh entry khớp
            if status == 'FILLED' and execution_type == 'TRADE':
                print(f"✅ Entry {symbol} đã khớp hoàn toàn (OrderID: {order_id}) lệnh {data['o']}")
                print(f"✅ MSG data: {data})")

                if order_type in ('TAKE_PROFIT_MARKET', 'STOP_MARKET', 'MARKET', 'TAKE_PROFIT', 'STOP'):
                    self.tp_orders.pop(symbol, None)
                    self.hedge_orders.pop(symbol, None)
                    self.sl_orders.pop(symbol, None)
                    self.orders.pop(symbol, None)
                    self.binance.clear_order(symbol)
                    self.trailing_stop.pop(symbol, None)
                    self.all_symbols[symbol] = deque(islice(self.all_symbols[symbol], 4, None))

                self.get_current_orders()

    def _handle_multi_signal_kline(self, msg):
        data = msg.get("data", {})
        symbol = data.get("s")
        kline = data.get("k")
        close_price = float(kline.get("c"))
        max_price = float(kline.get("h"))
        low_price = float(kline.get("l"))

        if kline.get("x"):
            if symbol in self.orders:
                orders = self.binance.get_limit_orders(symbol=symbol)
                for order in orders:
                    self.binance.cancel_order(symbol, order)
                    self.orders.pop(symbol, None)

                self.get_current_orders()

            self.message_queue.put(kline)

        self.handle_pnl(symbol, close_price, max_price, low_price)

    def handle_pnl(self, symbol, close_price, max_price, low_price):
        if symbol not in self.orders:
            return

        position = self.orders[symbol]
        qty = float(position.get('positionAmt', 0))
        entry = float(position.get('entryPrice', 0))
        if qty < 0:
            pnl = (entry - close_price) * abs(qty)
        else:
            pnl = (close_price - entry) * abs(qty)

        print(f'{symbol} PNL: {round(pnl, 2)}')
        offset = (abs(pnl) / 100)
        # if pnl > 0 and pnl >= self.tp_stop:
        #     if symbol not in self.trailing_stop:
        #         stop_be = self.binance.create_stop_loss_be(symbol=symbol)
        #         if stop_be:
        #             self.trailing_stop[symbol] = stop_be

        if pnl > 0 and pnl >= self.tp:
            if symbol not in self.tp_orders:
                if qty < 0:
                    side = "BUY"
                    price = close_price * (1 - offset)
                else:
                    side = "SELL"
                    price = close_price * (1 + offset)

                order = self.binance.create_order_take_profit(
                    symbol=symbol,
                    side=side,
                    price=price,
                    quantity=abs(qty)
                )

                if order:
                    self.tp_orders[symbol] = order

        if pnl < 0 and pnl <= self.sl:
            # Cắt lỗ 1 phần
            if symbol not in self.sl_orders:
                if qty < 0:
                    side = "BUY"
                    price = close_price * (1 + offset)
                else:
                    side = "SELL"
                    price = close_price * (1 - offset)

                print(f'{symbol} price SL: {price}, {close_price}')
                order = self.binance.create_order_stop_loss(
                    symbol=symbol,
                    side=side,
                    price=price,
                    quantity=abs(qty)
                )
                if order:
                    self.sl_orders[symbol] = order

    def handle_signal(self, symbol):
        closes = self.all_symbols[symbol]
        if len(closes) < self.kline:
            return None  # chưa đủ dữ liệu

        start = closes[0]  # Giá cũ nhất
        end = closes[self.kline - 1]  # Giá mới nhất

        percent = ((end - start) / start) * 100
        side = 'BUY' if start > end else 'SELL'
        return end, abs(percent), side

    def _handle_multi_kline_order_queue(self):
        data = self.message_queue.get()
        symbol = data.get("s")

        if symbol in self.orders:
            return

        open_price = float(data.get("o"))
        close_price = float(data.get("c"))
        dq = self.all_symbols[symbol]
        dq.append(close_price)

        if len(dq) == self.kline:
            price, change, side = self.handle_signal(symbol)

            if change >= self.oc:
                print(symbol, change)
                quantity = (self.usdt * self.leverage) / abs(price)

                if not self.binance.can_make_order(symbol):
                    return

                order = self.binance.create_order(
                    symbol=symbol,
                    side=side,
                    entry_price=abs(price),
                    quantity=quantity,
                    order_type=FUTURE_ORDER_TYPE_LIMIT,
                )

                if order:
                    self.orders[symbol] = order

    def _process_kline_message(self, data):
        symbol = data.get("s")
        if not symbol or symbol not in self.all_symbols:
            return

        symbol_lock = self.symbol_locks[symbol]
        with symbol_lock:
            if symbol in self.orders:
                return

            close_price = float(data.get("c"))
            dq = self.all_symbols[symbol]
            dq.append(close_price)

            if len(dq) == self.kline:
                result = self.handle_signal(symbol)
                if not result:
                    return

                price, change, side = result

                if change >= self.oc:
                    print(symbol, change)
                    quantity = (self.usdt * self.leverage) / abs(price)
                    if not self.binance.can_make_order(symbol):
                        return

                    order = self.binance.create_order(
                        symbol=symbol,
                        side=side,
                        entry_price=abs(price),
                        quantity=quantity,
                        order_type=FUTURE_ORDER_TYPE_LIMIT,
                    )

                    if order:
                        self.orders[symbol] = order

    # def _handle_multi_kline_order_queue(self):
    #     while self.running or not self.message_queue.empty():
    #         try:
    #             data = self.message_queue.get(timeout=0.01)
    #             print(data)
    #         except queue.Empty:
    #             continue
    #
    #         if data is None:
    #             self.message_queue.task_done()
    #             break
    #
    #         try:
    #             self._process_kline_message(data)
    #         finally:
    #             self.message_queue.task_done()

    def _start_queue_workers(self):
        if self.queue_workers:
            return

        for idx in range(self.worker_count):
            worker = threading.Thread(
                target=self._handle_multi_kline_order_queue,
                name=f"kline-worker-{idx}",
                daemon=True,
            )
            self.queue_workers.append(worker)
            worker.start()

    # ------------------------------------------
    def stop(self):
        print("🛑 Đang tắt bot...")

        self.running = False
        self.twm.stop()

        for _ in self.queue_workers:
            self.message_queue.put(None)

        for worker in self.queue_workers:
            worker.join(timeout=1)

        # Đợi queue hoàn tất
        try:
            self.message_queue.join()
        except:
            pass

        print("✅ Bot dừng hoàn toàn.")

    # ------------------------------------------
    # Chọn coin để listen
    # ------------------------------------------
    def get_top_coins(self):
        return self.binance.get_top_volatile_liquid_symbols()

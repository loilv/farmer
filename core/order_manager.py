# -*- coding: utf-8 -*-
"""
Module order_manager đơn giản để tạo order
"""

import requests
import hmac
import hashlib
import time
import logging
from typing import Dict, Any, Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class OrderBinanceManager:
    def __init__(self, config):
        self.api_key = config.api_key
        self.secret_key = config.secret_key
        self.base_url = config.base_url
        self.leverage = config.leverage
        self.position_size_usdt = config.position_size_usdt

    def _generate_signature(self, params: str) -> str:
        """Tạo chữ ký cho API request"""
        return hmac.new(
            self.secret_key.encode('utf-8'),
            params.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def _make_request(self, method: str, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Thực hiện API request với rate limiting và retry"""
        url = f"{self.base_url}{endpoint}"

        if params is None:
            params = {}

        params['timestamp'] = int(time.time() * 1000)
        query_string = urlencode(params)
        signature = self._generate_signature(query_string)
        params['signature'] = signature

        headers = {
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        if method.upper() == 'GET':
            response = requests.get(url, params=params, headers=headers)
        elif method.upper() == 'POST':
            response = requests.post(url, data=params, headers=headers)
        elif method.upper() == 'DELETE':
            response = requests.delete(url, params=params, headers=headers)
        else:
            response = requests.request(method, url, data=params, headers=headers)

        response.raise_for_status()
        return response.json()

    def create_listen_key(self):
        """Tạo listen key cho User Data Stream"""
        try:
            response = self._make_request('POST', '/fapi/v1/listenKey')
            return response.get('listenKey')
        except Exception as e:
            logging.info(f"❌ Lỗi tạo listen key: {e}")
            return None

    def keepalive_listen_key(self, listen_key):
        """Gia hạn listen key"""
        try:
            params = {'listenKey': listen_key}
            self._make_request('POST', '/fapi/v1/listenKey', params=params)
        except Exception as e:
            logging.info(f"❌ Lỗi gia hạn listen key: {e}")

    def get_balance(self) -> float:
        """Lấy số dư USDT"""
        try:
            result = self._make_request('GET', '/fapi/v2/account')
            if 'assets' in result:
                for asset in result['assets']:
                    if asset['asset'] == 'USDT':
                        return float(asset['walletBalance'])
            return 0.0
        except Exception as e:
            logger.error(f"Lỗi lấy số dư: {e}")
            return 0.0

    def _format_quantity(self, symbol: str, quantity: float) -> float:
        """Format quantity theo step size của symbol"""
        try:
            # Lấy thông tin symbol từ exchange info
            exchange_info = self._get_exchange_info()
            if not exchange_info:
                return round(quantity, 3)

            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    filters = symbol_info.get('filters', [])
                    for filter_info in filters:
                        if filter_info['filterType'] == 'LOT_SIZE':
                            step_size = float(filter_info['stepSize'])
                            formatted_qty = round(quantity / step_size) * step_size
                            return round(formatted_qty, 8)

            return round(quantity, 3)

        except Exception as e:
            return round(quantity, 3)

    def _format_price(self, symbol: str, price: float) -> float:
        """Format price theo tick size của symbol"""
        try:
            exchange_info = self._get_exchange_info()
            if not exchange_info:
                return round(price, 2)

            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info['symbol'] == symbol:
                    filters = symbol_info.get('filters', [])
                    for filter_info in filters:
                        if filter_info['filterType'] == 'PRICE_FILTER':
                            tick_size = float(filter_info['tickSize'])
                            formatted_price = round(price / tick_size) * tick_size
                            return round(formatted_price, 8)

            return round(price, 2)

        except Exception as e:
            return round(price, 2)

    def _get_exchange_info(self) -> Optional[Dict[str, Any]]:
        """Lấy thông tin exchange info (cache trong 1 phút)"""
        try:
            current_time = time.time()
            if hasattr(self, '_exchange_info_cache') and hasattr(self, '_exchange_info_time'):
                if current_time - self._exchange_info_time < 60:
                    return self._exchange_info_cache

            result = self._make_request('GET', '/fapi/v1/exchangeInfo')
            self._exchange_info_cache = result
            self._exchange_info_time = current_time
            return result

        except Exception as e:
            return None

    def create_order(self, symbol: str, side: str, quantity: float, price: float = None, order_type: str = 'MARKET') -> \
            Optional[Dict[str, Any]]:
        """
        Tạo lệnh mới
        
        Args:
            symbol: Symbol cần tạo lệnh
            side: 'BUY' hoặc 'SELL'
            quantity: Số lượng
            price: Giá (cho LIMIT order)
            order_type: 'MARKET' hoặc 'LIMIT'
            
        Returns:
            Dict chứa thông tin order hoặc None
        """
        try:
            # Format quantity
            formatted_quantity = self._format_quantity(symbol, quantity)

            if formatted_quantity <= 0:
                logger.error(f"Quantity không hợp lệ cho {symbol}: {formatted_quantity}")
                return None

            params = {
                'symbol': symbol,
                'side': side,
                'type': order_type,
                'quantity': formatted_quantity
            }

            if order_type == 'LIMIT' and price:
                formatted_price = self._format_price(symbol, price)
                params['price'] = formatted_price
                params['timeInForce'] = 'GTC'

            result = self._make_request('POST', '/fapi/v1/order', params)

            if 'orderId' in result:
                logger.info(f"✅ Đã tạo lệnh {order_type}: {side} {formatted_quantity} {symbol}")
                if price:
                    logger.info(f"   Giá: {price:.6f}")
                return result
            else:
                logger.error(f"❌ Lỗi tạo lệnh {symbol}: {result}")
                return None

        except Exception as e:
            logger.error(f"Lỗi tạo lệnh {symbol}: {e}")
            return None

    def calculate_position_size(self, symbol: str, current_price: float) -> float:
        """
        Tính toán kích thước position
        
        Args:
            symbol: Symbol cần tính
            balance: Số dư tài khoản
            
        Returns:
            Số lượng position
        """
        try:
            if not current_price:
                return 0.0

            # Tính số tiền vào lệnh theo USDT cố định
            position_value = self.position_size_usdt

            # Tính số lượng với leverage
            quantity = (position_value * self.leverage) / current_price

            # Làm tròn theo quy tắc của Binance
            quantity = round(quantity, 3)

            logger.info(f"Position size: {quantity} {symbol} (${position_value:.2f})")
            return quantity

        except Exception as e:
            logger.error(f"Lỗi tính position size {symbol}: {e}")
            return 0.0

    def create_take_profit_limit(self, symbol, side, quantity, price, positionSide="BOTH"):
        """
        Lệnh chốt lời (TP) - TAKE_PROFIT_LIMIT
        Tự động tính stop_price = price ± 5% tùy theo hướng lệnh
        """
        endpoint = "/fapi/v1/order"

        # Tính stop_price cách price 5%
        tp_percent = 0.05
        if side.upper() == "BUY":
            # Lệnh BUY (Long): chốt lời khi giá tăng -> stop_price thấp hơn 1 chút
            stop_price = price * (1 - tp_percent)
        else:
            # Lệnh SELL (Short): chốt lời khi giá giảm -> stop_price cao hơn 1 chút
            stop_price = price * (1 + tp_percent)

        # Format dữ liệu theo tick/lot size
        formatted_quantity = self._format_quantity(symbol, quantity)
        formatted_price = self._format_price(symbol, price)
        formatted_stop = self._format_price(symbol, stop_price)

        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "TAKE_PROFIT_LIMIT",
            "timeInForce": "GTC",
            "quantity": formatted_quantity,
            "price": formatted_price,  # giá khớp TP
            "stopPrice": formatted_stop,  # giá kích hoạt TP
            "positionSide": positionSide,
            "reduceOnly": "true",
            "recvWindow": 5000,
            "timestamp": int(time.time() * 1000)
        }

        return self._make_request("POST", endpoint, params)

    def create_stop_loss_limit(self, symbol, side, quantity, price, positionSide="BOTH"):
        """
        Lệnh cắt lỗ (SL) - STOP_LIMIT
        Tự động tính stop_price = price ± 5% tùy theo hướng lệnh
        """
        endpoint = "/fapi/v1/order"

        # Tính stop_price cách entry 5%
        stop_percent = 0.05
        if side.upper() == "BUY":
            stop_price = price * (1 + stop_percent)  # SL cho lệnh BUY → giá giảm thì lỗ, nên đặt cao hơn giá khớp Limit SELL
        else:
            stop_price = price * (1 - stop_percent)  # SL cho lệnh SELL → giá tăng thì lỗ, nên đặt thấp hơn giá khớp Limit BUY

        # Định dạng số theo tick size, lot size
        formatted_quantity = self._format_quantity(symbol, quantity)
        formatted_price = self._format_price(symbol, price)
        formatted_stop = self._format_price(symbol, stop_price)

        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "STOP_LIMIT",  # STOP_LIMIT để chặn lỗ bằng limit
            "timeInForce": "GTC",
            "quantity": formatted_quantity,
            "price": formatted_price,  # Giá khớp SL
            "stopPrice": formatted_stop,  # Giá kích hoạt SL
            "positionSide": positionSide,
            "reduceOnly": "true",
            "recvWindow": 5000,
            "timestamp": int(time.time() * 1000)
        }

        return self._make_request("POST", endpoint, params)


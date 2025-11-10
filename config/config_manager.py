import yaml
import logging


class Config:
    def __init__(self, config_path='config/config.yaml'):
        self.config_path = config_path
        self.load_config()

    def load_config(self):
        """Tải cấu hình từ file YAML"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)

            self.symbols = config_data.get('symbols', [])
            self.timeframe = config_data.get('timeframe', '5m')
            self.signal_time_frame = config_data.get('signal_time_frame', '5m')

            # Cấu hình logging
            logging_config = config_data.get('logging', {})
            self.log_level = logging_config.get('level', 'INFO')
            self.log_file = logging_config.get('log_file', 'logs/candle_scanner.log')
            self.signals_log_file = logging_config.get('signals_log_file', 'logs/trading_signals.log')
            self.trading_log_file = logging_config.get('trading_log_file', 'logs/trading_results.log')
            self.max_file_size = logging_config.get('max_file_size_mb', 100) * 1024 * 1024
            self.backup_count = logging_config.get('backup_count', 5)

            # Cấu hình phân tích
            analysis_config = config_data.get('analysis', {})
            self.update_interval = analysis_config.get('update_interval', 1.0)
            self.max_symbols = analysis_config.get('max_symbols_per_connection', 200)
            self.scan_all_pairs = analysis_config.get('scan_all_pairs', True)

            # Binance
            binance_config = config_data.get('binance', {})
            self.api_key = binance_config.get('api_key', '')
            self.secret_key = binance_config.get('secret_key', '')
            self.base_url = binance_config.get('base_url', '')
            self.testnet = binance_config.get('testnet', True)

            # Cấu hình trading
            trading_config = config_data.get('trading', {})
            self.stop_loss_percentage = trading_config.get('stop_loss_percentage', 1.0)
            self.take_profit_percentage = trading_config.get('take_profit_percentage', 1.5)
            self.leverage = trading_config.get('leverage', 10)
            self.position_size_usdt = trading_config.get('position_size_usdt', 100)

        except Exception as e:
            print(f"❌ Lỗi tải file cấu hình: {e}")
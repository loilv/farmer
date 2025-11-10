import pandas as pd
import numpy as np

class RSI:
    def __init__(self, length=9):
        self.length = length
        self.avg_gain = None
        self.avg_loss = None
        self.initialized = False

    def calculate_series(self, close_prices: pd.Series) -> pd.Series:
        delta = close_prices.diff()

        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        # SMA lần đầu
        avg_gain = gain.rolling(self.length).mean()
        avg_loss = loss.rolling(self.length).mean()

        # Tạo Series RSI rỗng
        rsi = pd.Series(dtype=float, index=close_prices.index)

        # Tính RSI với Wilder Smoothing
        for i in range(len(close_prices)):
            if i < self.length:
                rsi.iloc[i] = np.nan
            elif i == self.length:
                self.avg_gain = avg_gain.iloc[i]
                self.avg_loss = avg_loss.iloc[i]
                rs = self.avg_gain / self.avg_loss if self.avg_loss != 0 else np.inf
                rsi.iloc[i] = 100 - (100 / (1 + rs))
                self.initialized = True
            else:
                change = delta.iloc[i]
                gain_val = max(change, 0)
                loss_val = max(-change, 0)

                self.avg_gain = ((self.avg_gain * (self.length - 1)) + gain_val) / self.length
                self.avg_loss = ((self.avg_loss * (self.length - 1)) + loss_val) / self.length

                rs = self.avg_gain / self.avg_loss if self.avg_loss != 0 else np.inf
                rsi.iloc[i] = 100 - (100 / (1 + rs))

        return rsi

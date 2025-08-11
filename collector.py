# /app/collector.py

import os
import pandas as pd
import numpy as np
import asyncio
from binance import AsyncClient, BinanceSocketManager
from datetime import datetime, timezone

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
FILEPATH = os.path.join(DATA_DIR, "btc_eur_1m_clean.csv")
ROLL_WINDOW = 7770

AGGREGATES = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h"
}

# --- Додаємо всі індикатори, включаючи Bollinger Bands ---
def add_indicators(df):
    df = df.copy()
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up / (roll_down + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['ma7'] = df['close'].rolling(window=7).mean()
    df['ma25'] = df['close'].rolling(window=25).mean()
    df['macd'] = df['ema12'] - df['ema26']
    obv = [0]
    for i in range(1, len(df)):
        if df['close'].iloc[i] > df['close'].iloc[i - 1]:
            obv.append(obv[-1] + df['volume'].iloc[i])
        elif df['close'].iloc[i] < df['close'].iloc[i - 1]:
            obv.append(obv[-1] - df['volume'].iloc[i])
        else:
            obv.append(obv[-1])
    df['obv'] = obv
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    df['atr'] = true_range.rolling(14).mean()
    # --- Bollinger Bands ---
    bb_ma = df['close'].rolling(window=20).mean()
    bb_std = df['close'].rolling(window=20).std()
    df['bb_ma'] = bb_ma
    df['bb_std'] = bb_std
    df['bb_upper'] = bb_ma + 2 * bb_std
    df['bb_lower'] = bb_ma - 2 * bb_std
    df['bb_z'] = (df['close'] - bb_ma) / (bb_std + 1e-9)
    return df

# --- Вбудований utils: чистка, ffill, median, видалення некоректних рядків ---
def clean_data(df, ind_cols, price_cols, min_rolling=30, min_rows=50):
    df_clean = df.iloc[min_rolling:].copy()
    # 2. Forward fill для поодиноких NaN по індикаторах
    df_clean[ind_cols] = df_clean[ind_cols].ffill()
    # 3. Медіана для залишкових NaN по колонках
    for col in ind_cols:
        median_val = df_clean[col].median(skipna=True)
        df_clean[col] = df_clean[col].fillna(median_val)
    # 4. Повторно ffill/bfill якщо треба
    df_clean = df_clean.ffill().bfill()
    # 5. Якщо open/high/low/close/volume NaN — видаляємо рядок
    df_clean = df_clean.dropna(subset=price_cols)
    if len(df_clean) < min_rows:
        return pd.DataFrame()
    return df_clean

# --- Агрегація по таймфрейму + чистка ---
def aggregate_and_save(df_1m, freq, filename, ind_cols, price_cols):
    df = df_1m.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    df = df.set_index('timestamp')
    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }
    agg = df.resample(freq).agg(ohlc).dropna().reset_index()
    agg = add_indicators(agg)
    agg = clean_data(agg, ind_cols, price_cols, min_rolling=30, min_rows=10)
    if len(agg) > ROLL_WINDOW:
        agg = agg.iloc[-ROLL_WINDOW:].reset_index(drop=True)
    if not agg.empty:
        agg.to_csv(filename, index=False)

async def main():
    columns = ["timestamp", "open", "high", "low", "close", "volume"]
    ind_cols = ['rsi', 'ema12', 'ema26', 'ma7', 'ma25', 'macd', 'obv', 'atr',
                'bb_ma', 'bb_std', 'bb_upper', 'bb_lower', 'bb_z']
    price_cols = ['open', 'high', 'low', 'close', 'volume']

    # Ініціалізація DataFrame
    if os.path.exists(FILEPATH):
        df = pd.read_csv(FILEPATH, parse_dates=['timestamp'])
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    else:
        df = pd.DataFrame(columns=columns)

    print(f"🟢 Collector-Integrator запущено. Початкових рядків: {len(df)}")
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    socket = bm.kline_socket(symbol="btceur", interval=AsyncClient.KLINE_INTERVAL_1MINUTE)

    async with socket as s:
        while True:
            msg = await s.recv()
            if 'k' not in msg:
                continue  # skip non-kline messages
            k = msg['k']
            if not k['x']:
                continue

            row = {
                "timestamp": datetime.fromtimestamp(k['T'] / 1000, tz=timezone.utc),
                "open": float(k['o']),
                "high": float(k['h']),
                "low": float(k['l']),
                "close": float(k['c']),
                "volume": float(k['v'])
            }
            if df.empty:
                df = pd.DataFrame([row])
            else:
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            # Обрізаємо історію
            if len(df) > ROLL_WINDOW:
                df = df.iloc[-ROLL_WINDOW:].reset_index(drop=True)

            # Додаємо індикатори
            df = add_indicators(df)
            # Чистимо дані (тільки якщо зібрали хоча б 30 рядків)
            if len(df) >= 30:
                df_clean = clean_data(df, ind_cols, price_cols, min_rolling=30, min_rows=30)
                if not df_clean.empty:
                    df_clean.to_csv(FILEPATH, index=False)
                    print(f"✅ Додано {len(df_clean)} чистих рядків. Останній close: {row['close']:.2f}, час: {row['timestamp']}")
                    # Оновлюємо всі таймфрейми
                    for key, rule in AGGREGATES.items():
                        fname = os.path.join(DATA_DIR, f"btc_eur_{key}_clean.csv")
                        aggregate_and_save(df_clean, rule, fname, ind_cols, price_cols)
            else:
                print(f"⏳ Мало рядків ({len(df)}), чекаємо наповнення для індикаторів/clean...")

            await asyncio.sleep(1)

    await client.close_connection()

if __name__ == "__main__":
    asyncio.run(main())

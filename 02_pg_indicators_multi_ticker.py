# Nombre de archivo: pg_indicators_multi_ticker.py
# Script para calcular indicadores históricos para múltiples tickers y timeframes.
# Versión con ATR%, Volume Oscillator y precio_actual, adaptado para PostgreSQL
# Modificado para manejar timestamp_col y timestamp_last

import pandas as pd
import psycopg2
import talib
import numpy as np
from datetime import datetime, timedelta
import pytz
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('indicators_calc.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Importar de parmspg.py
from parmspg import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

# --- Parámetros de Indicadores ---
SMA_PERIODS = [10, 20, 50, 100, 200]
EMA_PERIODS = [10, 20, 50, 100, 200]
RSI_BASE_PERIOD = 14
RSI_SMOOTH_PERIOD = 5

MACD_FAST_PERIOD = 12
MACD_SLOW_PERIOD = 26
MACD_SIGNAL_PERIOD = 9

ATR_PERIOD = 14
VO_FAST_PERIOD = 5
VO_SLOW_PERIOD = 10

# --- NOMBRES DE COLUMNAS DE INDICADORES GENERADOS ---
RSI_SMOOTH_COL_NAME = f'rsi_{RSI_BASE_PERIOD}_smooth'
INDICATOR_COLUMN_NAMES = [f'sma_{p}' for p in SMA_PERIODS] + \
                        [f'ema_{p}' for p in EMA_PERIODS] + \
                        [f'rsi_{RSI_BASE_PERIOD}', RSI_SMOOTH_COL_NAME] + \
                        ['macd', 'macd_signal', 'macd_hist'] + \
                        ['atr_percent', 'volumen_osc', 'precio_actual']

# Determinar el lookback máximo necesario para los cálculos
LOOKBACK_PERIODS_CALC = []
if SMA_PERIODS:
    LOOKBACK_PERIODS_CALC.append(max(SMA_PERIODS))
if EMA_PERIODS:
    LOOKBACK_PERIODS_CALC.append(max(EMA_PERIODS))
if RSI_BASE_PERIOD:
    LOOKBACK_PERIODS_CALC.append(RSI_BASE_PERIOD + RSI_SMOOTH_PERIOD)
if MACD_FAST_PERIOD and MACD_SLOW_PERIOD and MACD_SIGNAL_PERIOD:
    LOOKBACK_PERIODS_CALC.append(MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD)
if ATR_PERIOD:
    LOOKBACK_PERIODS_CALC.append(ATR_PERIOD)
if VO_FAST_PERIOD and VO_SLOW_PERIOD:
    LOOKBACK_PERIODS_CALC.append(VO_SLOW_PERIOD)

MAX_LOOKBACK_PERIOD = max(LOOKBACK_PERIODS_CALC) if LOOKBACK_PERIODS_CALC else 0
EFFECTIVE_LOOKBACK_PERIODS = MAX_LOOKBACK_PERIOD + 50
logger.info(f"Configuración: Lookback efectivo (en periodos del timeframe): {EFFECTIVE_LOOKBACK_PERIODS}")
logger.info(f"Configuración: MAX_LOOKBACK_PERIOD (técnico para TA-Lib): {MAX_LOOKBACK_PERIOD}")

# --- Funciones Auxiliares ---
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cursor = conn.cursor()
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.close()
        logger.info("Conexión a PostgreSQL establecida (Sesión en UTC).")
        return conn
    except psycopg2.Error as err:
        logger.error(f"Error al conectar a PostgreSQL: {err}")
        return None

def get_active_tickers(conn):
    active_tickers_list = []
    cursor = None
    try:
        cursor = conn.cursor()
        query = "SELECT ticker FROM tickers WHERE activo = TRUE ORDER BY ticker"
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            active_tickers_list = [row[0] for row in results]
        logger.info(f"Tickers activos obtenidos: {active_tickers_list}")
        return active_tickers_list
    except psycopg2.Error as err:
        logger.error(f"Error al obtener tickers activos: {err}")
        return []
    finally:
        if cursor:
            cursor.close()

def get_active_timeframes(conn):
    cursor = None
    try:
        cursor = conn.cursor()
        query = "SELECT id_timeframe FROM timeframes ORDER BY id_timeframe"
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            timeframes = [row[0] for row in results]
            logger.info(f"Timeframes activos obtenidos: {timeframes}")
            return timeframes
        logger.warning("No se encontraron timeframes en la tabla 'timeframes'.")
        return []
    except psycopg2.Error as err:
        logger.error(f"Error al obtener timeframes: {err}")
        return []
    finally:
        if cursor:
            cursor.close()

def fetch_ohlcv_data(conn, ticker, timeframe_minutes, fetch_data_from_dt_utc, fetch_data_until_dt_utc):
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
            SELECT timestamp, open, high, low, close, volume, timestamp_col, timestamp_last
            FROM ohlcv
            WHERE ticker = %s AND timeframe = %s AND timestamp >= %s AND timestamp < %s
            ORDER BY timestamp ASC
        """
        params = [
            ticker,
            timeframe_minutes,
            fetch_data_from_dt_utc,
            fetch_data_until_dt_utc
        ]
        cursor.execute(query, params)
        data = cursor.fetchall()
        logger.info(f"OHLCV obtenidos para {ticker} TF:{timeframe_minutes}m: {len(data)} filas (Rango: {params[2]} a {params[3]})")
        if data:
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'timestamp_col', 'timestamp_last'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
            else:
                df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
            df['timestamp_col'] = pd.to_datetime(df['timestamp_col'])
            df['timestamp_last'] = pd.to_datetime(df['timestamp_last'])
            return df
        else:
            return pd.DataFrame()
    except psycopg2.Error as err:
        logger.error(f"Error al obtener datos OHLCV para {ticker} TF:{timeframe_minutes}m: {err}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()

# --- Calcular Indicadores ---
def calculate_indicators(df_ohlcv, ticker, timeframe_minutes):
    """Calcula indicadores técnicos sobre el DataFrame OHLCV."""
    if df_ohlcv.empty:
        return df_ohlcv

    if len(df_ohlcv) < MAX_LOOKBACK_PERIOD:
        logger.warning(f"Datos OHLCV insuficientes ({len(df_ohlcv)} filas) para {ticker} TF:{timeframe_minutes}m (Req. técnico: {MAX_LOOKBACK_PERIOD}). Algunos indicadores serán NaN.")

    logger.info(f"Calculando indicadores sobre {len(df_ohlcv)} filas OHLCV para {ticker} TF:{timeframe_minutes}m...")
    df_with_indicators = df_ohlcv.copy()
    high_prices = df_with_indicators['high'].astype(float)
    low_prices = df_with_indicators['low'].astype(float)
    close_prices = df_with_indicators['close'].astype(float)
    volume_data = df_with_indicators['volume'].astype(float)

    if close_prices.isna().all():
        logger.error(f"No hay precios de cierre válidos para {ticker} TF:{timeframe_minutes}m. No se pueden calcular indicadores.")
        return df_with_indicators

    df_with_indicators['precio_actual'] = close_prices

    for period in SMA_PERIODS:
        if len(close_prices.dropna()) >= period:
            df_with_indicators[f'sma_{period}'] = talib.SMA(close_prices, timeperiod=period)
        else:
            df_with_indicators[f'sma_{period}'] = np.nan

    for period in EMA_PERIODS:
        if len(close_prices.dropna()) >= period:
            df_with_indicators[f'ema_{period}'] = talib.EMA(close_prices, timeperiod=period)
        else:
            df_with_indicators[f'ema_{period}'] = np.nan

    if len(close_prices.dropna()) >= RSI_BASE_PERIOD:
        rsi_values = talib.RSI(close_prices, timeperiod=RSI_BASE_PERIOD)
        df_with_indicators[f'rsi_{RSI_BASE_PERIOD}'] = rsi_values
        rsi_valid_values = rsi_values.dropna()
        if not rsi_valid_values.empty and len(rsi_valid_values) >= RSI_SMOOTH_PERIOD:
            df_with_indicators[RSI_SMOOTH_COL_NAME] = talib.EMA(rsi_values, timeperiod=RSI_SMOOTH_PERIOD)
        else:
            df_with_indicators[RSI_SMOOTH_COL_NAME] = np.nan
    else:
        df_with_indicators[f'rsi_{RSI_BASE_PERIOD}'] = np.nan
        df_with_indicators[RSI_SMOOTH_COL_NAME] = np.nan

    if len(close_prices.dropna()) >= (MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD):
        macd_line, signal_line, hist = talib.MACD(close_prices,
                                                  fastperiod=MACD_FAST_PERIOD,
                                                  slowperiod=MACD_SLOW_PERIOD,
                                                  signalperiod=MACD_SIGNAL_PERIOD)
        df_with_indicators['macd'] = macd_line
        df_with_indicators['macd_signal'] = signal_line
        df_with_indicators['macd_hist'] = hist
    else:
        df_with_indicators['macd'] = np.nan
        df_with_indicators['macd_signal'] = np.nan
        df_with_indicators['macd_hist'] = np.nan

    if len(high_prices.dropna()) >= ATR_PERIOD and len(low_prices.dropna()) >= ATR_PERIOD and len(close_prices.dropna()) >= ATR_PERIOD:
        atr_values = talib.ATR(high_prices, low_prices, close_prices, timeperiod=ATR_PERIOD)
        df_with_indicators['atr_temp'] = atr_values
        df_with_indicators['atr_percent'] = np.where(
            close_prices != 0,
            (atr_values / close_prices) * 100,
            np.nan
        )
        df_with_indicators['atr_percent'] = df_with_indicators['atr_percent'].replace([np.inf, -np.inf], np.nan)
    else:
        df_with_indicators['atr_temp'] = np.nan
        df_with_indicators['atr_percent'] = np.nan

    if len(volume_data.dropna()) >= VO_SLOW_PERIOD:
        vol_ma_fast = talib.MA(volume_data, timeperiod=VO_FAST_PERIOD)
        vol_ma_slow = talib.MA(volume_data, timeperiod=VO_SLOW_PERIOD)
        df_with_indicators['volumen_osc'] = np.where(
            vol_ma_slow != 0,
            ((vol_ma_fast - vol_ma_slow) / vol_ma_slow) * 100,
            np.nan
        )
        df_with_indicators['volumen_osc'] = df_with_indicators['volumen_osc'].replace([np.inf, -np.inf], np.nan)
    else:
        df_with_indicators['volumen_osc'] = np.nan

    if 'atr_temp' in df_with_indicators.columns:
        df_with_indicators = df_with_indicators.drop(columns=['atr_temp'])

    logger.info(f"Indicadores calculados para {ticker} TF:{timeframe_minutes}m.")
    return df_with_indicators

# --- Almacenar Indicadores ---
def store_indicators(conn, full_indicators_df, ticker, timeframe_minutes,
                    day_storage_start_dt_utc, day_storage_end_dt_utc, batch_size=1000):
    """Almacena los indicadores calculados en la tabla histórica 'indicadores'."""
    if full_indicators_df.empty:
        return 0

    df_day_to_store = full_indicators_df[
        (full_indicators_df['timestamp'] >= day_storage_start_dt_utc) &
        (full_indicators_df['timestamp'] < day_storage_end_dt_utc)
    ].copy()

    if df_day_to_store.empty:
        return 0

    cursor = None
    try:
        cursor = conn.cursor()

        # --- Incluir los campos timestamp_col y timestamp_last en la inserción ---
        cols_to_store_in_df = ['timestamp', 'timestamp_col', 'timestamp_last', 'precio_actual'] + \
                             [col for col in INDICATOR_COLUMN_NAMES if col in full_indicators_df.columns and col != 'precio_actual']

        df_subset = df_day_to_store[cols_to_store_in_df].copy()
        df_subset = df_subset.replace({np.nan: None, pd.NaT: None})

        indicator_cols_for_dropna = [col for col in cols_to_store_in_df if col not in ['timestamp', 'timestamp_col', 'timestamp_last']]
        if not indicator_cols_for_dropna or df_subset.empty:
            return 0
        df_filtered = df_subset.dropna(subset=indicator_cols_for_dropna, how='all')

        if df_filtered.empty:
            return 0

        data_tuples = []
        for index, row in df_filtered.iterrows():
            values_for_tuple = [row.get(col_name) for col_name in cols_to_store_in_df]
            final_tuple_values = [ticker, timeframe_minutes] + values_for_tuple
            data_tuples.append(tuple(final_tuple_values))

        if not data_tuples:
            return 0

        sql_cols_str = ", ".join([f'"{col}"' for col in cols_to_store_in_df])
        sql_placeholders_str = ", ".join(["%s"] * (2 + len(cols_to_store_in_df)))
        sql_update_parts = [f'"{db_col}" = EXCLUDED."{db_col}"' for db_col in cols_to_store_in_df if db_col not in ['timestamp']]
        sql_update_str = ", ".join(sql_update_parts)

        sql_base = (f'INSERT INTO indicadores ("ticker", "timeframe", {sql_cols_str}) '
                    f'VALUES ({sql_placeholders_str}) '
                    f'ON CONFLICT ("ticker", "timeframe", "timestamp") DO UPDATE '
                    f'SET {sql_update_str}')

        total_rows_affected_day = 0
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            if not batch:
                continue
            cursor.executemany(sql_base, batch)
            total_rows_affected_day += len(batch)

        conn.commit()
        return total_rows_affected_day

    except psycopg2.Error as err:
        logger.error(f"Error al almacenar indicadores para {ticker} TF:{timeframe_minutes}m día {day_storage_start_dt_utc.strftime('%Y-%m-%d')}: {err}")
        if conn:
            conn.rollback()
        return 0
    except Exception as e_store:
        logger.error(f"Excepción general en store_indicators para {ticker} TF:{timeframe_minutes}m día {day_storage_start_dt_utc.strftime('%Y-%m-%d')}: {e_store}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        return 0
    finally:
        if cursor:
            cursor.close()

# --- Función Principal ---
def main():
    OVERALL_STORAGE_START_DATE_STR = "2023-01-01"
    OVERALL_STORAGE_END_DATE_STR = "2023-02-01"

    overall_storage_start_dt_utc = datetime.strptime(OVERALL_STORAGE_START_DATE_STR, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
    )
    overall_storage_end_dt_inclusive_utc = datetime.strptime(OVERALL_STORAGE_END_DATE_STR, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
    )
    overall_process_until_dt_exclusive_utc = overall_storage_end_dt_inclusive_utc + timedelta(days=1)

    logger.info(f"Inicio del proceso de cálculo de indicadores históricos.")
    logger.info(f"Rango de almacenamiento: {overall_storage_start_dt_utc.strftime('%Y-%m-%d')} a {overall_storage_end_dt_inclusive_utc.strftime('%Y-%m-%d')}")

    conn = connect_db()
    if not conn:
        return

    active_tickers = get_active_tickers(conn)
    if not active_tickers:
        logger.error("No se encontraron tickers activos en la tabla 'tickers'. Finalizando.")
        if conn:
            conn.close()
        return

    TIMEFRAMES_TO_PROCESS_MINUTES = get_active_timeframes(conn)
    if not TIMEFRAMES_TO_PROCESS_MINUTES:
        logger.error("No hay timeframes para procesar. Finalizando.")
        if conn:
            conn.close()
        return

    try:
        for ticker_to_process_loop in active_tickers:
            logger.info(f"Procesando ticker: {ticker_to_process_loop}")

            for tf_minutes in TIMEFRAMES_TO_PROCESS_MINUTES:
                logger.info(f"Procesando timeframe: {tf_minutes}m para {ticker_to_process_loop}")

                lookback_duration_total_minutes = EFFECTIVE_LOOKBACK_PERIODS * tf_minutes
                ohlcv_fetch_start_dt_utc = overall_storage_start_dt_utc - timedelta(minutes=lookback_duration_total_minutes)
                ohlcv_fetch_end_dt_utc = overall_process_until_dt_exclusive_utc

                logger.info(f"Obteniendo OHLCV desde {ohlcv_fetch_start_dt_utc.strftime('%Y-%m-%d %H:%M')} hasta {ohlcv_fetch_end_dt_utc.strftime('%Y-%m-%d %H:%M')}")

                ohlcv_df = fetch_ohlcv_data(conn, ticker_to_process_loop, tf_minutes,
                                            ohlcv_fetch_start_dt_utc, ohlcv_fetch_end_dt_utc)

                if ohlcv_df.empty or len(ohlcv_df) < MAX_LOOKBACK_PERIOD:
                    logger.warning(f"Datos OHLCV insuficientes ({len(ohlcv_df)}) para {ticker_to_process_loop} TF:{tf_minutes}m (Req. técnico: {MAX_LOOKBACK_PERIOD}). Saltando timeframe.")
                    continue

                indicators_df_full = calculate_indicators(ohlcv_df, ticker_to_process_loop, tf_minutes)

                # Asegurarse de que timestamp_col y timestamp_last estén presentes
                if 'timestamp_col' not in indicators_df_full.columns:
                    indicators_df_full['timestamp_col'] = ohlcv_df['timestamp_col']
                if 'timestamp_last' not in indicators_df_full.columns:
                    indicators_df_full['timestamp_last'] = ohlcv_df['timestamp_last']

                calculated_indicator_cols_present = [col for col in INDICATOR_COLUMN_NAMES if col in indicators_df_full.columns]
                if indicators_df_full.empty or not calculated_indicator_cols_present:
                    logger.warning(f"Cálculo de indicadores no produjo resultados para {ticker_to_process_loop} TF:{tf_minutes}m. Saltando almacenamiento.")
                    continue

                logger.info(f"Iniciando almacenamiento diario de indicadores para {ticker_to_process_loop} TF:{tf_minutes}m...")
                total_rows_affected_timeframe = 0
                current_day_to_process_utc = overall_storage_start_dt_utc
                while current_day_to_process_utc <= overall_storage_end_dt_inclusive_utc:
                    day_storage_start_dt_utc = current_day_to_process_utc
                    day_storage_end_dt_exclusive_utc = current_day_to_process_utc + timedelta(days=1)

                    rows_affected = store_indicators(conn, indicators_df_full, ticker_to_process_loop, tf_minutes,
                                                    day_storage_start_dt_utc, day_storage_end_dt_exclusive_utc)
                    total_rows_affected_timeframe += rows_affected

                    current_day_to_process_utc += timedelta(days=1)

                logger.info(f"Procesados {total_rows_affected_timeframe} registros de indicadores para {ticker_to_process_loop} TF:{tf_minutes}m desde {overall_storage_start_dt_utc.strftime('%Y-%m-%d')} hasta {overall_storage_end_dt_inclusive_utc.strftime('%Y-%m-%d')}.")

        logger.info("Procesamiento histórico de todos los tickers y timeframes completado.")

    except Exception as e:
        logger.error(f"Ocurrió un error general en main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a PostgreSQL cerrada.")

if __name__ == '__main__':
    main()

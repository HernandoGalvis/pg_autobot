# 02_pg_ohlcv_agg_5m_15m_60m_240m_1440m.py
# Consolida velas de 1 minuto (tabla ohlcv_raw_1m) en velas de 5, 15, 60, 240 y 1440 minutos.
# Guarda resultados en tabla ohlcv, usando el campo cantidad_velas y timestamp_last.
# yyyy, mm, dd, hh y timestamp_col se CALCULAN DIRECTAMENTE desde el timestamp de la primera vela de cada bloque.

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import pytz

# --- Parámetros de fechas de procesamiento ---
YEAR_START_DT = datetime(2023, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
YEAR_END_DT   = datetime(2023, 6, 1, 0, 0, 0, tzinfo=pytz.UTC)

# --- Parámetros de conexión (importados) ---
from parmspg import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

# --- Inicialización de conexión a PostgreSQL ---
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'UTC'")
    print("Conexión a PostgreSQL establecida y configurada en UTC.")

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
except psycopg2.Error as err:
    print(f"Error al conectar o configurar PostgreSQL: {err}")
    exit()

# Lista de columnas en el mismo orden que la tabla ohlcv (incluye timestamp_last)
ohlcv_cols = [
    'ticker', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close',
    'volume', 'is_closed', 'cantidad_velas', 'timestamp_col',
    'yyyy', 'mm', 'dd', 'hh', 'timestamp_last'
]

# --- Función para calcular OHLCV de una lista de velas ---
def calc_ohlcv(velas):
    opens = [v['open'] for v in velas]
    highs = [v['high'] for v in velas]
    lows  = [v['low'] for v in velas]
    closes= [v['close'] for v in velas]
    vols  = [v['volume'] for v in velas]
    return {
        'open': opens[0],
        'high': max(highs),
        'low': min(lows),
        'close': closes[-1],
        'volume': sum(vols)
    }

# --- Función para grabar una vela de cualquier timeframe ---
def grabar_vela(df, engine):
    try:
        df.to_sql(
            'ohlcv',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f"Grabadas {len(df)} velas en ohlcv")
    except Exception as err:
        print(f"Error al insertar velas en ohlcv: {err}")

# --- Timezone Bogotá ---
bogota_tz = pytz.timezone('America/Bogota')

# --- Procesamiento principal ---
try:
    cursor.execute("SELECT ticker FROM tickers WHERE activo = true")
    tickers = [row[0] for row in cursor.fetchall()]
    print(f"Tickers activos a procesar: {tickers}")

    for ticker in tickers:
        print(f"\n== Procesando ticker: {ticker} ==")
        fecha_actual = YEAR_START_DT

        while fecha_actual < YEAR_END_DT:
            fecha_siguiente = fecha_actual + timedelta(days=1)
            print(f"\n-- Día: {fecha_actual.strftime('%Y-%m-%d')} --")

            # Lectura de velas de 1 minuto para este ticker y día
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, timestamp_col, yyyy, mm, dd, hh
                FROM ohlcv_raw_1m
                WHERE ticker = %s
                  AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
            """, (ticker, fecha_actual, fecha_siguiente))
            rows = cursor.fetchall()

            if not rows:
                print(f"No hay velas de 1m para {ticker} en {fecha_actual.strftime('%Y-%m-%d')}")
                fecha_actual = fecha_siguiente
                continue

            # Inicialización de variables por timeframe
            # 15m
            c_15m = 0
            open_15m = None
            high_15m = None
            low_15m = None
            close_15m = None
            volume_15m = 0.0
            timestamp_15m = None

            # 60m
            c_60m = 0
            open_60m = None
            high_60m = None
            low_60m = None
            close_60m = None
            volume_60m = 0.0
            timestamp_60m = None

            # 240m
            c_240m = 0
            open_240m = None
            high_240m = None
            low_240m = None
            close_240m = None
            volume_240m = 0.0
            timestamp_240m = None

            # 1440m
            c_1440m = 0
            open_1440m = None
            high_1440m = None
            low_1440m = None
            close_1440m = None
            volume_1440m = 0.0
            timestamp_1440m = None

            # 5m (buffer)
            buffer_5m = []
            ts_first_5m = None
            c_5m = 0

            for i, row in enumerate(rows):
                vela_1m = {
                    'timestamp': row[0],
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': float(row[5])
                }

                # --- Actualizar variables para 15m ---
                if c_15m == 0:
                    open_15m = vela_1m['open']
                    high_15m = vela_1m['high']
                    low_15m = vela_1m['low']
                    volume_15m = vela_1m['volume']
                    timestamp_15m = vela_1m['timestamp']
                else:
                    if vela_1m['high'] > high_15m:
                        high_15m = vela_1m['high']
                    if vela_1m['low'] < low_15m:
                        low_15m = vela_1m['low']
                    volume_15m += vela_1m['volume']
                close_15m = vela_1m['close']
                c_15m += 1

                # --- Actualizar variables para 60m ---
                if c_60m == 0:
                    open_60m = vela_1m['open']
                    high_60m = vela_1m['high']
                    low_60m = vela_1m['low']
                    volume_60m = vela_1m['volume']
                    timestamp_60m = vela_1m['timestamp']
                else:
                    if vela_1m['high'] > high_60m:
                        high_60m = vela_1m['high']
                    if vela_1m['low'] < low_60m:
                        low_60m = vela_1m['low']
                    volume_60m += vela_1m['volume']
                close_60m = vela_1m['close']
                c_60m += 1

                # --- Actualizar variables para 240m ---
                if c_240m == 0:
                    open_240m = vela_1m['open']
                    high_240m = vela_1m['high']
                    low_240m = vela_1m['low']
                    volume_240m = vela_1m['volume']
                    timestamp_240m = vela_1m['timestamp']
                else:
                    if vela_1m['high'] > high_240m:
                        high_240m = vela_1m['high']
                    if vela_1m['low'] < low_240m:
                        low_240m = vela_1m['low']
                    volume_240m += vela_1m['volume']
                close_240m = vela_1m['close']
                c_240m += 1

                # --- Actualizar variables para 1440m ---
                if c_1440m == 0:
                    open_1440m = vela_1m['open']
                    high_1440m = vela_1m['high']
                    low_1440m = vela_1m['low']
                    volume_1440m = vela_1m['volume']
                    timestamp_1440m = vela_1m['timestamp']
                else:
                    if vela_1m['high'] > high_1440m:
                        high_1440m = vela_1m['high']
                    if vela_1m['low'] < low_1440m:
                        low_1440m = vela_1m['low']
                    volume_1440m += vela_1m['volume']
                close_1440m = vela_1m['close']
                c_1440m += 1

                # --- Procesamiento de 5m ---
                if not buffer_5m:
                    ts_first_5m = vela_1m['timestamp']
                buffer_5m.append(vela_1m)
                c_5m += 1

                if len(buffer_5m) == 5:
                    ohlcv_5m = calc_ohlcv(buffer_5m)
                    cantidad_velas_5m = 5
                    timestamp_last_5m = ts_first_5m + timedelta(minutes=cantidad_velas_5m-1)
                    yyyy_5m = ts_first_5m.year
                    mm_5m = ts_first_5m.month
                    dd_5m = ts_first_5m.day
                    hh_5m = ts_first_5m.hour
                    timestamp_col_5m = ts_first_5m.astimezone(bogota_tz)
                    vela_5m = {
                        'ticker': ticker,
                        'timeframe': 5,
                        'timestamp': ts_first_5m,
                        'open': ohlcv_5m['open'],
                        'high': ohlcv_5m['high'],
                        'low': ohlcv_5m['low'],
                        'close': ohlcv_5m['close'],
                        'volume': ohlcv_5m['volume'],
                        'is_closed': True,
                        'cantidad_velas': cantidad_velas_5m,
                        'timestamp_col': timestamp_col_5m,
                        'yyyy': yyyy_5m,
                        'mm': mm_5m,
                        'dd': dd_5m,
                        'hh': hh_5m,
                        'timestamp_last': timestamp_last_5m
                    }
                    df_5m = pd.DataFrame([vela_5m], columns=ohlcv_cols)
                    grabar_vela(df_5m, engine)
                    try:
                        conn.commit()
                        print(f"Commit realizado para {ticker} {fecha_actual.strftime('%Y-%m-%d')} (Vela de 5m)")
                    except Exception as err:
                        print(f"Error al hacer commit para {ticker} {fecha_actual.strftime('%Y-%m-%d')}: {err}")
                        conn.rollback()
                    buffer_5m = []
                    ts_first_5m = None
                    c_5m = 0

                    # --- Lógica de 15m: Vela parcial solo si c_15m < 15 ---
                    if c_15m < 15:
                        cantidad_velas_15m = c_15m
                        timestamp_last_15m = timestamp_15m + timedelta(minutes=cantidad_velas_15m-1)
                        yyyy_15m = timestamp_15m.year
                        mm_15m = timestamp_15m.month
                        dd_15m = timestamp_15m.day
                        hh_15m = timestamp_15m.hour
                        timestamp_col_15m = timestamp_15m.astimezone(bogota_tz)
                        vela_15m = {
                            'ticker': ticker,
                            'timeframe': 15,
                            'timestamp': timestamp_15m,
                            'open': open_15m,
                            'high': high_15m,
                            'low': low_15m,
                            'close': close_15m,
                            'volume': volume_15m,
                            'is_closed': False,
                            'cantidad_velas': cantidad_velas_15m,
                            'timestamp_col': timestamp_col_15m,
                            'yyyy': yyyy_15m,
                            'mm': mm_15m,
                            'dd': dd_15m,
                            'hh': hh_15m,
                            'timestamp_last': timestamp_last_15m
                        }
                        df_15m_parcial = pd.DataFrame([vela_15m], columns=ohlcv_cols)
                        grabar_vela(df_15m_parcial, engine)
                        try:
                            conn.commit()
                            print(f"Commit 15m parcial {cantidad_velas_15m}")
                        except Exception as err:
                            print(f"Error commit 15m parcial: {err}")
                            conn.rollback()

                    # --- Lógica de 60m: Vela parcial solo si c_60m < 60 ---
                    if c_60m < 60:
                        cantidad_velas_60m = c_60m
                        timestamp_last_60m = timestamp_60m + timedelta(minutes=cantidad_velas_60m-1)
                        yyyy_60m = timestamp_60m.year
                        mm_60m = timestamp_60m.month
                        dd_60m = timestamp_60m.day
                        hh_60m = timestamp_60m.hour
                        timestamp_col_60m = timestamp_60m.astimezone(bogota_tz)
                        vela_60m = {
                            'ticker': ticker,
                            'timeframe': 60,
                            'timestamp': timestamp_60m,
                            'open': open_60m,
                            'high': high_60m,
                            'low': low_60m,
                            'close': close_60m,
                            'volume': volume_60m,
                            'is_closed': False,
                            'cantidad_velas': cantidad_velas_60m,
                            'timestamp_col': timestamp_col_60m,
                            'yyyy': yyyy_60m,
                            'mm': mm_60m,
                            'dd': dd_60m,
                            'hh': hh_60m,
                            'timestamp_last': timestamp_last_60m
                        }
                        df_60m_parcial = pd.DataFrame([vela_60m], columns=ohlcv_cols)
                        grabar_vela(df_60m_parcial, engine)
                        try:
                            conn.commit()
                            print(f"Commit 60m parcial {cantidad_velas_60m}")
                        except Exception as err:
                            print(f"Error commit 60m parcial: {err}")
                            conn.rollback()

                    # --- Lógica de 240m: Vela parcial solo si c_240m < 240 ---
                    if c_240m < 240:
                        cantidad_velas_240m = c_240m
                        timestamp_last_240m = timestamp_240m + timedelta(minutes=cantidad_velas_240m-1)
                        yyyy_240m = timestamp_240m.year
                        mm_240m = timestamp_240m.month
                        dd_240m = timestamp_240m.day
                        hh_240m = timestamp_240m.hour
                        timestamp_col_240m = timestamp_240m.astimezone(bogota_tz)
                        vela_240m = {
                            'ticker': ticker,
                            'timeframe': 240,
                            'timestamp': timestamp_240m,
                            'open': open_240m,
                            'high': high_240m,
                            'low': low_240m,
                            'close': close_240m,
                            'volume': volume_240m,
                            'is_closed': False,
                            'cantidad_velas': cantidad_velas_240m,
                            'timestamp_col': timestamp_col_240m,
                            'yyyy': yyyy_240m,
                            'mm': mm_240m,
                            'dd': dd_240m,
                            'hh': hh_240m,
                            'timestamp_last': timestamp_last_240m
                        }
                        df_240m_parcial = pd.DataFrame([vela_240m], columns=ohlcv_cols)
                        grabar_vela(df_240m_parcial, engine)
                        try:
                            conn.commit()
                            print(f"Commit 240m parcial {cantidad_velas_240m}")
                        except Exception as err:
                            print(f"Error commit 240m parcial: {err}")
                            conn.rollback()

                    # --- Lógica de 1440m: Vela parcial solo si c_1440m < 1440 ---
                    if c_1440m < 1440:
                        cantidad_velas_1440m = c_1440m
                        timestamp_last_1440m = timestamp_1440m + timedelta(minutes=cantidad_velas_1440m-1)
                        yyyy_1440m = timestamp_1440m.year
                        mm_1440m = timestamp_1440m.month
                        dd_1440m = timestamp_1440m.day
                        hh_1440m = timestamp_1440m.hour
                        timestamp_col_1440m = timestamp_1440m.astimezone(bogota_tz)
                        vela_1440m = {
                            'ticker': ticker,
                            'timeframe': 1440,
                            'timestamp': timestamp_1440m,
                            'open': open_1440m,
                            'high': high_1440m,
                            'low': low_1440m,
                            'close': close_1440m,
                            'volume': volume_1440m,
                            'is_closed': False,
                            'cantidad_velas': cantidad_velas_1440m,
                            'timestamp_col': timestamp_col_1440m,
                            'yyyy': yyyy_1440m,
                            'mm': mm_1440m,
                            'dd': dd_1440m,
                            'hh': hh_1440m,
                            'timestamp_last': timestamp_last_1440m
                        }
                        df_1440m_parcial = pd.DataFrame([vela_1440m], columns=ohlcv_cols)
                        grabar_vela(df_1440m_parcial, engine)
                        try:
                            conn.commit()
                            print(f"Commit 1440m parcial {cantidad_velas_1440m}")
                        except Exception as err:
                            print(f"Error commit 1440m parcial: {err}")
                            conn.rollback()

                # --- Cierre de vela de 15m ---
                if c_15m == 15:
                    cantidad_velas_15m = 15
                    timestamp_last_15m = timestamp_15m + timedelta(minutes=cantidad_velas_15m-1)
                    yyyy_15m = timestamp_15m.year
                    mm_15m = timestamp_15m.month
                    dd_15m = timestamp_15m.day
                    hh_15m = timestamp_15m.hour
                    timestamp_col_15m = timestamp_15m.astimezone(bogota_tz)
                    vela_15m = {
                        'ticker': ticker,
                        'timeframe': 15,
                        'timestamp': timestamp_15m,
                        'open': open_15m,
                        'high': high_15m,
                        'low': low_15m,
                        'close': close_15m,
                        'volume': volume_15m,
                        'is_closed': True,
                        'cantidad_velas': cantidad_velas_15m,
                        'timestamp_col': timestamp_col_15m,
                        'yyyy': yyyy_15m,
                        'mm': mm_15m,
                        'dd': dd_15m,
                        'hh': hh_15m,
                        'timestamp_last': timestamp_last_15m
                    }
                    df_15m_closed = pd.DataFrame([vela_15m], columns=ohlcv_cols)
                    grabar_vela(df_15m_closed, engine)
                    try:
                        conn.commit()
                        print(f"Commit 15m cerrada")
                    except Exception as err:
                        print(f"Error commit 15m cerrada: {err}")
                        conn.rollback()
                    c_15m = 0
                    open_15m = None
                    high_15m = None
                    low_15m = None
                    close_15m = None
                    volume_15m = 0.0
                    timestamp_15m = None

                # --- Cierre de vela de 60m ---
                if c_60m == 60:
                    cantidad_velas_60m = 60
                    timestamp_last_60m = timestamp_60m + timedelta(minutes=cantidad_velas_60m-1)
                    yyyy_60m = timestamp_60m.year
                    mm_60m = timestamp_60m.month
                    dd_60m = timestamp_60m.day
                    hh_60m = timestamp_60m.hour
                    timestamp_col_60m = timestamp_60m.astimezone(bogota_tz)
                    vela_60m = {
                        'ticker': ticker,
                        'timeframe': 60,
                        'timestamp': timestamp_60m,
                        'open': open_60m,
                        'high': high_60m,
                        'low': low_60m,
                        'close': close_60m,
                        'volume': volume_60m,
                        'is_closed': True,
                        'cantidad_velas': cantidad_velas_60m,
                        'timestamp_col': timestamp_col_60m,
                        'yyyy': yyyy_60m,
                        'mm': mm_60m,
                        'dd': dd_60m,
                        'hh': hh_60m,
                        'timestamp_last': timestamp_last_60m
                    }
                    df_60m_closed = pd.DataFrame([vela_60m], columns=ohlcv_cols)
                    grabar_vela(df_60m_closed, engine)
                    try:
                        conn.commit()
                        print(f"Commit 60m cerrada")
                    except Exception as err:
                        print(f"Error commit 60m cerrada: {err}")
                        conn.rollback()
                    c_60m = 0
                    open_60m = None
                    high_60m = None
                    low_60m = None
                    close_60m = None
                    volume_60m = 0.0
                    timestamp_60m = None

                # --- Cierre de vela de 240m ---
                if c_240m == 240:
                    cantidad_velas_240m = 240
                    timestamp_last_240m = timestamp_240m + timedelta(minutes=cantidad_velas_240m-1)
                    yyyy_240m = timestamp_240m.year
                    mm_240m = timestamp_240m.month
                    dd_240m = timestamp_240m.day
                    hh_240m = timestamp_240m.hour
                    timestamp_col_240m = timestamp_240m.astimezone(bogota_tz)
                    vela_240m = {
                        'ticker': ticker,
                        'timeframe': 240,
                        'timestamp': timestamp_240m,
                        'open': open_240m,
                        'high': high_240m,
                        'low': low_240m,
                        'close': close_240m,
                        'volume': volume_240m,
                        'is_closed': True,
                        'cantidad_velas': cantidad_velas_240m,
                        'timestamp_col': timestamp_col_240m,
                        'yyyy': yyyy_240m,
                        'mm': mm_240m,
                        'dd': dd_240m,
                        'hh': hh_240m,
                        'timestamp_last': timestamp_last_240m
                    }
                    df_240m_closed = pd.DataFrame([vela_240m], columns=ohlcv_cols)
                    grabar_vela(df_240m_closed, engine)
                    try:
                        conn.commit()
                        print(f"Commit 240m cerrada")
                    except Exception as err:
                        print(f"Error commit 240m cerrada: {err}")
                        conn.rollback()
                    c_240m = 0
                    open_240m = None
                    high_240m = None
                    low_240m = None
                    close_240m = None
                    volume_240m = 0.0
                    timestamp_240m = None

                # --- Cierre de vela de 1440m ---
                if c_1440m == 1440:
                    cantidad_velas_1440m = 1440
                    timestamp_last_1440m = timestamp_1440m + timedelta(minutes=cantidad_velas_1440m-1)
                    yyyy_1440m = timestamp_1440m.year
                    mm_1440m = timestamp_1440m.month
                    dd_1440m = timestamp_1440m.day
                    hh_1440m = timestamp_1440m.hour
                    timestamp_col_1440m = timestamp_1440m.astimezone(bogota_tz)
                    vela_1440m = {
                        'ticker': ticker,
                        'timeframe': 1440,
                        'timestamp': timestamp_1440m,
                        'open': open_1440m,
                        'high': high_1440m,
                        'low': low_1440m,
                        'close': close_1440m,
                        'volume': volume_1440m,
                        'is_closed': True,
                        'cantidad_velas': cantidad_velas_1440m,
                        'timestamp_col': timestamp_col_1440m,
                        'yyyy': yyyy_1440m,
                        'mm': mm_1440m,
                        'dd': dd_1440m,
                        'hh': hh_1440m,
                        'timestamp_last': timestamp_last_1440m
                    }
                    df_1440m_closed = pd.DataFrame([vela_1440m], columns=ohlcv_cols)
                    grabar_vela(df_1440m_closed, engine)
                    try:
                        conn.commit()
                        print(f"Commit 1440m cerrada")
                    except Exception as err:
                        print(f"Error commit 1440m cerrada: {err}")
                        conn.rollback()
                    c_1440m = 0
                    open_1440m = None
                    high_1440m = None
                    low_1440m = None
                    close_1440m = None
                    volume_1440m = 0.0
                    timestamp_1440m = None

            # --- Final del día: velas parciales ---
            if buffer_5m:
                cantidad_velas_5m = len(buffer_5m)
                timestamp_last_5m = ts_first_5m + timedelta(minutes=cantidad_velas_5m-1)
                yyyy_5m = ts_first_5m.year
                mm_5m = ts_first_5m.month
                dd_5m = ts_first_5m.day
                hh_5m = ts_first_5m.hour
                timestamp_col_5m = ts_first_5m.astimezone(bogota_tz)
                ohlcv_5m = calc_ohlcv(buffer_5m)
                vela_5m = {
                    'ticker': ticker,
                    'timeframe': 5,
                    'timestamp': ts_first_5m,
                    'open': ohlcv_5m['open'],
                    'high': ohlcv_5m['high'],
                    'low': ohlcv_5m['low'],
                    'close': ohlcv_5m['close'],
                    'volume': ohlcv_5m['volume'],
                    'is_closed': False,
                    'cantidad_velas': cantidad_velas_5m,
                    'timestamp_col': timestamp_col_5m,
                    'yyyy': yyyy_5m,
                    'mm': mm_5m,
                    'dd': dd_5m,
                    'hh': hh_5m,
                    'timestamp_last': timestamp_last_5m
                }
                df_5m_parcial = pd.DataFrame([vela_5m], columns=ohlcv_cols)
                grabar_vela(df_5m_parcial, engine)
                try:
                    conn.commit()
                    print(f"Commit 5m parcial fin de día")
                except Exception as err:
                    print(f"Error commit 5m parcial fin de día: {err}")
                    conn.rollback()
            if c_15m > 0 and c_15m < 15:
                cantidad_velas_15m = c_15m
                timestamp_last_15m = timestamp_15m + timedelta(minutes=cantidad_velas_15m-1)
                yyyy_15m = timestamp_15m.year
                mm_15m = timestamp_15m.month
                dd_15m = timestamp_15m.day
                hh_15m = timestamp_15m.hour
                timestamp_col_15m = timestamp_15m.astimezone(bogota_tz)
                vela_15m = {
                    'ticker': ticker,
                    'timeframe': 15,
                    'timestamp': timestamp_15m,
                    'open': open_15m,
                    'high': high_15m,
                    'low': low_15m,
                    'close': close_15m,
                    'volume': volume_15m,
                    'is_closed': False,
                    'cantidad_velas': cantidad_velas_15m,
                    'timestamp_col': timestamp_col_15m,
                    'yyyy': yyyy_15m,
                    'mm': mm_15m,
                    'dd': dd_15m,
                    'hh': hh_15m,
                    'timestamp_last': timestamp_last_15m
                }
                df_15m_parcial_final = pd.DataFrame([vela_15m], columns=ohlcv_cols)
                grabar_vela(df_15m_parcial_final, engine)
                try:
                    conn.commit()
                    print(f"Commit 15m parcial fin de día")
                except Exception as err:
                    print(f"Error commit 15m parcial fin de día: {err}")
                    conn.rollback()
            if c_60m > 0 and c_60m < 60:
                cantidad_velas_60m = c_60m
                timestamp_last_60m = timestamp_60m + timedelta(minutes=cantidad_velas_60m-1)
                yyyy_60m = timestamp_60m.year
                mm_60m = timestamp_60m.month
                dd_60m = timestamp_60m.day
                hh_60m = timestamp_60m.hour
                timestamp_col_60m = timestamp_60m.astimezone(bogota_tz)
                vela_60m = {
                    'ticker': ticker,
                    'timeframe': 60,
                    'timestamp': timestamp_60m,
                    'open': open_60m,
                    'high': high_60m,
                    'low': low_60m,
                    'close': close_60m,
                    'volume': volume_60m,
                    'is_closed': False,
                    'cantidad_velas': cantidad_velas_60m,
                    'timestamp_col': timestamp_col_60m,
                    'yyyy': yyyy_60m,
                    'mm': mm_60m,
                    'dd': dd_60m,
                    'hh': hh_60m,
                    'timestamp_last': timestamp_last_60m
                }
                df_60m_parcial_final = pd.DataFrame([vela_60m], columns=ohlcv_cols)
                grabar_vela(df_60m_parcial_final, engine)
                try:
                    conn.commit()
                    print(f"Commit 60m parcial fin de día")
                except Exception as err:
                    print(f"Error commit 60m parcial fin de día: {err}")
                    conn.rollback()
            if c_240m > 0 and c_240m < 240:
                cantidad_velas_240m = c_240m
                timestamp_last_240m = timestamp_240m + timedelta(minutes=cantidad_velas_240m-1)
                yyyy_240m = timestamp_240m.year
                mm_240m = timestamp_240m.month
                dd_240m = timestamp_240m.day
                hh_240m = timestamp_240m.hour
                timestamp_col_240m = timestamp_240m.astimezone(bogota_tz)
                vela_240m = {
                    'ticker': ticker,
                    'timeframe': 240,
                    'timestamp': timestamp_240m,
                    'open': open_240m,
                    'high': high_240m,
                    'low': low_240m,
                    'close': close_240m,
                    'volume': volume_240m,
                    'is_closed': False,
                    'cantidad_velas': cantidad_velas_240m,
                    'timestamp_col': timestamp_col_240m,
                    'yyyy': yyyy_240m,
                    'mm': mm_240m,
                    'dd': dd_240m,
                    'hh': hh_240m,
                    'timestamp_last': timestamp_last_240m
                }
                df_240m_parcial_final = pd.DataFrame([vela_240m], columns=ohlcv_cols)
                grabar_vela(df_240m_parcial_final, engine)
                try:
                    conn.commit()
                    print(f"Commit 240m parcial fin de día")
                except Exception as err:
                    print(f"Error commit 240m parcial fin de día: {err}")
                    conn.rollback()
            if c_1440m > 0 and c_1440m < 1440:
                cantidad_velas_1440m = c_1440m
                timestamp_last_1440m = timestamp_1440m + timedelta(minutes=cantidad_velas_1440m-1)
                yyyy_1440m = timestamp_1440m.year
                mm_1440m = timestamp_1440m.month
                dd_1440m = timestamp_1440m.day
                hh_1440m = timestamp_1440m.hour
                timestamp_col_1440m = timestamp_1440m.astimezone(bogota_tz)
                vela_1440m = {
                    'ticker': ticker,
                    'timeframe': 1440,
                    'timestamp': timestamp_1440m,
                    'open': open_1440m,
                    'high': high_1440m,
                    'low': low_1440m,
                    'close': close_1440m,
                    'volume': volume_1440m,
                    'is_closed': False,
                    'cantidad_velas': cantidad_velas_1440m,
                    'timestamp_col': timestamp_col_1440m,
                    'yyyy': yyyy_1440m,
                    'mm': mm_1440m,
                    'dd': dd_1440m,
                    'hh': hh_1440m,
                    'timestamp_last': timestamp_last_1440m
                }
                df_1440m_parcial_final = pd.DataFrame([vela_1440m], columns=ohlcv_cols)
                grabar_vela(df_1440m_parcial_final, engine)
                try:
                    conn.commit()
                    print(f"Commit 1440m parcial fin de día")
                except Exception as err:
                    print(f"Error commit 1440m parcial fin de día: {err}")
                    conn.rollback()

            fecha_actual = fecha_siguiente

except Exception as e:
    print(f"Error inesperado: {e}")
finally:
    if 'cursor' in locals():
        try:
            cursor.close()
            print("Cursor de PostgreSQL cerrado.")
        except:
            pass
    if 'conn' in locals():
        try:
            conn.close()
            print("Conexión a PostgreSQL cerrada.")
        except:
            pass
    if 'engine' in locals():
        try:
            engine.dispose()
            print("Motor SQLAlchemy cerrado.")
        except:
            pass

# ----------------------------------------------------------------------
# FIN DEL SCRIPT - Consolidador OHLCV 5, 15, 60, 240 y 1440 minutos (solo append, con timestamp_last)
# ----------------------------------------------------------------------

# pg_candles_import_x_paquetes.py
# PERMITE TRAER DATOS DE LAS VELAS EN UN RANGO DE TIEMPO DETERMINADO
# VERSIÓN MODIFICADA PARA OBTENER DATOS DE FUTUROS (SWAPS PERPETUOS USDT)
# ADAPTADO PARA POSTGRESQL CON PSYCOPG2, SQLALCHEMY Y PANDAS

import time
import ccxt
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import pytz
import math

# Asume que parmspg.py contiene las credenciales
from parmspg import API_KEY, API_SECRET, API_PASSPHRASE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

# Timeframes a procesar (en minutos)
TIMEFRAME_STR_TO_MIN = {
    "1d": 1440,
    "1D": 1440,
    "4h": 240,
    "4H": 240,
    "1h": 60,
    "1H": 60,
    "15m": 15,
    "15M": 15,
    "5m": 5,
    "5M": 5
}
timeframes_min = [5, 15, 60, 240, 1440]  # En minutos
limit_por_fetch = 200  # Límite de velas por petición API
DAYS_PER_BATCH = 3  # Número de días a procesar y guardar en cada lote

# --- Inicialización del Exchange ---
try:
    exchange = ccxt.bitget({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'password': API_PASSPHRASE,
        'enableRateLimit': True,
        'options': {
            'defaultTimeInForce': 'GTC',
            'adjustForTimeZone': False,
        }
    })
    exchange.load_markets()
    print("Conexión con CCXT (Bitget) establecida y mercados cargados.")
except Exception as e:
    print(f"Error al inicializar CCXT con Bitget: {e}")
    exit()

# --- Conexión a PostgreSQL ---
try:
    print("Credenciales de conexión:")
    print(f"DB_HOST: {DB_HOST}")
    print(f"DB_PORT: {DB_PORT}")
    print(f"DB_USER: {DB_USER}")
    print(f"DB_PASSWORD: {DB_PASSWORD}")
    print(f"DB_NAME: {DB_NAME}")
    
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
except UnicodeDecodeError as ude:
    print(f"Error de codificación en las credenciales: {ude}")
    exit()

# --- Función fetch_ohlcv (sin cambios) ---
def fetch_ohlcv(symbol_for_api, timeframe_str, since_ts, until_ts):
    """Obtiene datos OHLCV usando CCXT, especificando tipo 'swap' para futuros."""
    all_data = []
    ts = since_ts
    params = {'type': 'swap'}
    print(f"   [API Call] Fetching {symbol_for_api} {timeframe_str} since {datetime.fromtimestamp(ts/1000, tz=pytz.UTC)} with params={params}")

    while ts < until_ts:
        try:
            bars = exchange.fetch_ohlcv(symbol_for_api, timeframe_str, since=ts, limit=limit_por_fetch, params=params)
        except ccxt.RateLimitExceeded as e:
            print(f"   [{symbol_for_api} - {timeframe_str}] Límite de tasa excedido, esperando 60s... {e}")
            time.sleep(60)
            continue
        except ccxt.NetworkError as e:
            print(f"   [{symbol_for_api} - {timeframe_str}] Error de red: {e}. Reintentando en 10s...")
            time.sleep(10)
            continue
        except ccxt.ExchangeError as e:
            print(f"   [{symbol_for_api} - {timeframe_str}] Error del exchange: {e}. Podría ser símbolo inválido para 'swap' o problema temporal. Saltando fetch para este rango.")
            break
        except Exception as e:
            print(f"   [{symbol_for_api} - {timeframe_str}] Error inesperado al traer datos: {e}. Reintentando en 5s...")
            time.sleep(5)
            continue

        if not bars:
            break

        newest_ts_in_batch = bars[-1][0]
        added_count = 0
        for bar in bars:
            if bar[0] >= until_ts:
                continue
            if bar[0] >= ts:
                all_data.append(bar)
                added_count += 1

        if added_count == 0 or newest_ts_in_batch < ts:
            break

        ts = newest_ts_in_batch + 1
        time.sleep(exchange.rateLimit / 1000)

    return all_data

# --- Función store_ohlcv ---
def store_ohlcv(symbol_in_db, timeframe_str, bars, start_dt_req, end_dt_req):
    """Almacena las barras OHLCV en la BD usando el ticker base (ej. 'BTC/USDT'). Convierte timeframe a número antes de grabar."""
    if not bars:
        return 0

    # Aseguramos conversión correcta de timeframe a número
    tf_str_lower = timeframe_str.lower()
    if tf_str_lower not in TIMEFRAME_STR_TO_MIN:
        print(f"   [store_ohlcv] ERROR: timeframe_str '{timeframe_str}' no reconocido para conversión a minutos.")
        return -1
    timeframe_min = TIMEFRAME_STR_TO_MIN[tf_str_lower]

    records = []
    start_ts_req = int(start_dt_req.timestamp() * 1000)
    end_ts_req = int(end_dt_req.timestamp() * 1000)

    for bar_data in bars:
        ts, o, h, l, c, v = bar_data
        start_ts_candle = ts

        if start_ts_candle < start_ts_req or start_ts_candle >= end_ts_req:
            continue

        dt_candle = datetime.fromtimestamp(start_ts_candle / 1000, tz=pytz.UTC)

        o = o if o is not None else 0.0
        h = h if h is not None else 0.0
        l = l if l is not None else 0.0
        c = c if c is not None else 0.0
        v = v if v is not None else 0.0

        records.append({
            'ticker': symbol_in_db,
            'timeframe': timeframe_min,
            'timestamp': dt_candle,
            'open': o,
            'high': h,
            'low': l,
            'close': c,
            'volume': v
        })

    if not records:
        return 0

    df = pd.DataFrame(records)

    try:
        df.to_sql(
            'ohlcv',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            schema=None
        )
        inserted_count = len(df)
        return inserted_count
    except Exception as err:
        print(f"   [{symbol_in_db} - {timeframe_str}] Error al insertar en BD ohlcv: {err}")
        print("   Primer registro (si existe):", records[0] if records else "N/A")
        conn.rollback()
        return -1

# --- Función procesar_rango ---
def procesar_rango(symbol_in_db, timeframe_min, start_dt, end_dt):
    """Procesa un rango de fechas para un ticker y timeframe, obteniendo datos de futuros."""
    start_dt = start_dt.replace(tzinfo=pytz.UTC)
    end_dt = end_dt.replace(tzinfo=pytz.UTC)

    timeframe_map = {1: '1m', 5: '5m', 15: '15m', 60: '1h', 240: '4h', 1440: '1d'}
    if timeframe_min not in timeframe_map:
        print(f"Timeframe en minutos {timeframe_min} no soportado en el mapeo.")
        return
    timeframe_str = timeframe_map[timeframe_min]

    parts = symbol_in_db.split('/')
    if len(parts) == 2:
        base = parts[0]
        quote = parts[1]
        symbol_for_api = f"{base}/{quote}:{quote}"
    else:
        print(f"   Error: Formato de ticker inesperado en la BD: {symbol_in_db}. No se puede construir símbolo API.")
        return

    print(f"🔹 {symbol_in_db} ({symbol_for_api}) - {timeframe_str} ({timeframe_min}m): {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%Y-%m-%d %H:%M')}")

    since_ts = int(start_dt.timestamp() * 1000)
    until_ts = int(end_dt.timestamp() * 1000)

    bars = fetch_ohlcv(symbol_for_api, timeframe_str, since_ts, until_ts)

    if not bars:
        print(f"   ⚠️ Sin datos obtenidos para {symbol_in_db} ({symbol_for_api}) - {timeframe_str} en este rango.")
        return

    total_inserted = store_ohlcv(symbol_in_db, timeframe_str, bars, start_dt, end_dt)

    if total_inserted == -1:
        print(f"   ❌ Error al guardar velas para {symbol_in_db} - {timeframe_str}.")
    elif total_inserted == 0 and bars:
        print(f"   ℹ️ No se insertaron velas nuevas (posiblemente ya existían) para {symbol_in_db} - {timeframe_str}.")
    elif total_inserted > 0:
        print(f"   ✅ {total_inserted} registros afectados en BD para {symbol_in_db} - {timeframe_str}.")
    else:
        print(f"   ? Situación inesperada para {symbol_in_db} - {timeframe_str}.")

# --- Función main ---
def main():
    global conn, cursor, engine
    try:
        cursor.execute("SELECT ticker FROM tickers WHERE activo = true")
        symbols_in_db = [row[0] for row in cursor.fetchall()]
        if not symbols_in_db:
            print("No hay tickers activos encontrados en la tabla 'tickers'. Finalizando.")
            return
        print(f"Tickers base a procesar: {symbols_in_db}")

        year_start_dt = datetime(2023, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
        year_end_dt = datetime(2023, 6, 1, 0, 0, 0, tzinfo=pytz.UTC)

        total_days = (year_end_dt - year_start_dt).days
        if total_days <= 0:
            print("Error: La fecha de fin debe ser posterior a la fecha de inicio.")
            return
        total_batches = math.ceil(total_days / DAYS_PER_BATCH)
        print(f"Procesando rango de fechas: {year_start_dt.strftime('%Y-%m-%d')} a {(year_end_dt - timedelta(days=1)).strftime('%Y-%m-%d')}")
        print(f"Total días: {total_days}, Lotes de {DAYS_PER_BATCH} días, Total Lotes: {total_batches}.")

        current_batch_start_dt = year_start_dt
        batch_num = 0

        while current_batch_start_dt < year_end_dt:
            batch_num += 1
            current_batch_end_dt = current_batch_start_dt + timedelta(days=DAYS_PER_BATCH)
            if current_batch_end_dt > year_end_dt:
                current_batch_end_dt = year_end_dt

            print(f"\n--- Procesando Lote {batch_num}/{total_batches} ({current_batch_start_dt.strftime('%Y-%m-%d')} a {(current_batch_end_dt - timedelta(microseconds=1)).strftime('%Y-%m-%d %H:%M')}) ---")

            for symbol_db in symbols_in_db:
                print(f"\n Procesando Símbolo DB: {symbol_db}")
                for tf_min in timeframes_min:
                    try:
                        procesar_rango(symbol_db, tf_min, current_batch_start_dt, current_batch_end_dt)
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"!! Error INESPERADO procesando {symbol_db} {tf_min}m en lote {batch_num}: {e}")
                        import traceback
                        traceback.print_exc()

            try:
                print(f"\n--- Fin Lote {batch_num}/{total_batches}. Realizando COMMIT. ---")
                conn.commit()
            except psycopg2.Error as err:
                print(f"!! Error al realizar COMMIT para el lote {batch_num}: {err}")
                print("Intentando rollback...")
                conn.rollback()

            current_batch_start_dt = current_batch_end_dt
            time.sleep(2)

        print(f"\nProceso completado para el rango de fechas definido.")

    except psycopg2.Error as err:
        print(f"Error de PostgreSQL durante la ejecución principal: {err}")
    except ccxt.AuthenticationError as e:
        print(f"Error de autenticación con Bitget: {e}. Verifica tus credenciales API en parmspg.py")
    except ccxt.ExchangeNotAvailable as e:
        print(f"Error: Exchange Bitget no disponible o problemas de conexión: {e}")
    except Exception as e:
        print(f"Error inesperado en main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if locals().get('cursor'):
            try:
                cursor.close()
                print("Cursor de PostgreSQL cerrado.")
            except:
                pass
        if locals().get('conn') and conn:
            try:
                conn.close()
                print("Conexión a PostgreSQL cerrada.")
            except:
                pass
        if locals().get('engine'):
            try:
                engine.dispose()
                print("Motor SQLAlchemy cerrado.")
            except:
                pass

if __name__ == '__main__':
    main()
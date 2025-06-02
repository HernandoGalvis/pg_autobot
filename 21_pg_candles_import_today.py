# ==============================================================================
# 21_pg_candles_import_today.py
# ------------------------------------------------------------------------------
# Obtiene e inserta velas OHLCV de 1 minuto (candles) para el día actual (UTC)
# desde Bitget usando CCXT y las almacena incrementalmente en PostgreSQL.
# Si no existen registros para el ticker hoy, inicia desde las 00:00:00 UTC.
# Si existen, continúa desde el último timestamp registrado +1 minuto.
# Se puede ejecutar en paralelo con el proceso histórico.
#
# Versión: 1.1.1
# Autores: HernandoGalvis + Copilot
# Fecha: 2025-06-02
# Corrección: Normalización robusta de tipos datetime para evitar errores naive/aware
# ==============================================================================

import time
import ccxt
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz

# =========================
#  CONFIGURACIÓN GENERAL
# =========================
VERSION = "1.1.1"

from parmspg import API_KEY, API_SECRET, API_PASSPHRASE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

LIMIT_PER_FETCH = 200
RATE_LIMIT_SECONDS = 1.2

TIMEFRAME_STR = '1m'
TIMEFRAME_MIN = 1

# =========================
# CONEXIÓN EXCHANGE (CCXT)
# =========================
try:
    exchange = ccxt.bitget({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'password': API_PASSPHRASE,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',
            'adjustForTimeZone': False,
        }
    })
    exchange.load_markets()
    print("Conexión con CCXT (Bitget) establecida y mercados cargados.")
except Exception as e:
    print(f"Error al inicializar CCXT con Bitget: {e}")
    exit()

# =========================
# CONEXIÓN POSTGRESQL
# =========================
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
except UnicodeDecodeError as ude:
    print(f"Error de codificación en las credenciales: {ude}")
    exit()

# =========================
# FUNCIONES AUXILIARES
# =========================

def get_today_stats(ticker):
    """Devuelve (num_registros_hoy, max_timestamp_hoy) de la vista para el ticker."""
    cursor.execute(
        "SELECT num_registros_hoy, max_timestamp_hoy FROM ohlcv_raw_1m_today_stats WHERE ticker = %s",
        (ticker,)
    )
    res = cursor.fetchone()
    if res:
        return int(res[0]), res[1]
    return 0, None

def fetch_ohlcv_1m(symbol_for_api, since_dt, until_dt):
    """Descarga las velas de 1m desde Bitget, entre since_dt (incluido) y until_dt (incluido)."""
    all_data = []
    ts = int(since_dt.timestamp() * 1000)
    until_ts = int(until_dt.timestamp() * 1000)
    params = {'type': 'swap'}
    while ts <= until_ts:
        try:
            bars = exchange.fetch_ohlcv(symbol_for_api, TIMEFRAME_STR, since=ts, limit=LIMIT_PER_FETCH, params=params)
        except ccxt.RateLimitExceeded as e:
            print(f"[{symbol_for_api}] Límite de tasa excedido, esperando 60s... {e}")
            time.sleep(60)
            continue
        except ccxt.NetworkError as e:
            print(f"[{symbol_for_api}] Error de red: {e}. Reintentando en 10s...")
            time.sleep(10)
            continue
        except ccxt.ExchangeError as e:
            print(f"[{symbol_for_api}] Error del exchange: {e}. Saltando fetch para este rango.")
            break
        except Exception as e:
            print(f"[{symbol_for_api}] Error inesperado al traer datos: {e}. Reintentando en 5s...")
            time.sleep(5)
            continue

        if not bars:
            break

        newest_ts_in_batch = bars[-1][0]
        added_count = 0
        for bar in bars:
            ts_api = bar[0]
            o, h, l, c, v = bar[1:]
            time_utc_w = datetime.fromtimestamp(ts_api / 1000, tz=pytz.UTC)
            time_bogota = time_utc_w.astimezone(pytz.timezone('America/Bogota'))

            # Solo aceptar datos dentro del rango actual de hoy (UTC)
            if ts_api < int(since_dt.timestamp() * 1000) or ts_api > until_ts:
                continue

            bar_struct = {
                'ticker': None,  # Se llena después
                'timestamp': time_utc_w.replace(tzinfo=None),   # Guardar como naive UTC
                'timestamp_col': time_bogota.replace(tzinfo=None), # naive Bogotá (acorde con tipo en BD)
                'yyyy': time_utc_w.year,
                'mm': time_utc_w.month,
                'dd': time_utc_w.day,
                'hh': time_utc_w.hour,
                'open': float(o) if o is not None else 0.0,
                'high': float(h) if h is not None else 0.0,
                'low': float(l) if l is not None else 0.0,
                'close': float(c) if c is not None else 0.0,
                'volume': float(v) if v is not None else 0.0,
                'ts_api': ts_api  # solo para debug opcional
            }
            all_data.append(bar_struct)
            added_count += 1

        if added_count == 0 or newest_ts_in_batch < ts:
            break

        ts = newest_ts_in_batch + 1
        time.sleep(RATE_LIMIT_SECONDS)

    return all_data

def store_ohlcv_1m(symbol_in_db, bars):
    """Guarda los registros de velas en ohlcv_raw_1m. Ignora duplicados."""
    if not bars:
        return 0

    records = []
    for bar_struct in bars:
        rec = dict(bar_struct)
        rec['ticker'] = symbol_in_db
        records.append(rec)

    if not records:
        return 0

    df = pd.DataFrame(records)

    # SOLO columnas válidas para la tabla (quita ts_api y cualquier otra)
    cols_validos = ['ticker', 'timestamp', 'timestamp_col', 'yyyy', 'mm', 'dd', 'hh', 'open', 'high', 'low', 'close', 'volume']
    df = df[cols_validos]

    try:
        # Usar método 'multi' para eficiencia
        # Usar parámetro 'if_exists=append' y dejar que la restricción UNIQUE evite duplicados
        df.to_sql(
            'ohlcv_raw_1m',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            schema=None,
            chunksize=1000
        )
        inserted_count = len(df)
        return inserted_count
    except Exception as err:
        # Si ocurre error de duplicado, continuar (la restricción UNIQUE en BD lo previene)
        if 'duplicate key value violates unique constraint' in str(err):
            print(f"⚠️  Duplicados detectados para {symbol_in_db}, algunos registros ya existen (esto es esperado si hay concurrencia).")
            conn.rollback()
            # Puedes refinar para contar cuántos sí se insertaron
            return 0
        print(f"[{symbol_in_db}] Error al insertar en BD ohlcv_raw_1m: {err}")
        print("Primer registro (si existe):", records[0] if records else "N/A")
        conn.rollback()
        return -1

# =========================
# PROCESO PRINCIPAL (HOY)
# =========================

def procesar_incremental_today(symbol_in_db):
    """Procesa el ticker para traer e insertar solo los datos nuevos de hoy."""
    parts = symbol_in_db.split('/')
    if len(parts) == 2:
        base = parts[0]
        quote = parts[1]
        symbol_for_api = f"{base}/{quote}:{quote}"
    else:
        print(f"Error: Formato de ticker inesperado en la BD: {symbol_in_db}. No se puede construir símbolo API.")
        return

    # Inicio del día actual en UTC (aware)
    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    # Buscar stats de hoy en la vista
    num_registros, max_timestamp = get_today_stats(symbol_in_db)

    # Normalización robusta: max_timestamp siempre aware UTC
    if max_timestamp is not None:
        if max_timestamp.tzinfo is None:
            max_timestamp = max_timestamp.replace(tzinfo=timezone.utc)
        else:
            max_timestamp = max_timestamp.astimezone(timezone.utc)

    if num_registros == 0 or max_timestamp is None:
        since_dt = today_utc
    else:
        since_dt = max_timestamp + timedelta(minutes=1)

    until_dt = now_utc - timedelta(minutes=1)  # Última vela cerrada

    # Normalización: ambos aware UTC
    if since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=timezone.utc)
    if until_dt.tzinfo is None:
        until_dt = until_dt.replace(tzinfo=timezone.utc)

    if since_dt > until_dt:
        print(f"   ℹ️ No hay nuevos datos para {symbol_in_db}: since_dt={since_dt}, until_dt={until_dt}")
        return

    print(f"🔹 {symbol_in_db} ({symbol_for_api}) - Fetch: {since_dt.strftime('%Y-%m-%d %H:%M')} → {until_dt.strftime('%Y-%m-%d %H:%M')}")
    bars = fetch_ohlcv_1m(symbol_for_api, since_dt, until_dt)
    if not bars:
        print(f"   ⚠️ Sin datos nuevos obtenidos para {symbol_in_db} en este rango.")
        return

    total_inserted = store_ohlcv_1m(symbol_in_db, bars)
    if total_inserted == -1:
        print(f"   ❌ Error al guardar velas para {symbol_in_db}.")
    elif total_inserted == 0 and bars:
        print(f"   ℹ️ No se insertaron velas nuevas (posiblemente ya existían) para {symbol_in_db}.")
    elif total_inserted > 0:
        print(f"   ✅ {total_inserted} registros insertados en BD para {symbol_in_db}.")
    else:
        print(f"   ? Situación inesperada para {symbol_in_db}.")

# =========================
# MAIN
# =========================

def main():
    global conn, cursor, engine
    print("=====================================")
    print(" 21_pg_candles_import_today.py")
    print(f" Versión: {VERSION}")
    print("=====================================")
    try:
        cursor.execute("SELECT ticker FROM tickers WHERE activo = true")
        symbols_in_db = [row[0] for row in cursor.fetchall()]
        if not symbols_in_db:
            print("No hay tickers activos encontrados en la tabla 'tickers'. Finalizando.")
            return

        print(f"Tickers activos a procesar hoy: {symbols_in_db}")
        for symbol_db in symbols_in_db:
            try:
                procesar_incremental_today(symbol_db)
                time.sleep(0.5)
            except Exception as e:
                print(f"!! Error INESPERADO procesando {symbol_db}: {e}")
                import traceback
                traceback.print_exc()
        try:
            print(f"\n--- Fin de ciclo. Realizando COMMIT. ---")
            conn.commit()
        except psycopg2.Error as err:
            print(f"!! Error al realizar COMMIT: {err}")
            print("Intentando rollback...")
            conn.rollback()

        print(f"\nProceso incremental del día actual completado.")

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

# ==============================================================================
# 21_pg_candles_import_today.py - FIN DEL SCRIPT
# ==============================================================================

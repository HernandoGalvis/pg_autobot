# -----------------------------------------------------------------------------
# Nombre Script: generador_resumen_Alertas_historico_pg.py
# Versión Modificada: 2025-06-08_utc (Campos yyyy, mm, dd, hh agregados)
# OBJETIVO: Lee alertas individuales de alertas_generadas, calcula resúmenes
#           por timestamp para un rango de fechas y actualiza/inserta en
#           resumen_alertas, incluyendo campos de año, mes, día y hora.
# -----------------------------------------------------------------------------

import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
import numpy as np
import pytz
import traceback
import os

from parmspg import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

START_DATE_SUMMARY_STR = '2023-01-01 00:00:00'
END_DATE_SUMMARY_STR   = '2025-06-10 00:00:00'

UMBRAL_DIRECCION = 0.5
UMBRAL_FUERTE = 20.0
UMBRAL_MODERADO = 8.0

FETCH_CHUNK_SIZE_DAYS = 1
VACUATE_SUMMARY_RANGE_BEFORE_RUN = False

UTC = pytz.timezone('UTC')

def connect_db_summary():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME,
            connect_timeout=15
        )
        conn.autocommit = False
        return conn
    except Exception as err:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] ERROR al conectar a PostgreSQL en generador_resumenes_historico_pg: {err}")
        return None

def fetch_alerts_for_summary_chunk(conn, ticker, chunk_start_dt_utc, chunk_end_dt_utc):
    if not conn:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] {ticker}: ERROR Conexión BD no disponible para fetch_alerts_for_summary_chunk.")
        return pd.DataFrame()
    alerts_df = pd.DataFrame()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT
                ticker,
                timestamp_alerta,
                puntos_long,
                puntos_short,
                puntos_neutral
            FROM alertas_generadas
            WHERE ticker = %s
              AND timestamp_alerta >= %s AND timestamp_alerta < %s
            ORDER BY timestamp_alerta ASC;
        """
        params = (
            ticker,
            chunk_start_dt_utc.strftime('%Y-%m-%d %H:%M:%S'),
            chunk_end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')
        )
        cursor.execute(query, params)
        data = cursor.fetchall()
        if data:
            alerts_df = pd.DataFrame(data)
            alerts_df['timestamp_alerta'] = pd.to_datetime(alerts_df['timestamp_alerta'], utc=True)
            alerts_df['puntos_long'] = pd.to_numeric(alerts_df['puntos_long'], errors='coerce').fillna(0.0)
            alerts_df['puntos_short'] = pd.to_numeric(alerts_df['puntos_short'], errors='coerce').fillna(0.0)
            alerts_df['puntos_neutral'] = pd.to_numeric(alerts_df['puntos_neutral'], errors='coerce').fillna(0.0)
            alerts_df.sort_values(by='timestamp_alerta', ascending=True, inplace=True)
            alerts_df.reset_index(drop=True, inplace=True)
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] ERROR en fetch_alerts_for_summary_chunk para {ticker} bloque {chunk_start_dt_utc.strftime('%Y-%m-%d')}: {e}")
        traceback.print_exc()
        alerts_df = pd.DataFrame()
    finally:
        if cursor: cursor.close()
    return alerts_df

def calculate_summary_for_alerts(alerts_df):
    if alerts_df.empty:
        return pd.DataFrame()
    summary_df = alerts_df.groupby('timestamp_alerta').agg(
        total_puntos_long=('puntos_long', 'sum'),
        total_puntos_short=('puntos_short', 'sum'),
        total_puntos_neutral=('puntos_neutral', 'sum')
    ).reset_index()
    summary_df.rename(columns={'timestamp_alerta': 'timestamp'}, inplace=True)
    summary_df['direccion_predominante'] = 'NEUTRAL'
    summary_df['fuerza_senal'] = 'N/A'
    diff = summary_df['total_puntos_long'] - summary_df['total_puntos_short']
    abs_diff = abs(diff)
    summary_df.loc[diff > UMBRAL_DIRECCION, 'direccion_predominante'] = 'LONG'
    summary_df.loc[diff < -UMBRAL_DIRECCION, 'direccion_predominante'] = 'SHORT'
    is_directional = summary_df['direccion_predominante'] != 'NEUTRAL'
    summary_df.loc[is_directional & (abs_diff >= UMBRAL_FUERTE), 'fuerza_senal'] = 'FUERTE'
    summary_df.loc[is_directional & (abs_diff >= UMBRAL_MODERADO) & (abs_diff < UMBRAL_FUERTE), 'fuerza_senal'] = 'MODERADA'
    summary_df.loc[is_directional & (abs_diff > UMBRAL_DIRECCION) & (abs_diff < UMBRAL_MODERADO), 'fuerza_senal'] = 'DEBIL'
    is_neutral = summary_df['direccion_predominante'] == 'NEUTRAL'
    all_points_near_zero = (abs(summary_df['total_puntos_long']) < 1e-9) & \
                           (abs(summary_df['total_puntos_short']) < 1e-9) & \
                           (abs(summary_df['total_puntos_neutral']) < 1e-9)
    context_active = (abs(summary_df['total_puntos_long']) < 1e-9) & \
                     (abs(summary_df['total_puntos_short']) < 1e-9) & \
                     (abs(summary_df['total_puntos_neutral']) > 1e-9)
    summary_df.loc[is_neutral & all_points_near_zero, 'fuerza_senal'] = 'PUNTOS CERO'
    summary_df.loc[is_neutral & context_active, 'fuerza_senal'] = 'CONTEXTO ACTIVO'
    summary_df.loc[is_neutral & ~(all_points_near_zero | context_active), 'fuerza_senal'] = 'INDEFINIDA'
    summary_df['total_puntos_long'] = summary_df['total_puntos_long'].round(2)
    summary_df['total_puntos_short'] = summary_df['total_puntos_short'].round(2)
    summary_df['total_puntos_neutral'] = summary_df['total_puntos_neutral'].round(2)
    # Agregar columnas yyyy, mm, dd, hh a partir del timestamp
    summary_df['yyyy'] = summary_df['timestamp'].dt.year.astype(np.int16)
    summary_df['mm'] = summary_df['timestamp'].dt.month.astype(np.int16)
    summary_df['dd'] = summary_df['timestamp'].dt.day.astype(np.int16)
    summary_df['hh'] = summary_df['timestamp'].dt.hour.astype(np.int16)
    return summary_df[['timestamp', 'total_puntos_long', 'total_puntos_short', 'total_puntos_neutral',
                       'direccion_predominante', 'fuerza_senal', 'yyyy', 'mm', 'dd', 'hh']]

def save_summary_to_db(conn, summary_df, ticker_symbol, script_run_timestamp_utc):
    if summary_df.empty:
        return 0
    if not conn:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] DB ERROR (save_summary_to_db): Conexión no disponible.")
        return -1
    cursor = None
    saved_count = 0
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO resumen_alertas
            (ticker, "timestamp", total_puntos_long, total_puntos_short, total_puntos_neutral,
             direccion_predominante, fuerza_senal, fecha_registro, yyyy, mm, dd, hh)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, "timestamp") DO UPDATE SET
                total_puntos_long = EXCLUDED.total_puntos_long,
                total_puntos_short = EXCLUDED.total_puntos_short,
                total_puntos_neutral = EXCLUDED.total_puntos_neutral,
                direccion_predominante = EXCLUDED.direccion_predominante,
                fuerza_senal = EXCLUDED.fuerza_senal,
                fecha_registro = EXCLUDED.fecha_registro,
                yyyy = EXCLUDED.yyyy,
                mm = EXCLUDED.mm,
                dd = EXCLUDED.dd,
                hh = EXCLUDED.hh
        """
        data_to_insert = []
        fecha_resumen_str = script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S')
        for index, row in summary_df.iterrows():
            timestamp_obj = row['timestamp']
            if timestamp_obj.tzinfo is None:
                timestamp_obj = timestamp_obj.replace(tzinfo=timezone.utc)
            timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            data_to_insert.append((
                ticker_symbol,
                timestamp_str,
                row['total_puntos_long'],
                row['total_puntos_short'],
                row['total_puntos_neutral'],
                row['direccion_predominante'],
                row['fuerza_senal'],
                fecha_resumen_str,
                int(row['yyyy']),
                int(row['mm']),
                int(row['dd']),
                int(row['hh'])
            ))
        if data_to_insert:
            cursor.executemany(sql, data_to_insert)
            saved_count = len(data_to_insert)
            conn.commit()
            return saved_count
        else:
            return 0
    except Exception as err:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] DB ERROR guardando resúmenes para {ticker_symbol}: {err}")
        traceback.print_exc()
        conn.rollback()
        return -1
    finally:
        if cursor: cursor.close()

def delete_summary_range(conn, ticker, start_date_utc, end_date_utc):
    if not conn:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] DB ERROR (delete_summary_range): Conexión no disponible.")
        return -1
    cursor = None
    try:
        cursor = conn.cursor()
        sql = """
            DELETE FROM resumen_alertas
            WHERE ticker = %s
            AND "timestamp" >= %s AND "timestamp" < %s
        """
        params = (
            ticker,
            start_date_utc.strftime('%Y-%m-%d %H:%M:%S'),
            end_date_utc.strftime('%Y-%m-%d %H:%M:%S')
        )
        cursor.execute(sql, params)
        deleted_count = cursor.rowcount
        conn.commit()
        return deleted_count
    except Exception as err:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] DB ERROR eliminando resúmenes para {ticker} rango {start_date_utc.strftime('%Y-%m-%d')} a {end_date_utc.strftime('%Y-%m-%d')}: {err}")
        traceback.print_exc()
        conn.rollback()
        return -1
    finally:
        if cursor: cursor.close()

def generate_historical_summaries():
    global UTC
    start_date_str = START_DATE_SUMMARY_STR
    end_date_str = END_DATE_SUMMARY_STR
    script_run_timestamp_utc = datetime.now(timezone.utc)
    try:
        start_date_utc = UTC.localize(datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S'))
        end_date_utc = UTC.localize(datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S'))
    except ValueError as e:
        print(f"[{script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] ERROR: Formato de fecha incorrecto en START_DATE_SUMMARY_STR o END_DATE_SUMMARY_STR. Use %Y-%m-%d %H:%M:%S. Error: {e}")
        return
    print(f"[{script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] ===============================================")
    print(f"[{script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Iniciando Generador de Resúmenes Históricos: `generate_historical_summaries`")
    print(f"[{script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Versión del Script: 2025-06-08_utc")
    print(f"[{script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Rango de Resumen: {start_date_utc.strftime('%Y-%m-%d %H:%M:%S%z')} a {end_date_utc.strftime('%Y-%m-%d %H:%M:%S%z')}")
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] Tamaño del Chunk de Lectura: {FETCH_CHUNK_SIZE_DAYS} Día(s)")
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] Guardado en tabla: resumen_alertas (usando ON CONFLICT)")
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] Vaciar rango antes de ejecutar: {VACUATE_SUMMARY_RANGE_BEFORE_RUN}")
    print(f"[{script_run_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] ===============================================")
    db_connection_main = connect_db_summary()
    if not db_connection_main:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] ABORTANDO generador de resúmenes: No se pudo conectar a BD para tareas iniciales/obtener tickers.");
        return
    active_tickers = []
    try:
        cursor_tickers = db_connection_main.cursor()
        cursor_tickers.execute("SELECT ticker FROM tickers WHERE activo = TRUE ORDER BY ticker ASC")
        active_tickers = [row[0] for row in cursor_tickers.fetchall()]
        cursor_tickers.close()
    except Exception as err:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] ERROR obtener tickers activos: {err}")
        traceback.print_exc()
        if db_connection_main: db_connection_main.close()
        return
    if not active_tickers:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] INFO No hay tickers activos encontrados. Generador de resúmenes finalizado.");
        if db_connection_main: db_connection_main.close()
        return
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] Tickers a procesar resumen: {active_tickers}")
    total_summaries_saved = 0
    tickers_processed_successfully = 0
    for ticker_symbol in active_tickers:
        print(f"\n--- [PID {os.getpid()}] Procesando resúmenes para Ticker: {ticker_symbol} ---")
        ticker_summary_success = True
        total_summaries_for_this_ticker = 0
        if VACUATE_SUMMARY_RANGE_BEFORE_RUN:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] Eliminando resúmenes existentes para {ticker_symbol} en rango {start_date_utc.strftime('%Y-%m-%d')} a {end_date_utc.strftime('%Y-%m-%d')}")
            deleted_count = delete_summary_range(db_connection_main, ticker_symbol, start_date_utc, end_date_utc)
            if deleted_count == -1:
                print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] ERROR al intentar eliminar resúmenes previos para {ticker_symbol}. Continuando, pero podría haber duplicados.")
                ticker_summary_success = False
            else:
                print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] Eliminados {deleted_count} resúmenes previos para {ticker_symbol} en el rango.")
        current_fetch_day_start = start_date_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        while current_fetch_day_start < end_date_utc:
            chunk_start_dt = current_fetch_day_start
            chunk_end_dt = current_fetch_day_start + timedelta(days=FETCH_CHUNK_SIZE_DAYS)
            if chunk_end_dt > end_date_utc:
                chunk_end_dt = end_date_utc
            if chunk_start_dt >= chunk_end_dt:
                break
            if not db_connection_main:
                print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] ADVERTENCIA: Conexión no disponible. Intentando reconectar.")
                db_connection_main = connect_db_summary()
                if not db_connection_main:
                    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] ERROR: Reconexión fallida. Saltando chunk.")
                    current_fetch_day_start += timedelta(days=FETCH_CHUNK_SIZE_DAYS)
                    ticker_summary_success = False
                    continue
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol}] Fetching alerts para bloque {chunk_start_dt.strftime('%Y-%m-%d')} a {chunk_end_dt.strftime('%Y-%m-%d')}")
            alerts_chunk_df = pd.DataFrame()
            try:
                alerts_chunk_df = fetch_alerts_for_summary_chunk(db_connection_main, ticker_symbol, chunk_start_dt, chunk_end_dt)
            except Exception as fetch_err:
                print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] ERROR durante fetch_alerts_for_summary_chunk: {fetch_err}")
                traceback.print_exc()
                ticker_summary_success = False
            if not alerts_chunk_df.empty:
                print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol}] Procesando {len(alerts_chunk_df)} alertas fetched...")
                try:
                    summary_for_chunk_df = calculate_summary_for_alerts(alerts_chunk_df)
                except Exception as calc_err:
                    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] ERROR calculando resumen: {calc_err}")
                    traceback.print_exc()
                    ticker_summary_success = False
                    summary_for_chunk_df = pd.DataFrame()
                if not summary_for_chunk_df.empty:
                    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] Calculados {len(summary_for_chunk_df)} resúmenes.")
                    try:
                        saved_count = save_summary_to_db(db_connection_main, summary_for_chunk_df, ticker_symbol, script_run_timestamp_utc)
                        if saved_count == -1:
                            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] ERROR guardando resúmenes.")
                            ticker_summary_success = False
                        else:
                            total_summaries_for_this_ticker += saved_count
                            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] Guardados {saved_count} resúmenes.")
                    except Exception as save_err:
                        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] [{ticker_symbol} Bloque {chunk_start_dt.strftime('%Y-%m-%d')}] ERROR inesperado guardando resúmenes: {save_err}")
                        traceback.print_exc()
                        ticker_summary_success = False
            current_fetch_day_start += timedelta(days=FETCH_CHUNK_SIZE_DAYS)
        if ticker_summary_success:
            tickers_processed_successfully += 1
            total_summaries_saved += total_summaries_for_this_ticker
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] --- Fin Resúmenes Ticker: {ticker_symbol}. Total resúmenes guardados para ticker: {total_summaries_for_this_ticker}")
        else:
            print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}] [PID {os.getpid()}] --- Fin Resúmenes Ticker: {ticker_symbol}. Hubo errores durante el procesamiento/guardado.")
    if db_connection_main:
        try: db_connection_main.close()
        except Exception: pass
    cycle_end_time_utc = datetime.now(timezone.utc)
    duration = (cycle_end_time_utc - script_run_timestamp_utc).total_seconds()
    print(f"\n[{cycle_end_time_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] ===============================================")
    print(f"[{cycle_end_time_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Fin del Generador de Resúmenes Históricos.")
    print(f"[{cycle_end_time_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Duración total: {duration:.2f}s.")
    print(f"[{cycle_end_time_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Total de Resúmenes guardados en resumen_alertas: {total_summaries_saved}.")
    print(f"[{cycle_end_time_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] Tickers procesados con éxito (sin errores mayores): {tickers_processed_successfully}/{len(active_tickers)}.")
    print(f"[{cycle_end_time_utc.strftime('%Y-%m-%d %H:%M:%S%z')}] ===============================================")

if __name__ == '__main__':
    generate_historical_summaries()
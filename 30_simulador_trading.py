# ===============================================================================
# Archivo: 30_simulador_trading.py
# Descripción: Simulador de trading realista basado en velas de 1 minuto, múltiples inversionistas y tickers.
#              Controla operaciones abiertas y límites en memoria, reflejando cada evento en la BD/log.
#              Para cada ticker y cada inversionista, recorre el tiempo minuto a minuto (ohlcv_raw_1m).
#              Procesa aperturas por señales y cierres por SL/TP.
#              Al finalizar la simulación, las operaciones abiertas se mantienen en la BD.
#              Al iniciar una nueva simulación, carga las operaciones abiertas a memoria.
# Versión: 2.1.0
# Fecha: 2025-06-17
# Autor: Hernando Galvis
# ===============================================================================

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import parmspg
from datetime import datetime, timedelta
import logging
import traceback
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

import numpy as np

UMBRAL_LONG = 70.0
UMBRAL_SHORT = 55.0

FECHA_INICIO = "2024-01-01 00:00:00"
FECHA_FIN    = "2025-06-20 00:00:00"

def py_native(val):
    import pandas as pd
    if isinstance(val, (np.generic, pd._libs.tslibs.nattype.NaTType)):
        if pd.isna(val):
            return None
        if isinstance(val, np.integer):
            return int(val)
        if isinstance(val, np.floating):
            return float(val)
        if isinstance(val, np.bool_):
            return bool(val)
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime()
    return val

def py_native_dict(d):
    return {k: py_native(v) for k, v in d.items()}

def get_engine():
    db_url = (
        f"postgresql+psycopg2://{parmspg.DB_USER}:{parmspg.DB_PASSWORD}"
        f"@{parmspg.DB_HOST}:{parmspg.DB_PORT}/{parmspg.DB_NAME}"
    )
    return create_engine(db_url)

def cargar_inversionistas(engine):
    query = "SELECT * FROM inversionistas WHERE activo = TRUE"
    return pd.read_sql(query, engine)

def cargar_inversionista_ticker(engine, id_inversionista, ticker):
    query = f"""
    SELECT * FROM inversionista_ticker
    WHERE id_inversionista_fk = {id_inversionista} 
      AND ticker_fk = '{ticker}'
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        return df.iloc[0].to_dict()
    return None

def cargar_senales_activas(engine, ticker, fecha_inicio=None, fecha_fin=None):
    query = f"""
    SELECT sg.*, e.primary_timeframe
    FROM senales_generadas sg
    JOIN estrategias e ON sg.id_estrategia_fk = e.id_estrategia
    WHERE sg.ticker_fk = '{ticker}'
      AND sg.id_estado_senal IN (
        SELECT id_estado_senal FROM estados_senales WHERE permite_operar = TRUE
      )
      AND e.activa = TRUE
    """
    if fecha_inicio:
        query += f" AND sg.timestamp_senal >= '{fecha_inicio}'"
    if fecha_fin:
        query += f" AND sg.timestamp_senal <= '{fecha_fin}'"
    query += " ORDER BY sg.timestamp_senal ASC"
    return pd.read_sql(query, engine)

def cargar_ohlcv_1m(engine, ticker, fecha_inicio, fecha_fin):
    query = f"""
    SELECT timestamp, open, high, low, close, volume
    FROM ohlcv_raw_1m
    WHERE ticker = '{ticker}'
      AND timestamp BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY timestamp ASC
    """
    return pd.read_sql(query, engine, index_col="timestamp")

def obtener_valor_indicador(engine, ticker, timestamp, timeframe, campo):
    query = f"""
    SELECT {campo}
    FROM indicadores
    WHERE ticker = '{ticker}'
      AND timeframe = {timeframe}
      AND "timestamp" <= '{timestamp}'
    ORDER BY "timestamp" DESC
    LIMIT 1
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        return float(df.iloc[0][campo]) if df.iloc[0][campo] is not None else None
    return None

def calcular_sl_tp(precio_entrada, atr_pct, mult_sl, mult_tp, tipo):
    if tipo.upper() == "LONG":
        sl = precio_entrada - (precio_entrada * atr_pct * mult_sl / 100)
        tp = precio_entrada + (precio_entrada * atr_pct * mult_tp / 100)
    elif tipo.upper() == "SHORT":
        sl = precio_entrada + (precio_entrada * atr_pct * mult_sl / 100)
        tp = precio_entrada - (precio_entrada * atr_pct * mult_tp / 100)
    else:
        raise ValueError("Tipo de operación no reconocido (debe ser LONG o SHORT)")
    return sl, tp

def extract_ymd(ts):
    if ts is None:
        return None, None, None
    if isinstance(ts, str):
        ts = pd.to_datetime(ts)
    return ts.year, ts.month, ts.day

def extract_hh(ts):
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = pd.to_datetime(ts)
    return ts.hour

def calcular_duracion_operacion_minutos(ts_apertura, ts_cierre):
    if ts_apertura is None or ts_cierre is None:
        return None
    if isinstance(ts_apertura, str):
        ts_apertura = pd.to_datetime(ts_apertura)
    if isinstance(ts_cierre, str):
        ts_cierre = pd.to_datetime(ts_cierre)
    return int((ts_cierre - ts_apertura).total_seconds() // 60)

def calcular_porc_sl_tp(precio_entrada, sl, tp):
    porc_sl = None
    porc_tp = None
    try:
        if precio_entrada and sl is not None:
            porc_sl = abs(precio_entrada - sl) / precio_entrada * 100
        if precio_entrada and tp is not None:
            porc_tp = abs(tp - precio_entrada) / precio_entrada * 100
    except Exception:
        porc_sl = None
        porc_tp = None
    return porc_sl, porc_tp

def registrar_log_operacion(
    session, tipo_evento, id_inversionista, id_estrategia, id_senal, id_operacion, ticker,
    detalle, capital_antes, capital_despues, precio_senal, sl, tp, cantidad,
    timestamp_apertura=None, timestamp_cierre=None, motivo_no_operacion=None,
    resultado=None, motivo_cierre=None, precio_cierre=None,
    duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
):
    yyyy_open, mm_open, dd_open = extract_ymd(timestamp_apertura)
    yyyy_close, mm_close, dd_close = extract_ymd(timestamp_cierre)
    hh_open = extract_hh(timestamp_apertura)
    hh_close = extract_hh(timestamp_cierre)
    params = {
        "timestamp_evento": datetime.now(),
        "id_inversionista_fk": py_native(id_inversionista),
        "id_estrategia_fk": py_native(id_estrategia),
        "id_senal_fk": py_native(id_senal),
        "id_operacion_fk": py_native(id_operacion),
        "ticker": ticker,
        "tipo_evento": tipo_evento,
        "detalle": detalle[:1000] if detalle else None,
        "capital_antes": py_native(capital_antes),
        "capital_despues": py_native(capital_despues),
        "precio_senal": py_native(precio_senal),
        "sl": py_native(sl),
        "tp": py_native(tp),
        "cantidad": py_native(cantidad),
        "yyyy_open": py_native(yyyy_open),
        "mm_open": py_native(mm_open),
        "dd_open": py_native(dd_open),
        "yyyy_close": py_native(yyyy_close),
        "mm_close": py_native(mm_close),
        "dd_close": py_native(dd_close),
        "hh_open": py_native(hh_open),
        "hh_close": py_native(hh_close),
        "motivo_no_operacion": motivo_no_operacion,
        "resultado": py_native(resultado),
        "motivo_cierre": motivo_cierre,
        "precio_cierre": py_native(precio_cierre),
        "duracion_operacion": py_native(duracion_operacion),
        "porc_sl": py_native(porc_sl),
        "porc_tp": py_native(porc_tp),
        "volumen_osc_asociado": py_native(volumen_osc_asociado)
    }
    params = py_native_dict(params)
    ins = text("""
    INSERT INTO log_operaciones_simuladas (
        timestamp_evento, id_inversionista_fk, id_estrategia_fk, id_senal_fk, id_operacion_fk, ticker, tipo_evento,
        detalle, capital_antes, capital_despues, precio_senal, sl, tp, cantidad,
        yyyy_open, mm_open, dd_open, yyyy_close, mm_close, dd_close,
        hh_open, hh_close,
        motivo_no_operacion, resultado, motivo_cierre, precio_cierre,
        duracion_operacion, porc_sl, porc_tp, volumen_osc_asociado
    ) VALUES (
        :timestamp_evento, :id_inversionista_fk, :id_estrategia_fk, :id_senal_fk, :id_operacion_fk, :ticker, :tipo_evento,
        :detalle, :capital_antes, :capital_despues, :precio_senal, :sl, :tp, :cantidad,
        :yyyy_open, :mm_open, :dd_open, :yyyy_close, :mm_close, :dd_close,
        :hh_open, :hh_close,
        :motivo_no_operacion, :resultado, :motivo_cierre, :precio_cierre,
        :duracion_operacion, :porc_sl, :porc_tp, :volumen_osc_asociado
    )
    """)
    session.execute(ins, params)
    session.commit()

def cargar_operaciones_abiertas(engine, id_inversionista, ticker):
    query = f"""
    SELECT id_operacion, id_estrategia_fk, id_senal_fk, ticker_fk, timestamp_apertura, 
           precio_entrada, cantidad, tipo_operacion, stop_loss_price, take_profit_price
    FROM operaciones_simuladas
    WHERE id_inversionista_fk = {id_inversionista}
      AND ticker_fk = '{ticker}'
      AND timestamp_cierre IS NULL
    """
    df = pd.read_sql(query, engine)
    abiertas = []
    for _, row in df.iterrows():
        abiertas.append({
            'id_operacion': row['id_operacion'],
            'ticker': row['ticker_fk'],
            'ts_apertura': pd.Timestamp(row['timestamp_apertura']),
            'precio_entrada': float(row['precio_entrada']),
            'sl': float(row['stop_loss_price']),
            'tp': float(row['take_profit_price']),
            'tipo': row['tipo_operacion'],
            'cantidad': float(row['cantidad']),
            'id_estrategia': row['id_estrategia_fk'],
            'id_senal': row['id_senal_fk'],
            'capital_antes': None,  # no lo sabemos, pero es opcional para los logs de cierre
            'origen_parametros': None
        })
    return abiertas

def simular():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        inversionistas = cargar_inversionistas(engine).to_dict("records")
        tickers = pd.read_sql(f"SELECT DISTINCT ticker FROM ohlcv_raw_1m WHERE timestamp BETWEEN '{FECHA_INICIO}' AND '{FECHA_FIN}'", engine)["ticker"].tolist()
        logging.info(f"Tickers a simular: {tickers}")

        for ticker in tickers:
            velas = cargar_ohlcv_1m(engine, ticker, FECHA_INICIO, FECHA_FIN)
            if velas.empty:
                continue
            senales = cargar_senales_activas(engine, ticker, FECHA_INICIO, FECHA_FIN)
            # Indexar señales por timestamp
            senales_por_timestamp = defaultdict(list)
            for _, s in senales.iterrows():
                ts = pd.Timestamp(s['timestamp_senal'])
                senales_por_timestamp[ts].append(s)
            timestamps_1m = velas.index

            for inv in inversionistas:
                logging.info(f"Simulando {ticker} para inversionista {inv['nombre']} ({inv['id_inversionista']})")
                # Cargar capital actual: si hay operaciones abiertas, no se sabe el capital real, pero se puede llevar como antes
                capital_actual = float(inv["capital_aportado"])
                # Cargar operaciones abiertas desde BD (NO se cierran al final de la simulación)
                operaciones_abiertas_mem = cargar_operaciones_abiertas(engine, inv["id_inversionista"], ticker)
                operaciones_diarias_count = defaultdict(int)
                inv_ticker = cargar_inversionista_ticker(engine, inv["id_inversionista"], ticker)
                if inv_ticker is not None and inv_ticker.get("limite_operaciones_abiertas") is not None:
                    limite_operaciones_abiertas = int(inv_ticker["limite_operaciones_abiertas"])
                else:
                    limite_operaciones_abiertas = int(inv.get("limite_operaciones_abiertas", 3))
                if inv_ticker is not None and inv_ticker.get("limite_diario_operaciones") is not None:
                    limite_diario_operaciones = int(inv_ticker["limite_diario_operaciones"])
                else:
                    limite_diario_operaciones = int(inv.get("limite_diario_operaciones", 10))

                for ts in timestamps_1m:
                    yyyy, mm, dd = extract_ymd(ts)
                    key_diaria = (yyyy, mm, dd)
                    # 1. Proceso de apertura por señales
                    if ts in senales_por_timestamp:
                        for senal in senales_por_timestamp[ts]:
                            capital_antes = capital_actual
                            tipo = str(senal.get("tipo_senal", "")).upper()
                            porc_long = float(senal.get("porc_long", 0) or 0)
                            porc_short = float(senal.get("porc_short", 0) or 0)
                            pasa_umbral = True
                            motivo_no_op = ""
                            if tipo == "LONG" and porc_long < UMBRAL_LONG:
                                pasa_umbral = False
                                motivo_no_op = f"porc_long_bajo ({porc_long} < {UMBRAL_LONG})"
                            elif tipo == "SHORT" and porc_short < UMBRAL_SHORT:
                                pasa_umbral = False
                                motivo_no_op = f"porc_short_bajo ({porc_short} < {UMBRAL_SHORT})"
                            elif tipo not in ("LONG", "SHORT"):
                                pasa_umbral = False
                                motivo_no_op = "tipo_senal_invalido"

                            if not pasa_umbral:
                                registrar_log_operacion(
                                    session, "no_operacion", inv['id_inversionista'], senal['id_estrategia_fk'], senal['id_senal'], None, ticker,
                                    f"Señal descartada por umbral: {motivo_no_op}",
                                    capital_antes, capital_actual, senal.get("precio_senal"), None, None, None,
                                    timestamp_apertura=ts,
                                    motivo_no_operacion=motivo_no_op,
                                    duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
                                )
                                continue

                            # Control diario
                            if operaciones_diarias_count[key_diaria] >= limite_diario_operaciones:
                                registrar_log_operacion(
                                    session, "no_operacion", inv['id_inversionista'], senal['id_estrategia_fk'], senal['id_senal'], None, ticker,
                                    f"Límite diario de operaciones ({limite_diario_operaciones}) para este ticker alcanzado.",
                                    capital_antes, capital_actual, senal.get("precio_senal"), None, None, None,
                                    timestamp_apertura=ts,
                                    motivo_no_operacion="limite_diario_operaciones",
                                    duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
                                )
                                continue

                            # Control simultáneas
                            abiertas_este_ticker = [op for op in operaciones_abiertas_mem if op['ticker'] == ticker]
                            if len(abiertas_este_ticker) >= limite_operaciones_abiertas:
                                registrar_log_operacion(
                                    session, "no_operacion", inv['id_inversionista'], senal['id_estrategia_fk'], senal['id_senal'], None, ticker,
                                    f"Límite de operaciones abiertas simultáneas ({limite_operaciones_abiertas}) para este ticker alcanzado.",
                                    capital_antes, capital_actual, senal.get("precio_senal"), None, None, None,
                                    timestamp_apertura=ts,
                                    motivo_no_operacion="limite_operaciones_abiertas",
                                    duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
                                )
                                continue

                            # MENSAJE DE NUEVA OPERACION
                            logging.info(
                                f"[Nueva operación] {inv['nombre']} | Estrategia: {senal['id_estrategia_fk']} | Señal: {senal['id_senal']} | "
                                f"Ticker: {ticker} | Fecha: {ts} | Tipo: {tipo} | Porc_long: {porc_long} | Porc_short: {porc_short} | "
                                f"Abiertas para este ticker: {len(abiertas_este_ticker)}/{limite_operaciones_abiertas}"
                            )

                            riesgo_max = capital_actual * (float(inv["riesgo_max_operacion_pct"]) / 100.0)
                            precio = float(senal["precio_senal"] or 0)
                            if not precio or precio <= 0:
                                registrar_log_operacion(
                                    session, "no_operacion", inv['id_inversionista'], senal['id_estrategia_fk'], senal['id_senal'], None, ticker,
                                    "Precio de señal inválido o nulo", capital_antes, capital_actual, precio,
                                    None, None, None, timestamp_apertura=ts,
                                    motivo_no_operacion="precio_invalido",
                                    duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
                                )
                                continue

                            usar_param_senal = bool(inv.get("usar_parametros_senal", False))
                            if inv_ticker is not None and not usar_param_senal:
                                tamano_max = float(inv_ticker.get("tamano_max_operacion", riesgo_max))
                                tamano_min = float(inv_ticker.get("tamano_min_operacion", 0.0))
                            else:
                                tamano_max = float(inv.get("tamano_max_operacion", riesgo_max))
                                tamano_min = float(inv.get("tamano_min_operacion", 0.0))

                            monto_usd = max(min(riesgo_max, tamano_max), tamano_min)
                            if capital_actual <= 0 or capital_actual < monto_usd:
                                msg = f"{inv['nombre']} sin capital suficiente para operar. Fin de simulación para este inversionista."
                                logging.warning(msg)
                                registrar_log_operacion(
                                    session, "no_operacion", inv['id_inversionista'], senal['id_estrategia_fk'], senal['id_senal'], None, ticker,
                                    msg, capital_antes, capital_actual, precio, None, None, None,
                                    timestamp_apertura=ts,
                                    motivo_no_operacion="capital_insuficiente",
                                    duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
                                )
                                break

                            cantidad = monto_usd / precio
                            origen_parametros = None

                            primary_timeframe = int(senal.get("primary_timeframe", 240))
                            atr_percent = obtener_valor_indicador(engine, ticker, ts, primary_timeframe, "atr_percent")
                            volumen_osc = obtener_valor_indicador(engine, ticker, ts, primary_timeframe, "volumen_osc")
                            if usar_param_senal:
                                sl_raw = senal.get("stop_loss_price")
                                tp_raw = senal.get("take_profit_price")
                                if sl_raw is None or tp_raw is None:
                                    mult_sl = float(senal.get("mult_sl_asignado", 1.0))
                                    mult_tp = float(senal.get("mult_tp_asignado", 2.0))
                                    atr_pct = atr_percent if atr_percent is not None else float(senal.get("atr_percent_usado", 0.01))
                                    sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])
                                else:
                                    sl = float(sl_raw)
                                    tp = float(tp_raw)
                                    atr_pct = atr_percent if atr_percent is not None else float(senal.get("atr_percent_usado", 0.01))
                                apalancamiento = int(senal.get("apalancamiento_calculado") or 1)
                                mult_sl = float(senal.get("mult_sl_asignado", 1.0))
                                mult_tp = float(senal.get("mult_tp_asignado", 2.0))
                                origen_parametros = "senal"
                            elif inv_ticker is not None:
                                mult_sl = float(inv_ticker.get("mult_sl", inv.get("mult_sl", 1.0)))
                                mult_tp = float(inv_ticker.get("mult_tp", inv.get("mult_tp", 2.0)))
                                atr_pct = atr_percent if atr_percent is not None else float(inv_ticker.get("atr_percent", senal.get("atr_percent_usado", 0.01)))
                                sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])
                                apalancamiento = int(inv_ticker.get("apalancamiento", inv.get("apalancamiento_max", 1)))
                                origen_parametros = "inversionista_ticker"
                            else:
                                mult_sl = float(inv.get("mult_sl", 1.0))
                                mult_tp = float(inv.get("mult_tp", 2.0))
                                atr_pct = atr_percent if atr_percent is not None else float(senal.get("atr_percent_usado", 0.01))
                                sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])
                                apalancamiento = int(inv.get("apalancamiento_max", 1))
                                origen_parametros = "inversionista"

                            porc_sl, porc_tp = calcular_porc_sl_tp(precio, sl, tp)

                            ins_op = text("""
                            INSERT INTO operaciones_simuladas (
                                id_inversionista_fk, id_estrategia_fk, id_senal_fk, ticker_fk, timestamp_apertura,
                                precio_entrada, cantidad, apalancamiento, tipo_operacion, capital_riesgo_usado,
                                capital_bloqueado,
                                stop_loss_price, take_profit_price, atr_percent_usado, mult_sl_asignado, mult_tp_asignado,
                                origen_parametros,
                                yyyy_open, mm_open, dd_open, hh_open,
                                porc_sl, porc_tp, volumen_osc_asociado
                            ) VALUES (
                                :id_inversionista, :id_estrategia, :id_senal, :ticker, :timestamp_apertura,
                                :precio_entrada, :cantidad, :apalancamiento, :tipo_operacion, :capital_riesgo_usado,
                                :capital_bloqueado,
                                :stop_loss_price, :take_profit_price, :atr_percent_usado, :mult_sl_asignado, :mult_tp_asignado,
                                :origen_parametros,
                                :yyyy_open, :mm_open, :dd_open, :hh_open,
                                :porc_sl, :porc_tp, :volumen_osc_asociado
                            ) RETURNING id_operacion
                            """)
                            op_params = {
                                "id_inversionista": inv["id_inversionista"],
                                "id_estrategia": senal["id_estrategia_fk"],
                                "id_senal": senal["id_senal"],
                                "ticker": ticker,
                                "timestamp_apertura": ts,
                                "precio_entrada": precio,
                                "cantidad": cantidad,
                                "apalancamiento": apalancamiento,
                                "tipo_operacion": senal["tipo_senal"],
                                "capital_riesgo_usado": monto_usd,
                                "capital_bloqueado": monto_usd,
                                "stop_loss_price": sl,
                                "take_profit_price": tp,
                                "atr_percent_usado": atr_pct,
                                "mult_sl_asignado": mult_sl,
                                "mult_tp_asignado": mult_tp,
                                "origen_parametros": origen_parametros,
                                "yyyy_open": yyyy,
                                "mm_open": mm,
                                "dd_open": dd,
                                "hh_open": extract_hh(ts),
                                "porc_sl": porc_sl,
                                "porc_tp": porc_tp,
                                "volumen_osc_asociado": volumen_osc
                            }
                            op_params = py_native_dict(op_params)
                            op_res = session.execute(ins_op, op_params)
                            id_operacion = op_res.fetchone()[0]
                            session.commit()
                            registrar_log_operacion(
                                session, "apertura", inv['id_inversionista'], senal['id_estrategia_fk'], senal['id_senal'], id_operacion, ticker,
                                f"Operación abierta (SL:{sl}, TP:{tp}, origen:{origen_parametros}, monto_usd:{monto_usd}, cantidad:{cantidad}, porc_sl:{porc_sl}, porc_tp:{porc_tp})",
                                capital_antes, capital_actual, precio, sl, tp, cantidad,
                                timestamp_apertura=ts,
                                duracion_operacion=None, porc_sl=porc_sl, porc_tp=porc_tp, volumen_osc_asociado=volumen_osc
                            )
                            operaciones_abiertas_mem.append({
                                'id_operacion': id_operacion,
                                'ticker': ticker,
                                'ts_apertura': ts,
                                'precio_entrada': precio,
                                'sl': sl,
                                'tp': tp,
                                'tipo': tipo,
                                'cantidad': cantidad,
                                'id_estrategia': senal['id_estrategia_fk'],
                                'id_senal': senal['id_senal'],
                                'capital_antes': capital_antes,
                                'origen_parametros': origen_parametros
                            })
                            operaciones_diarias_count[key_diaria] += 1

                    # 2. Proceso de cierre por SL/TP
                    nuevas_abiertas = []
                    for op in operaciones_abiertas_mem:
                        cerrar = False
                        motivo_cierre = None
                        precio_cierre = None
                        if op['tipo'] == "LONG":
                            if velas.loc[ts, "low"] <= op['sl']:
                                cerrar = True
                                motivo_cierre = "SL hit"
                                precio_cierre = op['sl']
                            elif velas.loc[ts, "high"] >= op['tp']:
                                cerrar = True
                                motivo_cierre = "TP hit"
                                precio_cierre = op['tp']
                        elif op['tipo'] == "SHORT":
                            if velas.loc[ts, "high"] >= op['sl']:
                                cerrar = True
                                motivo_cierre = "SL hit"
                                precio_cierre = op['sl']
                            elif velas.loc[ts, "low"] <= op['tp']:
                                cerrar = True
                                motivo_cierre = "TP hit"
                                precio_cierre = op['tp']
                        if cerrar:
                            yyyy_close, mm_close, dd_close = extract_ymd(ts)
                            hh_close = extract_hh(ts)
                            duracion_operacion = calcular_duracion_operacion_minutos(op['ts_apertura'], ts)
                            if precio_cierre is not None:
                                if op['tipo'] == "LONG":
                                    resultado = (precio_cierre - op['precio_entrada']) * op['cantidad']
                                else:
                                    resultado = (op['precio_entrada'] - precio_cierre) * op['cantidad']
                                capital_actual += resultado
                            up_op = text("""
                                UPDATE operaciones_simuladas
                                SET timestamp_cierre = :timestamp_cierre,
                                    precio_cierre = :precio_cierre,
                                    resultado = (:precio_cierre - precio_entrada) * cantidad * 
                                        (CASE WHEN tipo_operacion = 'LONG' THEN 1 ELSE -1 END),
                                    motivo_cierre = :motivo_cierre,
                                    yyyy_close = :yyyy_close,
                                    mm_close = :mm_close,
                                    dd_close = :dd_close,
                                    hh_close = :hh_close,
                                    duracion_operacion = :duracion_operacion
                                WHERE id_operacion = :id_operacion
                            """)
                            up_params = {
                                "timestamp_cierre": py_native(ts),
                                "precio_cierre": py_native(precio_cierre),
                                "motivo_cierre": motivo_cierre,
                                "yyyy_close": py_native(yyyy_close),
                                "mm_close": py_native(mm_close),
                                "dd_close": py_native(dd_close),
                                "hh_close": py_native(hh_close),
                                "duracion_operacion": py_native(duracion_operacion),
                                "id_operacion": op['id_operacion'],
                            }
                            up_params = py_native_dict(up_params)
                            session.execute(up_op, up_params)
                            session.commit()
                            registrar_log_operacion(
                                session, "cierre", inv['id_inversionista'], op['id_estrategia'], op['id_senal'], op['id_operacion'], ticker,
                                f"Cierre de operación {op['id_operacion']}: {motivo_cierre} en {ts}, precio {precio_cierre}, duración: {duracion_operacion} minutos",
                                op['capital_antes'], capital_actual, op['precio_entrada'], op['sl'], op['tp'], op['cantidad'],
                                timestamp_apertura=op['ts_apertura'],
                                timestamp_cierre=ts,
                                motivo_cierre=motivo_cierre, resultado=resultado, precio_cierre=precio_cierre,
                                duracion_operacion=duracion_operacion, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
                            )
                        else:
                            nuevas_abiertas.append(op)
                    operaciones_abiertas_mem = nuevas_abiertas

                # Al final, NO cerrar operaciones abiertas por fin de simulación.
                logging.info(f"Capital final para {inv['nombre']} ({ticker}): {capital_actual}")

    except Exception as e:
        session.rollback()
        tb = traceback.format_exc()
        error_msg = f"Error en simulador principal: {e}\n{tb}"
        logging.error(error_msg)
        registrar_log_operacion(
            session, "error", None, None, None, None, None,
            error_msg, None, None, None, None, None, None,
            motivo_no_operacion="error_global",
            duracion_operacion=None, porc_sl=None, porc_tp=None, volumen_osc_asociado=None
        )

    finally:
        session.close()

if __name__ == "__main__":
    simular()

# ===============================================================================
# FIN DEL ARCHIVO: 30_simulador_trading.py
# ===============================================================================

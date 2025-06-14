# ===============================================================================
# Archivo: 30_simulador_trading.py
# Descripción: Simulador de trading con control de operaciones simultáneas y totales diarias por ticker e inversionista.
#              Usa campos limite_operaciones_abiertas y limite_diario_operaciones (en inversionistas y en inversionista_ticker)
#              Controla solo estrategias activas y solo inversionistas activos.
#              Optimiza validación usando arreglo en memoria para operaciones abiertas y diccionario para conteo diario.
#              Corrige bug de tipos numpy/pandas en inserts SQL.
#              Corrige bug: convierte todos los parámetros a tipos Python nativos antes de cualquier insert/update SQL.
# Versión: 1.3.3
# Fecha: 2025-06-13
# Autor: Hernando Galvis
# ===============================================================================

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import parmspg
from datetime import datetime
import logging
import traceback

fecha_inicio = "2025-04-01 00:00:00"
fecha_fin    = "2025-05-01 00:00:00"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

import numpy as np

def py_native(val):
    """Convierte valores numpy/pandas a tipos nativos de Python para SQL."""
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
    """Convierte todos los valores de un dict a tipos Python nativos."""
    return {k: py_native(v) for k, v in d.items()}

def get_engine():
    db_url = (
        f"postgresql+psycopg2://{parmspg.DB_USER}:{parmspg.DB_PASSWORD}"
        f"@{parmspg.DB_HOST}:{parmspg.DB_PORT}/{parmspg.DB_NAME}"
    )
    return create_engine(db_url)

def cargar_inversionistas(engine):
    # Solo inversionistas activos (campo booleano)
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

def cargar_senales_activas(engine, tickers=None, fecha_inicio=None, fecha_fin=None):
    # Solo señales de estrategias activas (campo booleano)
    query = """
    SELECT sg.* FROM senales_generadas sg
    JOIN estrategias e ON sg.id_estrategia_fk = e.id_estrategia
    WHERE sg.id_estado_senal IN (
        SELECT id_estado_senal FROM estados_senales WHERE permite_operar = TRUE
    )
    AND e.activa = TRUE
    """
    if fecha_inicio:
        query += f" AND sg.timestamp_senal >= '{fecha_inicio}'"
    if fecha_fin:
        query += f" AND sg.timestamp_senal <= '{fecha_fin}'"
    if tickers:
        tickers_str = ",".join([f"'{t}'" for t in tickers])
        query += f" AND sg.ticker_fk IN ({tickers_str})"
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

def registrar_log_operacion(
    session, tipo_evento, id_inversionista, id_senal, id_operacion, ticker,
    detalle, capital_antes, capital_despues, precio_senal, sl, tp, cantidad,
    timestamp_apertura=None, timestamp_cierre=None, motivo_no_operacion=None,
    resultado=None, motivo_cierre=None, precio_cierre=None
):
    yyyy_open, mm_open, dd_open = extract_ymd(timestamp_apertura)
    yyyy_close, mm_close, dd_close = extract_ymd(timestamp_cierre)
    # Convierte todos los valores numéricos a tipos nativos de Python
    params = {
        "timestamp_evento": datetime.now(),
        "id_inversionista_fk": py_native(id_inversionista),
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
        "motivo_no_operacion": motivo_no_operacion,
        "resultado": py_native(resultado),
        "motivo_cierre": motivo_cierre,
        "precio_cierre": py_native(precio_cierre)
    }
    params = py_native_dict(params)
    ins = text("""
    INSERT INTO log_operaciones_simuladas (
        timestamp_evento, id_inversionista_fk, id_senal_fk, id_operacion_fk, ticker, tipo_evento,
        detalle, capital_antes, capital_despues, precio_senal, sl, tp, cantidad,
        yyyy_open, mm_open, dd_open, yyyy_close, mm_close, dd_close,
        motivo_no_operacion, resultado, motivo_cierre, precio_cierre
    ) VALUES (
        :timestamp_evento, :id_inversionista_fk, :id_senal_fk, :id_operacion_fk, :ticker, :tipo_evento,
        :detalle, :capital_antes, :capital_despues, :precio_senal, :sl, :tp, :cantidad,
        :yyyy_open, :mm_open, :dd_open, :yyyy_close, :mm_close, :dd_close,
        :motivo_no_operacion, :resultado, :motivo_cierre, :precio_cierre
    )
    """)
    session.execute(ins, params)
    session.commit()

def simular():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        inversionistas = cargar_inversionistas(engine).to_dict("records")
        logging.info(f"Se cargaron {len(inversionistas)} inversionistas activos.")

        senales = cargar_senales_activas(engine, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)
        logging.info(f"Se cargaron {len(senales)} señales activas de estrategias activas entre {fecha_inicio} y {fecha_fin}.")

        for inv in inversionistas:
            logging.info(f"Simulando para inversionista {inv['nombre']} (id={inv['id_inversionista']})")
            capital_actual = float(inv["capital_aportado"])
            operaciones_abiertas_mem = []  # Lista en memoria para operaciones abiertas

            # Diccionario para conteo de operaciones diarias por ticker: {(ticker, yyyy, mm, dd): count}
            operaciones_diarias_count = {}

            for idx, senal in senales.iterrows():
                logging.info(f"[{inv['nombre']}] Procesando señal {idx+1}/{len(senales)}: Ticker={senal['ticker_fk']} Hora={senal['timestamp_senal']} Tipo={senal['tipo_senal']} Precio={senal['precio_senal']}")
                capital_antes = capital_actual
                try:
                    yyyy_open, mm_open, dd_open = extract_ymd(senal["timestamp_senal"])
                    ticker = senal["ticker_fk"]
                    key_diaria = (ticker, yyyy_open, mm_open, dd_open)

                    # Obtener límites
                    inv_ticker = cargar_inversionista_ticker(engine, inv["id_inversionista"], ticker)
                    # Límite de operaciones simultáneas abiertas
                    if inv_ticker is not None and inv_ticker.get("limite_operaciones_abiertas") is not None:
                        limite_operaciones_abiertas = int(inv_ticker["limite_operaciones_abiertas"])
                    else:
                        limite_operaciones_abiertas = int(inv.get("limite_operaciones_abiertas", 3))
                    # Límite de operaciones diarias
                    if inv_ticker is not None and inv_ticker.get("limite_diario_operaciones") is not None:
                        limite_diario_operaciones = int(inv_ticker["limite_diario_operaciones"])
                    else:
                        limite_diario_operaciones = int(inv.get("limite_diario_operaciones", 10))

                    # Validar límite diario de operaciones (total abiertas en el día para ese ticker)
                    count_diario = operaciones_diarias_count.get(key_diaria, 0)
                    if count_diario >= limite_diario_operaciones:
                        registrar_log_operacion(
                            session, "no_operacion", inv['id_inversionista'], senal['id_senal'], None, ticker,
                            f"Límite diario de operaciones ({limite_diario_operaciones}) para este ticker alcanzado.",
                            capital_antes, capital_actual, senal.get("precio_senal"), None, None, None,
                            timestamp_apertura=senal["timestamp_senal"],
                            motivo_no_operacion="limite_diario_operaciones"
                        )
                        continue

                    # Validar en memoria operaciones abiertas para ese ticker
                    abiertas_este_ticker = [
                        op for op in operaciones_abiertas_mem if op['ticker'] == ticker
                    ]
                    if len(abiertas_este_ticker) >= limite_operaciones_abiertas:
                        registrar_log_operacion(
                            session, "no_operacion", inv['id_inversionista'], senal['id_senal'], None, ticker,
                            f"Límite de operaciones abiertas simultáneas ({limite_operaciones_abiertas}) para este ticker alcanzado.",
                            capital_antes, capital_actual, senal.get("precio_senal"), None, None, None,
                            timestamp_apertura=senal["timestamp_senal"],
                            motivo_no_operacion="limite_operaciones_abiertas"
                        )
                        continue

                    # Lógica normal de apertura
                    riesgo_max = capital_actual * (float(inv["riesgo_max_operacion_pct"]) / 100.0)
                    precio = float(senal["precio_senal"] or 0)
                    if not precio or precio <= 0:
                        registrar_log_operacion(
                            session, "no_operacion", inv['id_inversionista'], senal['id_senal'], None, ticker,
                            "Precio de señal inválido o nulo", capital_antes, capital_actual, precio,
                            None, None, None, timestamp_apertura=senal['timestamp_senal'],
                            motivo_no_operacion="precio_invalido"
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
                            session, "no_operacion", inv['id_inversionista'], senal['id_senal'], None, ticker,
                            msg, capital_antes, capital_actual, precio, None, None, None,
                            timestamp_apertura=senal['timestamp_senal'],
                            motivo_no_operacion="capital_insuficiente"
                        )
                        break

                    cantidad = monto_usd / precio

                    origen_parametros = None

                    if usar_param_senal:
                        sl_raw = senal.get("stop_loss_price")
                        tp_raw = senal.get("take_profit_price")
                        if sl_raw is None or tp_raw is None:
                            mult_sl = float(senal.get("mult_sl_asignado", 1.0))
                            mult_tp = float(senal.get("mult_tp_asignado", 2.0))
                            atr_pct = float(senal.get("atr_percent_usado", 0.01))
                            sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])
                        else:
                            sl = float(sl_raw)
                            tp = float(tp_raw)
                        apalancamiento = int(senal.get("apalancamiento_calculado") or 1)
                        mult_sl = float(senal.get("mult_sl_asignado", 1.0))
                        mult_tp = float(senal.get("mult_tp_asignado", 2.0))
                        atr_pct = float(senal.get("atr_percent_usado", 0.01))
                        origen_parametros = "senal"
                    elif inv_ticker is not None:
                        mult_sl = float(inv_ticker.get("mult_sl", inv.get("mult_sl", 1.0)))
                        mult_tp = float(inv_ticker.get("mult_tp", inv.get("mult_tp", 2.0)))
                        atr_pct = float(inv_ticker.get("atr_percent", senal.get("atr_percent_usado", 0.01)))
                        sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])
                        apalancamiento = int(inv_ticker.get("apalancamiento", inv.get("apalancamiento_max", 1)))
                        origen_parametros = "inversionista_ticker"
                    else:
                        mult_sl = float(inv.get("mult_sl", 1.0))
                        mult_tp = float(inv.get("mult_tp", 2.0))
                        atr_pct = float(senal.get("atr_percent_usado", 0.01))
                        sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])
                        apalancamiento = int(inv.get("apalancamiento_max", 1))
                        origen_parametros = "inversionista"

                    ins_op = text("""
                    INSERT INTO operaciones_simuladas (
                        id_inversionista_fk, id_estrategia_fk, id_senal_fk, ticker_fk, timestamp_apertura,
                        precio_entrada, cantidad, apalancamiento, tipo_operacion, capital_riesgo_usado,
                        capital_bloqueado,
                        stop_loss_price, take_profit_price, atr_percent_usado, mult_sl_asignado, mult_tp_asignado,
                        origen_parametros,
                        yyyy_open, mm_open, dd_open
                    ) VALUES (
                        :id_inversionista, :id_estrategia, :id_senal, :ticker, :timestamp_apertura,
                        :precio_entrada, :cantidad, :apalancamiento, :tipo_operacion, :capital_riesgo_usado,
                        :capital_bloqueado,
                        :stop_loss_price, :take_profit_price, :atr_percent_usado, :mult_sl_asignado, :mult_tp_asignado,
                        :origen_parametros,
                        :yyyy_open, :mm_open, :dd_open
                    ) RETURNING id_operacion
                    """)
                    # --- CONVIERTE TODOS LOS PARÁMETROS A TIPOS PYTHON NATIVOS ---
                    op_params = {
                        "id_inversionista": inv["id_inversionista"],
                        "id_estrategia": senal["id_estrategia_fk"],
                        "id_senal": senal["id_senal"],
                        "ticker": ticker,
                        "timestamp_apertura": senal["timestamp_senal"],
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
                        "yyyy_open": yyyy_open,
                        "mm_open": mm_open,
                        "dd_open": dd_open
                    }
                    op_params = py_native_dict(op_params)
                    op_res = session.execute(ins_op, op_params)
                    id_operacion = op_res.fetchone()[0]
                    session.commit()
                    registrar_log_operacion(
                        session, "apertura", inv['id_inversionista'], senal['id_senal'], id_operacion, ticker,
                        f"Operación abierta (SL:{sl}, TP:{tp}, origen:{origen_parametros}, monto_usd:{monto_usd}, cantidad:{cantidad})",
                        capital_antes, capital_actual, precio, sl, tp, cantidad,
                        timestamp_apertura=senal['timestamp_senal']
                    )
                    # --- AGREGAR OPERACIÓN AL ARREGLO EN MEMORIA ---
                    operaciones_abiertas_mem.append({
                        'id_operacion': id_operacion,
                        'ticker': ticker
                    })
                    # --- SUMAR AL LIMITE DIARIO DE OPERACIONES ---
                    operaciones_diarias_count[key_diaria] = count_diario + 1

                    ohlcv = cargar_ohlcv_1m(
                        engine,
                        ticker,
                        senal["timestamp_senal"],
                        fecha_fin
                    )
                    sl_hit = tp_hit = False
                    cierre_timestamp = cierre_precio = motivo_cierre = resultado = None
                    for ts, row in ohlcv.iterrows():
                        if senal["tipo_senal"].upper() == "LONG":
                            if row.low <= sl:
                                sl_hit, cierre_timestamp, cierre_precio, motivo_cierre = True, ts, sl, "SL hit"
                                break
                            if row.high >= tp:
                                tp_hit, cierre_timestamp, cierre_precio, motivo_cierre = True, ts, tp, "TP hit"
                                break
                        elif senal["tipo_senal"].upper() == "SHORT":
                            if row.high >= sl:
                                sl_hit, cierre_timestamp, cierre_precio, motivo_cierre = True, ts, sl, "SL hit"
                                break
                            if row.low <= tp:
                                tp_hit, cierre_timestamp, cierre_precio, motivo_cierre = True, ts, tp, "TP hit"
                                break

                    if not (sl_hit or tp_hit) and not ohlcv.empty:
                        cierre_timestamp = ohlcv.index[-1]
                        cierre_precio = ohlcv.iloc[-1].close
                        motivo_cierre = "Cierre fin de simulación"

                    yyyy_close, mm_close, dd_close = extract_ymd(cierre_timestamp)

                    if cierre_precio is not None:
                        if senal["tipo_senal"].upper() == "LONG":
                            resultado = (cierre_precio - precio) * cantidad
                        else:
                            resultado = (precio - cierre_precio) * cantidad
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
                            dd_close = :dd_close
                        WHERE id_operacion = :id_operacion
                    """)
                    up_params = {
                        "timestamp_cierre": py_native(cierre_timestamp),
                        "precio_cierre": py_native(cierre_precio),
                        "motivo_cierre": motivo_cierre,
                        "yyyy_close": py_native(yyyy_close),
                        "mm_close": py_native(mm_close),
                        "dd_close": py_native(dd_close),
                        "id_operacion": id_operacion,
                    }
                    up_params = py_native_dict(up_params)
                    session.execute(up_op, up_params)
                    session.commit()
                    registrar_log_operacion(
                        session, "cierre", inv['id_inversionista'], senal['id_senal'], id_operacion, ticker,
                        f"Cierre de operación {id_operacion}: {motivo_cierre} en {cierre_timestamp}, precio {cierre_precio}",
                        capital_antes, capital_actual, precio, sl, tp, cantidad,
                        timestamp_apertura=senal['timestamp_senal'],
                        timestamp_cierre=cierre_timestamp,
                        motivo_cierre=motivo_cierre, resultado=resultado, precio_cierre=cierre_precio
                    )
                    # --- REMOVER OPERACIÓN DEL ARREGLO EN MEMORIA AL CERRARLA ---
                    operaciones_abiertas_mem = [
                        op for op in operaciones_abiertas_mem if op['id_operacion'] != id_operacion
                    ]

                except Exception as e:
                    session.rollback()
                    tb = traceback.format_exc()
                    error_msg = f"Error en operación: {e}\n{tb}"
                    logging.error(error_msg)
                    registrar_log_operacion(
                        session, "error", inv['id_inversionista'], senal.get('id_senal'), None, ticker,
                        error_msg, capital_antes, capital_actual, senal.get("precio_senal"), None, None, None,
                        timestamp_apertura=senal.get('timestamp_senal'),
                        motivo_no_operacion="error"
                    )

            logging.info(f"Capital final para {inv['nombre']}: {capital_actual}")

    except Exception as e:
        session.rollback()
        tb = traceback.format_exc()
        error_msg = f"Error en simulador principal: {e}\n{tb}"
        logging.error(error_msg)
        registrar_log_operacion(
            session, "error", None, None, None, None,
            error_msg, None, None, None, None, None, None,
            motivo_no_operacion="error_global"
        )

    finally:
        session.close()

if __name__ == "__main__":
    simular()

# ===============================================================================
# FIN DEL ARCHIVO: 30_simulador_trading.py
# ===============================================================================

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import parmspg
from datetime import datetime
import logging
import traceback

# ================== AJUSTA AQUÍ LAS FECHAS DE INICIO Y FIN ==================
fecha_inicio = "2025-01-01 00:00:00"
fecha_fin    = "2025-02-01 00:00:00"
# ============================================================================

# Configuración de logging de consola
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_engine():
    db_url = (
        f"postgresql+psycopg2://{parmspg.DB_USER}:{parmspg.DB_PASSWORD}"
        f"@{parmspg.DB_HOST}:{parmspg.DB_PORT}/{parmspg.DB_NAME}"
    )
    return create_engine(db_url)


def cargar_inversionistas(engine):
    query = "SELECT * FROM inversionistas"
    return pd.read_sql(query, engine)


def cargar_senales_activas(engine, tickers=None, fecha_inicio=None, fecha_fin=None):
    query = """
    SELECT * FROM senales_generadas 
    WHERE id_estado_senal IN (
        SELECT id_estado_senal FROM estados_senales WHERE permite_operar = TRUE
    )
    """
    if fecha_inicio:
        query += f" AND timestamp_senal >= '{fecha_inicio}'"
    if fecha_fin:
        query += f" AND timestamp_senal <= '{fecha_fin}'"
    if tickers:
        tickers_str = ",".join([f"'{t}'" for t in tickers])
        query += f" AND ticker_fk IN ({tickers_str})"
    query += " ORDER BY timestamp_senal ASC"
    return pd.read_sql(query, engine)


def cargar_ohlcv(engine, ticker, fecha_inicio, fecha_fin):
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


def registrar_log(session, tipo_evento, descripcion, id_operacion=None, id_senal=None, id_inversionista=None):
    try:
        ins_log = text("""
        INSERT INTO log_eventos_simulador (
            timestamp_evento, id_operacion_fk, id_senal_fk, id_inversionista_fk, tipo_evento, descripcion
        ) VALUES (
            :timestamp_evento, :id_operacion_fk, :id_senal_fk, :id_inversionista_fk, :tipo_evento, :descripcion
        )
        """)
        session.execute(ins_log, {
            "timestamp_evento": datetime.now(),
            "id_operacion_fk": id_operacion,
            "id_senal_fk": id_senal,
            "id_inversionista_fk": id_inversionista,
            "tipo_evento": tipo_evento,
            "descripcion": descripcion[:1000]  # Truncar si es muy largo
        })
        session.commit()
    except Exception as e:
        logging.error(f"Error registrando en log_eventos_simulador: {e}")


def simular():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        inversionistas = cargar_inversionistas(engine).to_dict("records")
        logging.info(f"Se cargaron {len(inversionistas)} inversionistas.")

        senales = cargar_senales_activas(engine, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)
        logging.info(f"Se cargaron {len(senales)} señales activas entre {fecha_inicio} y {fecha_fin}.")

        for inv in inversionistas:
            logging.info(f"Simulando para inversionista {inv['nombre']} (id={inv['id_inversionista']})")
            capital_actual = float(inv["capital_inicial"])
            operaciones_abiertas = []

            for _, senal in senales.iterrows():
                try:
                    if capital_actual <= 0:
                        msg = f"{inv['nombre']} sin capital disponible. Fin de simulación para este inversionista."
                        logging.warning(msg)
                        registrar_log(session, "capital_insuficiente", msg, None, None, inv["id_inversionista"])
                        break

                    riesgo_max = capital_actual * (float(inv["riesgo_max_operacion_pct"]) / 100.0)
                    precio = float(senal["precio_senal"] or 0)
                    if not precio or precio <= 0:
                        continue

                    cantidad = riesgo_max / precio
                    cantidad = max(min(cantidad, float(inv["tamano_max_operacion"] or cantidad)), float(inv["tamano_min_operacion"] or 0.0))
                    atr_pct = float(senal.get("atr_percent_usado", 0.01))
                    mult_sl = float(senal.get("mult_sl_asignado", 1.0))
                    mult_tp = float(senal.get("mult_tp_asignado", 2.0))

                    sl, tp = calcular_sl_tp(precio, atr_pct, mult_sl, mult_tp, senal["tipo_senal"])

                    ins_op = text("""
                    INSERT INTO operaciones_simuladas (
                        id_inversionista_fk, id_estrategia_fk, id_senal_fk, ticker_fk, timestamp_apertura,
                        precio_entrada, cantidad, apalancamiento, tipo_operacion, capital_riesgo_usado,
                        stop_loss_price, take_profit_price, atr_percent_usado, mult_sl_asignado, mult_tp_asignado
                    ) VALUES (
                        :id_inversionista, :id_estrategia, :id_senal, :ticker, :timestamp_apertura,
                        :precio_entrada, :cantidad, :apalancamiento, :tipo_operacion, :capital_riesgo_usado,
                        :stop_loss_price, :take_profit_price, :atr_percent_usado, :mult_sl_asignado, :mult_tp_asignado
                    ) RETURNING id_operacion
                    """)
                    op_res = session.execute(ins_op, {
                        "id_inversionista": inv["id_inversionista"],
                        "id_estrategia": senal["id_estrategia_fk"],
                        "id_senal": senal["id_senal"],
                        "ticker": senal["ticker_fk"],
                        "timestamp_apertura": senal["timestamp_senal"],
                        "precio_entrada": precio,
                        "cantidad": cantidad,
                        "apalancamiento": min(int(inv["apalancamiento_max"]), int(senal.get("apalancamiento_calculado") or 1)),
                        "tipo_operacion": senal["tipo_senal"],
                        "capital_riesgo_usado": riesgo_max,
                        "stop_loss_price": sl,
                        "take_profit_price": tp,
                        "atr_percent_usado": atr_pct,
                        "mult_sl_asignado": mult_sl,
                        "mult_tp_asignado": mult_tp,
                    })
                    id_operacion = op_res.fetchone()[0]
                    session.commit()
                    logging.info(f"Operación registrada para {inv['nombre']}: Señal {senal['id_senal']}, operación {id_operacion}")
                    registrar_log(session, "apertura", f"Operación abierta (SL:{sl}, TP:{tp})", id_operacion, senal["id_senal"], inv["id_inversionista"])

                    # Monitoreo de operación
                    ohlcv = cargar_ohlcv(
                        engine,
                        senal["ticker_fk"],
                        senal["timestamp_senal"],
                        fecha_fin
                    )
                    sl_hit = tp_hit = False
                    cierre_timestamp = cierre_precio = motivo_cierre = None
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

                    if not (sl_hit or tp_hit):
                        cierre_timestamp = ohlcv.index[-1]
                        cierre_precio = ohlcv.iloc[-1].close
                        motivo_cierre = "Cierre fin de simulación"

                    up_op = text("""
                        UPDATE operaciones_simuladas
                        SET timestamp_cierre = :timestamp_cierre,
                            precio_cierre = :precio_cierre,
                            resultado = (:precio_cierre - precio_entrada) * cantidad * 
                                (CASE WHEN tipo_operacion = 'LONG' THEN 1 ELSE -1 END),
                            motivo_cierre = :motivo_cierre
                        WHERE id_operacion = :id_operacion
                    """)
                    session.execute(up_op, {
                        "timestamp_cierre": cierre_timestamp,
                        "precio_cierre": cierre_precio,
                        "motivo_cierre": motivo_cierre,
                        "id_operacion": id_operacion,
                    })
                    session.commit()
                    msg_cierre = f"Cierre de operación {id_operacion}: {motivo_cierre} en {cierre_timestamp}, precio {cierre_precio}"
                    logging.info(msg_cierre)
                    registrar_log(session, "cierre", msg_cierre, id_operacion, senal["id_senal"], inv["id_inversionista"])

                    # Actualiza capital (simplificado)
                    if senal["tipo_senal"].upper() == "LONG":
                        capital_actual += (cierre_precio - precio) * cantidad
                    else:
                        capital_actual += (precio - cierre_precio) * cantidad

                except Exception as e:
                    session.rollback()
                    tb = traceback.format_exc()
                    error_msg = f"Error en operación: {e}\n{tb}"
                    logging.error(error_msg)
                    registrar_log(session, "error", error_msg, None, senal["id_senal"], inv["id_inversionista"])

            logging.info(f"Capital final para {inv['nombre']}: {capital_actual}")

    except Exception as e:
        session.rollback()
        tb = traceback.format_exc()
        error_msg = f"Error en simulador principal: {e}\n{tb}"
        logging.error(error_msg)
        # Registrar error global (no ligado a operación/señal)
        registrar_log(session, "error", error_msg, None, None, None)

    finally:
        session.close()


if __name__ == "__main__":
    simular()
# ===========================================================
# Script: 20_pg_gestor_estrategias.py
# Resumen: 
#   Motor batch para ejecutar estrategias de trading almacenadas en BD,
#   evaluando reglas sobre datos históricos y generando señales.
#   Señales viables se insertan en senales_generadas,
#   señales no viables en senales_no_viables.
#   Permite delimitar por fecha y recorre la tabla INDICADORES (5min) como guía temporal.
# Versión: 1.2.5   (2025-06-10)
# ===========================================================

import sys
import os
import datetime
import logging
import operator
import importlib.util
from decimal import Decimal

from sqlalchemy import create_engine, MetaData, Table, select, and_
from sqlalchemy.orm import sessionmaker

def load_db_params():
    parmspg_path = os.path.join(os.path.dirname(__file__), 'parmspg.py')
    spec = importlib.util.spec_from_file_location("parmspg", parmspg_path)
    parmspg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parmspg)
    return parmspg

def get_engine(parmspg):
    user = parmspg.DB_USER
    password = parmspg.DB_PASSWORD
    host = parmspg.DB_HOST
    port = parmspg.DB_PORT
    db = parmspg.DB_NAME
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

SCRIPT_VERSION = "1.2.5"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

PY_OPERATORS = {
    'eq': operator.eq,
    'ne': operator.ne,
    'lt': operator.lt,
    'le': operator.le,
    'gt': operator.gt,
    'ge': operator.ge,
    'between': lambda x, a, b: a <= x <= b,
    'not_between': lambda x, a, b: not (a <= x <= b),
    'in_': lambda x, lista: x in lista,
    'not_in': lambda x, lista: x not in lista,
}

def parse_value(val, typ):
    if val is None:
        return None
    if typ == 'numeric':
        try:
            return float(val)
        except Exception:
            return None
    return val

def string_to_list(s):
    if s is None:
        return []
    return [x.strip() for x in s.split(',') if x.strip()]

def cargar_operadores(conn, metadata):
    operadores_tbl = Table('operadores', metadata, autoload_with=conn)
    result = conn.execute(select(operadores_tbl.c.operador, operadores_tbl.c.operador_python))
    mapping = {}
    for row in result.mappings():
        mapping[row['operador']] = row['operador_python']
    return mapping

def cargar_estados_senales(conn, metadata):
    estados_tbl = Table('estados_senales', metadata, autoload_with=conn)
    result = conn.execute(select(estados_tbl))
    return {
        row['estado_senal']: row['id_estado_senal']
        for row in result.mappings()
    }

def cargar_estrategias_activas(conn, metadata):
    estrategias_tbl = Table('estrategias', metadata, autoload_with=conn)
    result = conn.execute(select(estrategias_tbl).where(estrategias_tbl.c.activa == True))
    return [row for row in result.mappings()]

def cargar_reglas(conn, metadata, tabla, id_estrategia):
    reglas_tbl = Table(tabla, metadata, autoload_with=conn)
    result = conn.execute(select(reglas_tbl).where(and_(
        reglas_tbl.c.id_estrategia_fk == id_estrategia,
        reglas_tbl.c.activo == True
    )).order_by(reglas_tbl.c.orden))
    return [row for row in result.mappings()]

def obtener_funcion_operador(operador_python):
    if operador_python in PY_OPERATORS:
        return PY_OPERATORS[operador_python]
    raise NotImplementedError(f"Operador Python '{operador_python}' no implementado en mapeo.")

def evaluar_regla(regla, data_valor, op_python_map):
    if data_valor is None:
        logger.debug(f"[Regla: {regla.get('codigo_regla','')}]: Valor a evaluar es None, se considera NO CUMPLIDA.")
        return False
    operador = regla['operador']
    operador_python = op_python_map.get(operador)
    if not operador_python:
        logger.error(f"Operador {operador} no encontrado en tabla operadores.")
        return False
    func = obtener_funcion_operador(operador_python)
    val1 = regla.get('valor_comparacion_1')
    val2 = regla.get('valor_comparacion_2')
    try:
        if operador_python in ('between', 'not_between'):
            return func(data_valor, float(val1), float(val2))
        elif operador_python in ('in_', 'not_in'):
            return func(data_valor, string_to_list(val1))
        else:
            if val1 is not None:
                try:
                    return func(data_valor, float(val1))
                except Exception:
                    return func(data_valor, val1)
            else:
                return func(data_valor)
    except Exception as ex:
        logger.debug(f"[Regla: {regla.get('codigo_regla','')}] Error en evaluación: {ex}")
        return False

def main():
    fecha_inicio = datetime.datetime(2025, 1, 1, 0, 0, 0)
    fecha_fin    = datetime.datetime(2025, 6, 10, 0, 0, 0)
    timeframe_base = 5
    tickers_filtrar = None

    logger.info(f"INICIO DE SCRIPT v{SCRIPT_VERSION}")
    parmspg = load_db_params()
    engine = get_engine(parmspg)
    metadata = MetaData()
    Session = sessionmaker(bind=engine)

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            logger.info("Cargando metadatos...")
            operadores_map = cargar_operadores(conn, metadata)
            op_python_map = {op: operadores_map[op] for op in operadores_map if operadores_map[op] in PY_OPERATORS}
            estados_senales = cargar_estados_senales(conn, metadata)
            estrategias = cargar_estrategias_activas(conn, metadata)
            logger.info(f"Se encontraron {len(estrategias)} estrategias activas.")

            for estrategia in estrategias:
                id_estrategia = estrategia['id_estrategia']
                logger.info(f"Procesando estrategia: {estrategia['nombre_estrategia']} (ID {id_estrategia})")
                reglas_resumen = cargar_reglas(conn, metadata, 'estrategia_reglas_resumen', id_estrategia)
                reglas_alertas = cargar_reglas(conn, metadata, 'estrategia_reglas_alertas', id_estrategia)
                reglas_indicadores = cargar_reglas(conn, metadata, 'estrategia_reglas_indicadores', id_estrategia)

                tickers = get_all_tickers_indicadores(conn, metadata, timeframe_base) if not tickers_filtrar else tickers_filtrar
                for ticker in tickers:
                    timestamps = get_timestamps_for_ticker_indicadores(
                        conn, metadata, ticker, timeframe_base, fecha_inicio, fecha_fin
                    )
                    for ts in timestamps:
                        try:
                            resultado, es_viable, motivo_no_viable, detalle_no_viable = procesar_estrategia_sobre_timestamp(
                                conn, metadata, estrategia, reglas_resumen, reglas_alertas, reglas_indicadores,
                                ticker, ts, op_python_map, estados_senales
                            )
                            if resultado:
                                if es_viable:
                                    insertar_senal(conn, metadata, resultado)
                                else:
                                    insertar_senal_no_viable(conn, metadata, resultado, motivo_no_viable, detalle_no_viable)
                        except Exception as ex:
                            logger.error(f"Error procesando señal para {ticker} ts={ts}: {ex}")
            trans.commit()
            logger.info(f"FIN DE SCRIPT v{SCRIPT_VERSION} - Proceso completado.")
        except Exception as ex:
            trans.rollback()
            logger.error(f"Transacción revertida por error crítico: {ex}")
            sys.exit(1)

def get_all_tickers_indicadores(conn, metadata, timeframe):
    indicadores_tbl = Table('indicadores', metadata, autoload_with=conn)
    result = conn.execute(
        select(indicadores_tbl.c.ticker).where(indicadores_tbl.c.timeframe == timeframe).distinct()
    )
    return [row['ticker'] for row in result.mappings()]

def get_timestamps_for_ticker_indicadores(conn, metadata, ticker, timeframe, fecha_inicio, fecha_fin):
    indicadores_tbl = Table('indicadores', metadata, autoload_with=conn)
    result = conn.execute(
        select(indicadores_tbl.c.timestamp).where(
            and_(
                indicadores_tbl.c.ticker == ticker,
                indicadores_tbl.c.timeframe == timeframe,
                indicadores_tbl.c.timestamp >= fecha_inicio,
                indicadores_tbl.c.timestamp <= fecha_fin
            )
        ).order_by(indicadores_tbl.c.timestamp)
    )
    return [row['timestamp'] for row in result.mappings()]

def procesar_estrategia_sobre_timestamp(
    conn, metadata, estrategia, reglas_resumen, reglas_alertas, reglas_indicadores,
    ticker, ts, op_python_map, estados_senales
):
    puntos_totales = 0.0
    puntos_obtenidos = 0.0
    reglas_cumplidas = []
    indispensables_fallidas = []
    indispensable_fallida = False

    resumen_alertas_tbl = Table('resumen_alertas', metadata, autoload_with=conn)
    resumen_row = conn.execute(
        select(resumen_alertas_tbl).where(
            and_(
                resumen_alertas_tbl.c.ticker == ticker,
                resumen_alertas_tbl.c.timestamp == ts
            )
        )
    ).mappings().first()
    resumen_dict = resumen_row if resumen_row else {}

    for regla in reglas_resumen:
        campo = regla['campo_resumen']
        puntos_totales += float(regla['puntos_si_cumple'])
        valor_actual = resumen_dict.get(campo)
        cumple = evaluar_regla(regla, valor_actual, op_python_map)
        if cumple:
            puntos_obtenidos += float(regla['puntos_si_cumple'])
            reglas_cumplidas.append(regla['codigo_regla'])
        elif regla.get('indispensable', False):
            indispensable_fallida = True
            indispensables_fallidas.append(regla['codigo_regla'])

    alertas_generadas_tbl = Table('alertas_generadas', metadata, autoload_with=conn)
    for regla in reglas_alertas:
        filtro = [
            alertas_generadas_tbl.c.ticker == ticker,
            alertas_generadas_tbl.c.timestamp_alerta == ts,
            alertas_generadas_tbl.c.timeframe == regla['timeframe'],
            alertas_generadas_tbl.c.id_criterio_fk == regla['id_criterio_fk']
        ]
        alerta_row = conn.execute(
            select(alertas_generadas_tbl).where(and_(*filtro))
        ).mappings().first()
        alerta_dict = alerta_row if alerta_row else {}
        campo = regla['campo_verificar']
        valor_actual = alerta_dict.get(campo) if campo else None
        cumple = evaluar_regla(regla, valor_actual, op_python_map)
        puntos_totales += float(regla['puntos_si_cumple'])
        if cumple:
            puntos_obtenidos += float(regla['puntos_si_cumple'])
            reglas_cumplidas.append(regla['codigo_regla'])
        elif regla.get('indispensable', False):
            indispensable_fallida = True
            indispensables_fallidas.append(regla['codigo_regla'])

    indicadores_tbl = Table('indicadores', metadata, autoload_with=conn)
    ind_row = conn.execute(
        select(indicadores_tbl).where(
            and_(
                indicadores_tbl.c.ticker == ticker,
                indicadores_tbl.c.timestamp == ts,
                indicadores_tbl.c.timeframe == estrategia['primary_timeframe']
            )
        )
    ).mappings().first()
    ind_dict = ind_row if ind_row else {}

    for regla in reglas_indicadores:
        campo = regla['campo_evaluar']
        valor_actual = ind_dict.get(campo)
        cumple = evaluar_regla(regla, valor_actual, op_python_map)
        puntos_totales += float(regla['puntos_si_cumple'])
        if cumple:
            puntos_obtenidos += float(regla['puntos_si_cumple'])
            reglas_cumplidas.append(regla['codigo_regla'])
        elif regla.get('indispensable', False):
            indispensable_fallida = True
            indispensables_fallidas.append(regla['codigo_regla'])

    calificacion_pct = 0.0
    if puntos_totales > 0:
        calificacion_pct = round((puntos_obtenidos / puntos_totales) * 100, 2)
    umbral = float(estrategia.get('umbral_calificacion_minimo', 0.0))
    id_estado_senal = None
    motivo_no_viable = ""
    detalle_no_viable = {}

    if indispensable_fallida:
        id_estado_senal = 2
        motivo_no_viable = "No cumple regla indispensable"
        detalle_no_viable = {
            "indispensables_fallidas": indispensables_fallidas,
            "puntos_obtenidos": puntos_obtenidos,
            "puntos_totales": puntos_totales,
            "calificacion_pct": calificacion_pct
        }
    elif calificacion_pct < umbral:
        id_estado_senal = 3
        motivo_no_viable = "Calificación insuficiente"
        detalle_no_viable = {
            "umbral_min": umbral,
            "puntos_obtenidos": puntos_obtenidos,
            "puntos_totales": puntos_totales,
            "calificacion_pct": calificacion_pct
        }
    else:
        id_estado_senal = 1

    precio_senal = ind_dict.get('precio_actual') if ind_dict else None
    atr_percent_usado = ind_dict.get('atr_percent') if ind_dict else None

    yyyy, mm, dd, hh, min_ = None, None, None, None, None
    if ts is not None:
        yyyy = ts.year
        mm = ts.month
        dd = ts.day
        hh = ts.hour
        min_ = ts.minute

    senal = {
        'id_estrategia_fk': estrategia['id_estrategia'],
        'ticker_fk': ticker,
        'timestamp_senal': ts,
        'tipo_senal': estrategia['tipo_operacion'] or '',
        'calificacion_pct': calificacion_pct,
        'indispensable_fallida': indispensable_fallida,
        'precio_senal': precio_senal,
        'target_profit_price': None,
        'stop_loss_price': None,
        'reglas_cumplidas_codigos': ','.join(reglas_cumplidas) if reglas_cumplidas else None,
        'reglas_indispensables_fallidas_codigos': ','.join(indispensables_fallidas) if indispensables_fallidas else None,
        'fecha_registro': datetime.datetime.now(),
        'version': 1,
        'apalancamiento_calculado': estrategia.get('lim_inf_apalancamiento', 1),
        'mult_sl_asignado': estrategia.get('sl_atr_mult', 1.0),
        'mult_tp_asignado': estrategia.get('tp_atr_mult', 1.0),
        'atr_percent_usado': atr_percent_usado,
        'yyyy': yyyy,
        'mm': mm,
        'dd': dd,
        'hh': hh,
        'min': min_,
        'id_estado_senal': id_estado_senal,
        'comentarios': None  # Se mantiene para compatibilidad SOLO en senales_generadas
    }
    senal = convert_decimals_to_floats(senal)
    es_viable = (id_estado_senal == 1)
    return senal, es_viable, motivo_no_viable, detalle_no_viable

def convert_decimals_to_floats(d):
    for k, v in d.items():
        if isinstance(v, Decimal):
            d[k] = float(v)
    return d

def insertar_senal(conn, metadata, senal):
    senales_tbl = Table('senales_generadas', metadata, autoload_with=conn)
    try:
        insert_stmt = senales_tbl.insert().values(**senal)
        result = conn.execute(insert_stmt)
        if hasattr(result, "rowcount") and result.rowcount and result.rowcount > 0:
            logger.info(
                f"Se insertó señal VIABLE en senales_generadas para estrategia {senal['id_estrategia_fk']}, ticker {senal['ticker_fk']}, ts {senal['timestamp_senal']} (estado {senal['id_estado_senal']})"
            )
        else:
            logger.warning(
                f"No se insertó señal VIABLE para estrategia {senal['id_estrategia_fk']}, ticker {senal['ticker_fk']}, ts {senal['timestamp_senal']}."
            )
    except Exception as ex:
        logger.error(f"Error insertando señal viable: {ex}")
        logger.error(f"Datos que fallaron: {repr(senal)}")
        try:
            conn.rollback()
        except Exception as rb_ex:
            logger.error(f"Error al hacer rollback tras fallo de insert VIABLE: {rb_ex}")

def insertar_senal_no_viable(conn, metadata, senal, motivo_no_viable, detalle_no_viable):
    senales_no_viables_tbl = Table('senales_no_viables', metadata, autoload_with=conn)
    senal_nv = dict(senal)
    senal_nv['motivo_no_viable'] = motivo_no_viable
    senal_nv['detalle_evaluacion'] = detalle_no_viable if detalle_no_viable else None
    # Elimina el campo 'comentarios' si existe, para evitar el error "Unconsumed column names: comentarios"
    if 'comentarios' in senal_nv:
        del senal_nv['comentarios']
    senal_nv = convert_decimals_to_floats(senal_nv)
    try:
        insert_stmt = senales_no_viables_tbl.insert().values(**senal_nv)
        result = conn.execute(insert_stmt)
        if hasattr(result, "rowcount") and result.rowcount and result.rowcount > 0:
            logger.info(
                f"Se insertó señal NO VIABLE en senales_no_viables para estrategia {senal['id_estrategia_fk']}, ticker {senal['ticker_fk']}, ts {senal['timestamp_senal']} (estado {senal['id_estado_senal']}) -- Motivo: {motivo_no_viable}"
            )
        else:
            logger.warning(
                f"No se insertó señal NO VIABLE para estrategia {senal['id_estrategia_fk']}, ticker {senal['ticker_fk']}, ts {senal['timestamp_senal']}."
            )
    except Exception as ex:
        logger.error(f"Error insertando señal no viable: {ex}")
        logger.error(f"Datos que fallaron: {repr(senal_nv)}")
        try:
            conn.rollback()
        except Exception as rb_ex:
            logger.error(f"Error al hacer rollback tras fallo de insert NO VIABLE: {rb_ex}")

if __name__ == '__main__':
    main()

# ===========================================================
# Script: 20_pg_gestor_estrategias.py
# Resumen: Motor batch para ejecutar estrategias de trading sobre datos históricos,
#          evaluando reglas y generando señales en la tabla senales_generadas (viables)
#          y senales_no_viables (descartadas/no operables).
# Versión: 1.2.5   (2025-06-10)
# ===========================================================
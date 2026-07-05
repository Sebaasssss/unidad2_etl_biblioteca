"""
ETL - Prestamos de Biblioteca
Autor: Sebastian
Descripcion: Lee data/prestamos_biblioteca_100.csv, limpia, valida,
carga a un mini Data Warehouse en MySQL (biblioteca_dw) y genera
evidencias/reporte_ejecucion.txt
Ejecucion: python scripts/etl_biblioteca.py
"""

import os
import sys
import pandas as pd
import mysql.connector
from datetime import datetime

# -----------------------------
# CONFIGURACION
# -----------------------------
NOMBRE_ALUMNO = "Sebastian Jimenez"  

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",       
    "database": "biblioteca_dw",
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "prestamos_biblioteca_100.csv")
REPORTE_PATH = os.path.join(BASE_DIR, "evidencias", "reporte_ejecucion.txt")

COLUMNAS_OBLIGATORIAS = [
    "id_prestamo", "fecha_prestamo", "alumno", "carrera",
    "libro", "categoria", "dias_prestamo", "multa_diaria",
    "sede", "total_multa",
]


# -----------------------------
# PARTE 1: LECTURA Y LIMPIEZA
# -----------------------------
def leer_y_limpiar(csv_path):
    df = pd.read_csv(csv_path)

    # 1. Estandarizar nombres de columnas
    df.columns = [c.strip().lower() for c in df.columns]

    # 2. Quitar espacios en columnas de texto
    columnas_texto = ["alumno", "carrera", "libro", "categoria", "sede"]
    for col in columnas_texto:
        df[col] = df[col].astype(str).str.strip()

    # 3. Convertir fecha
    df["fecha_prestamo"] = pd.to_datetime(df["fecha_prestamo"], errors="coerce")

    # 4. Convertir numericos
    for col in ["dias_prestamo", "multa_diaria", "total_multa"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5. Verificar nulos en columnas obligatorias
    nulos = df[COLUMNAS_OBLIGATORIAS].isnull().any().any()
    if nulos:
        print("ADVERTENCIA: hay valores nulos en columnas obligatorias.")

    return df


# -----------------------------
# PARTE 4: VALIDACION
# -----------------------------
def validar(df):
    """
    Devuelve (validos_df, errores_list)
    No corrige nada automaticamente. Solo detecta y separa.
    """
    validos = []
    errores = []
    ids_vistos = set()

    for idx, row in df.iterrows():
        fila_csv = idx + 2  # +2 porque idx empieza en 0 y hay encabezado
        id_prestamo = row["id_prestamo"]

        # Regla 1: duplicado -> se conserva la 1ra aparicion, se rechaza la 2da
        if id_prestamo in ids_vistos:
            errores.append({
                "fila_csv": fila_csv,
                "id_registro": id_prestamo,
                "descripcion_error": "id_prestamo duplicado",
                "datos_originales": row.to_dict(),
            })
            continue

        ids_vistos.add(id_prestamo)

        # Regla 2: total_multa incorrecto
        esperado = row["dias_prestamo"] * row["multa_diaria"]
        if esperado != row["total_multa"]:
            errores.append({
                "fila_csv": fila_csv,
                "id_registro": id_prestamo,
                "descripcion_error": "total_multa incorrecto",
                "datos_originales": row.to_dict(),
            })
            continue

        validos.append(row)

    validos_df = pd.DataFrame(validos)
    return validos_df, errores


# -----------------------------
# PARTE 2 y 3: BASE DE DATOS
# -----------------------------
def crear_tablas(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_alumno (
            id_alumno INT AUTO_INCREMENT PRIMARY KEY,
            alumno VARCHAR(150) UNIQUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_carrera (
            id_carrera INT AUTO_INCREMENT PRIMARY KEY,
            carrera VARCHAR(100) UNIQUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_libro (
            id_libro INT AUTO_INCREMENT PRIMARY KEY,
            libro VARCHAR(150),
            categoria VARCHAR(100),
            UNIQUE KEY uq_libro (libro, categoria)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_sede (
            id_sede INT AUTO_INCREMENT PRIMARY KEY,
            sede VARCHAR(100) UNIQUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_fecha (
            id_fecha INT AUTO_INCREMENT PRIMARY KEY,
            fecha DATE UNIQUE,
            anio INT,
            mes INT,
            dia INT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_prestamos (
            id_prestamo INT PRIMARY KEY,
            id_fecha INT,
            id_alumno INT,
            id_carrera INT,
            id_libro INT,
            id_sede INT,
            dias_prestamo INT,
            multa_diaria DECIMAL(10,2),
            total_multa DECIMAL(10,2),
            FOREIGN KEY (id_fecha) REFERENCES dim_fecha(id_fecha),
            FOREIGN KEY (id_alumno) REFERENCES dim_alumno(id_alumno),
            FOREIGN KEY (id_carrera) REFERENCES dim_carrera(id_carrera),
            FOREIGN KEY (id_libro) REFERENCES dim_libro(id_libro),
            FOREIGN KEY (id_sede) REFERENCES dim_sede(id_sede)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_errores (
            id_error INT AUTO_INCREMENT PRIMARY KEY,
            fecha_error DATETIME,
            archivo_origen VARCHAR(255),
            fila_csv INT,
            id_registro INT,
            descripcion_error VARCHAR(255),
            datos_originales TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id_log INT AUTO_INCREMENT PRIMARY KEY,
            fecha_ejecucion DATETIME,
            archivo_origen VARCHAR(255),
            filas_leidas INT,
            filas_cargadas INT,
            filas_rechazadas INT,
            estado VARCHAR(50)
        )
    """)


def limpiar_tablas(cursor):
    # Orden importante por llaves foraneas
    tablas = [
        "fact_prestamos", "dim_alumno", "dim_carrera",
        "dim_libro", "dim_sede", "dim_fecha", "etl_errores",
    ]
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    for t in tablas:
        cursor.execute(f"TRUNCATE TABLE {t}")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")


def obtener_o_crear_id(cursor, tabla, columna_pk, columnas_dict):
    """
    Busca si ya existe el registro por sus columnas, si no, lo inserta.
    columnas_dict: {columna: valor}
    """
    cols = list(columnas_dict.keys())
    vals = list(columnas_dict.values())
    where_clause = " AND ".join([f"{c}=%s" for c in cols])

    cursor.execute(f"SELECT {columna_pk} FROM {tabla} WHERE {where_clause}", vals)
    resultado = cursor.fetchone()
    if resultado:
        return resultado[0]

    cols_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    cursor.execute(f"INSERT INTO {tabla} ({cols_str}) VALUES ({placeholders})", vals)
    return cursor.lastrowid


def cargar_validos(cursor, validos_df):
    for _, row in validos_df.iterrows():
        id_alumno = obtener_o_crear_id(cursor, "dim_alumno", "id_alumno", {"alumno": row["alumno"]})
        id_carrera = obtener_o_crear_id(cursor, "dim_carrera", "id_carrera", {"carrera": row["carrera"]})
        id_libro = obtener_o_crear_id(cursor, "dim_libro", "id_libro", {"libro": row["libro"], "categoria": row["categoria"]})
        id_sede = obtener_o_crear_id(cursor, "dim_sede", "id_sede", {"sede": row["sede"]})

        fecha = row["fecha_prestamo"]
        id_fecha = obtener_o_crear_id(cursor, "dim_fecha", "id_fecha", {
            "fecha": fecha.date(),
            "anio": fecha.year,
            "mes": fecha.month,
            "dia": fecha.day,
        })

        cursor.execute("""
            INSERT INTO fact_prestamos
            (id_prestamo, id_fecha, id_alumno, id_carrera, id_libro, id_sede,
             dias_prestamo, multa_diaria, total_multa)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row["id_prestamo"]), id_fecha, id_alumno, id_carrera, id_libro, id_sede,
            int(row["dias_prestamo"]), float(row["multa_diaria"]), float(row["total_multa"]),
        ))


def registrar_errores(cursor, errores, archivo_origen):
    ahora = datetime.now()
    for e in errores:
        cursor.execute("""
            INSERT INTO etl_errores
            (fecha_error, archivo_origen, fila_csv, id_registro, descripcion_error, datos_originales)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            ahora, archivo_origen, e["fila_csv"], int(e["id_registro"]),
            e["descripcion_error"], str(e["datos_originales"]),
        ))


def registrar_log(cursor, archivo_origen, filas_leidas, filas_cargadas, filas_rechazadas, estado):
    cursor.execute("""
        INSERT INTO etl_log
        (fecha_ejecucion, archivo_origen, filas_leidas, filas_cargadas, filas_rechazadas, estado)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (datetime.now(), archivo_origen, filas_leidas, filas_cargadas, filas_rechazadas, estado))


# -----------------------------
# PARTE 6: REPORTE DE EJECUCION
# -----------------------------
def generar_reporte(alumno, archivo_origen, filas_leidas, filas_cargadas, filas_rechazadas, estado, errores):
    with open(REPORTE_PATH, "w", encoding="utf-8") as f:
        f.write("REPORTE DE EJECUCION - ETL PRESTAMOS BIBLIOTECA\n")
        f.write("=" * 50 + "\n")
        f.write(f"Nombre del alumno: {alumno}\n")
        f.write(f"Fecha y hora de ejecucion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Archivo procesado: {archivo_origen}\n")
        f.write(f"Filas leidas: {filas_leidas}\n")
        f.write(f"Filas cargadas: {filas_cargadas}\n")
        f.write(f"Filas rechazadas: {filas_rechazadas}\n")
        f.write(f"Estado final: {estado}\n")
        f.write("\nErrores detectados:\n")
        for e in errores:
            f.write(f"  - id_prestamo {e['id_registro']}: {e['descripcion_error']} (fila CSV {e['fila_csv']})\n")


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("Iniciando ETL de prestamos de biblioteca...")

    if not os.path.exists(CSV_PATH):
        print(f"ERROR: no se encontro el archivo {CSV_PATH}")
        sys.exit(1)

    df = leer_y_limpiar(CSV_PATH)
    filas_leidas = len(df)

    validos_df, errores = validar(df)
    filas_cargadas = len(validos_df)
    filas_rechazadas = len(errores)
    estado = "FINALIZADO_CON_ERRORES" if filas_rechazadas > 0 else "FINALIZADO_OK"

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        crear_tablas(cursor)
        conn.commit()

        limpiar_tablas(cursor)
        conn.commit()

        cargar_validos(cursor, validos_df)
        registrar_errores(cursor, errores, "prestamos_biblioteca_100.csv")
        registrar_log(cursor, "prestamos_biblioteca_100.csv", filas_leidas, filas_cargadas, filas_rechazadas, estado)

        conn.commit()
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"ERROR de MySQL: {err}")
        sys.exit(1)

    generar_reporte(
        NOMBRE_ALUMNO, "prestamos_biblioteca_100.csv",
        filas_leidas, filas_cargadas, filas_rechazadas, estado, errores,
    )

    print(f"Filas leidas: {filas_leidas}")
    print(f"Filas cargadas: {filas_cargadas}")
    print(f"Filas rechazadas: {filas_rechazadas}")
    print(f"Estado: {estado}")
    print("Reporte generado en evidencias/reporte_ejecucion.txt")


if __name__ == "__main__":
    main()

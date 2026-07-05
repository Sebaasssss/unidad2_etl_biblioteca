# Unidad 2 - ETL Prestamos de Biblioteca

## 1. Objetivo del proyecto
Construir un mini proyecto ETL completo: leer un dataset CSV de prestamos de
biblioteca, limpiarlo, validarlo, cargarlo a un mini Data Warehouse en MySQL
(`biblioteca_dw`) y generar evidencias de la ejecucion.

## 2. Requisitos para ejecutarlo
- Python 3.9+
- MySQL Server 8.x (o MariaDB compatible)
- Librerias: `pandas`, `mysql-connector-python`

Instalar librerias:
```
pip install pandas mysql-connector-python
```

## 3. Como crear la base de datos
Desde MySQL, DataGrip o consola:
```sql
CREATE DATABASE biblioteca_dw;
```
El script crea las tablas automaticamente si no existen, no hace falta
crearlas a mano.

## 4. Como instalar librerias
```
pip install pandas mysql-connector-python
```

## 5. Como ejecutar el script
1. Ajusta usuario/password en `DB_CONFIG` dentro de `scripts/etl_biblioteca.py`.
2. Ejecuta desde la raiz del repositorio:
```
python scripts/etl_biblioteca.py
```
3. El script puede ejecutarse mas de una vez: limpia las tablas antes de
   volver a cargar, para no duplicar datos.

## 6. Resultado esperado
```
Filas leidas: 100
Filas cargadas: 98
Filas rechazadas: 2
Estado: FINALIZADO_CON_ERRORES
```

Errores esperados:
- id_prestamo 5099: total_multa incorrecto
- id_prestamo 5002: id_prestamo duplicado (se conserva la 1a aparicion)

Al finalizar se genera `evidencias/reporte_ejecucion.txt` con el resumen
de la ejecucion.

## Estructura del repositorio
```
unidad2_etl_biblioteca/
|-- data/
|   |-- prestamos_biblioteca_100.csv
|-- scripts/
|   |-- etl_biblioteca.py
|-- sql/
|   |-- consultas_verificacion.sql
|-- evidencias/
|   |-- evidencias_unidad2.pdf
|   |-- reporte_ejecucion.txt
|-- README.md
```

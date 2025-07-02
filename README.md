# etl-iaenp-airflow-gcp
# ETL IAENP con Apache Airflow y Google BigQuery

Este repositorio contiene la definición de un pipeline ETL utilizando Apache Airflow para extraer, transformar y cargar datos del dataset IAENP (Indicadores de Actividad Económica No Petrolera) del SRI en un Data Warehouse en Google BigQuery.

## Tecnologías utilizadas

- Python 3.12
- Apache Airflow 3.0.2 (vía Docker)
- Google Cloud Storage (GCS)
- Google BigQuery
- Pandas
- Google Cloud SDK

## Estructura del proyecto
etl-iaenp-airflow-gcp/

├── dags/

│ └── etl_iaenp_dag.py # DAG de Airflow con tareas ETL

├── scripts/

│ └── transformaciones_auxiliares.py # Script auxiliar para pruebas locales (opcional)

├── gcs/

│ └── SRI_IAENP_Mensual.csv # Dataset original usado para la práctica

└── README.md

## Descripción del flujo ETL

- **Extracción:** Descarga del archivo CSV desde GCS.
- **Transformación:** Limpieza, normalización de fechas, generación de claves para dimensiones.
- **Carga:** Inserción en BigQuery de las tablas `dim_tiempo`, `dim_sector` y `fact_iaenp`.

## Dataset

El dataset proviene de datos abiertos del SRI, con indicadores mensuales no petroleros de actividad económica.

- Archivo: `SRI_IAENP_Mensual.csv`
- Ubicación original: Bucket de GCS configurado previamente.

## Requisitos previos

- Tener una cuenta de Google Cloud con un bucket y dataset en BigQuery configurados.
- Haber creado una cuenta de servicio y montado su clave `.json` en el contenedor de Airflow.

## Cómo ejecutar el DAG

1. Inicia los contenedores:
   ```bash
   docker compose up -d
2. Abre Airflow en http://localhost:8080 y activa el DAG etl_iaenp.

3. Ejecuta manualmente el DAG para iniciar el proceso ETL completo.

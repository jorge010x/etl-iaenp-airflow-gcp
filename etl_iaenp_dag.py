from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
from google.cloud import storage, bigquery
import os

#Extraccion del dataset fuente desde GCS
def download_csv_from_gcs(bucket_name, source_blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

#Carga de la dimension tiempo
def load_dim_tiempo(ds=None, **kwargs):
    download_csv_from_gcs("bucket_etl_iaenp", "SRI_IAENP_Mensual.csv", "/tmp/iaenp.csv")
    df = pd.read_csv("/tmp/iaenp.csv", sep=";")
    df.columns = df.columns.str.strip()

    df_dim_tiempo = df[["anio", "mes"]].drop_duplicates().copy()
    df_dim_tiempo["anio"] = df_dim_tiempo["anio"].astype(int)
    df_dim_tiempo["mes"] = df_dim_tiempo["mes"].astype(int)
    df_dim_tiempo["trimestre"] = ((df_dim_tiempo["mes"] - 1) // 3 + 1).astype(str)
    df_dim_tiempo["nombre_mes"] = df_dim_tiempo["mes"].apply(
        lambda x: pd.to_datetime(str(x), format='%m').strftime('%B')
    )

    # Crear columna id_tiempo como concatenación "yyyymmdd" con día fijo "01"
    df_dim_tiempo["id_tiempo"] = (
        df_dim_tiempo["anio"].astype(str).str.zfill(4) +
        df_dim_tiempo["mes"].astype(str).str.zfill(2) +
        "01"
    ).astype(int)

    client = bigquery.Client()
    table_id = "solar-curve-464603-h7.dw_iaenp.dim_tiempo"
    client.load_table_from_dataframe(df_dim_tiempo, table_id).result()

#Carga de la dimension sector
def load_dim_sector(ds=None, **kwargs):
    download_csv_from_gcs("bucket_etl_iaenp", "SRI_IAENP_Mensual.csv", "/tmp/iaenp.csv")
    df = pd.read_csv("/tmp/iaenp.csv", sep=";")
    sectores = [
        "indice_manufacturas_mensual",
        "indice_comercio_mensual",
        "indice_construccion_mensual",
        "indice_servicios_mensual",
        "indice_de_actividad_empresarial_no_petrolera_total_mensual"
    ]
    
    nombres = [
        "Manufacturas",
        "Comercio",
        "Construcción",
        "Servicios",
        "Total AENP"
    ]
    
    df_dim_sector = pd.DataFrame({
        "id_sector": list(range(1, len(sectores) + 1)),
        "nombre_sector": nombres
    })

    client = bigquery.Client()
    table_id = "solar-curve-464603-h7.dw_iaenp.dim_sector"
    client.load_table_from_dataframe(df_dim_sector, table_id).result()

# Carga de la tabla de hechos
def load_fact_iaenp(ds=None, **kwargs):
    download_csv_from_gcs("bucket_etl_iaenp", "SRI_IAENP_Mensual.csv", "/tmp/iaenp.csv")
    df = pd.read_csv("/tmp/iaenp.csv", sep=";")
    df.columns = df.columns.str.strip()

    client = bigquery.Client()

    # Cargar las dimensiones ya existentes desde BigQuery
    df_tiempo = client.query("""
        SELECT id_tiempo, anio, mes, trimestre, nombre_mes
        FROM `solar-curve-464603-h7.dw_iaenp.dim_tiempo`
    """).to_dataframe()

    df_sector = client.query("""
        SELECT id_sector, nombre_sector
        FROM `solar-curve-464603-h7.dw_iaenp.dim_sector`
    """).to_dataframe()

    # Normalizar: pasar de columnas múltiples a filas (melt)
    sectores = {
        "indice_manufacturas_mensual": "Manufacturas",
        "indice_comercio_mensual": "Comercio",
        "indice_construccion_mensual": "Construcción",
        "indice_servicios_mensual": "Servicios",
        "indice_de_actividad_empresarial_no_petrolera_total_mensual": "Total AENP"
    }

    df_melt = df.melt(id_vars=["anio", "mes"], 
                      value_vars=sectores.keys(),
                      var_name="nombre_columna", 
                      value_name="valor_indice")
    df_melt["nombre_sector"] = df_melt["nombre_columna"].map(sectores)
    df_melt.drop(columns=["nombre_columna"], inplace=True)

    # Asegurar tipos consistentes para el merge
    df_melt["anio"] = df_melt["anio"].astype(int)
    df_melt["mes"] = df_melt["mes"].astype(int)
    df_melt["trimestre"] = ((df_melt["mes"] - 1) // 3 + 1).astype(str)
    df_melt["nombre_mes"] = df_melt["mes"].apply(lambda x: pd.to_datetime(str(x), format='%m').strftime('%B'))

    # Hacer joins con las dimensiones
    fact_df = df_melt.merge(df_tiempo, on=["anio", "mes", "trimestre", "nombre_mes"], how="left")
    fact_df = fact_df.merge(df_sector, on="nombre_sector", how="left")

    # Seleccionar columnas finales
    fact_df = fact_df[["id_tiempo", "id_sector", "valor_indice"]]

    # Cargar a BigQuery
    table_id = "solar-curve-464603-h7.dw_iaenp.fact_iaenp"
    client.load_table_from_dataframe(fact_df, table_id).result()

#Tareas del DAG
with DAG(
    dag_id="etl_iaenp",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["iaenp", "etl", "bigquery"]
) as dag:

    start = EmptyOperator(task_id="inicio")
    end = EmptyOperator(task_id="fin")

    task_dim_tiempo = PythonOperator(
        task_id="cargar_dim_tiempo",
        python_callable=load_dim_tiempo,
    )

    task_dim_sector = PythonOperator(
        task_id="cargar_dim_sector",
        python_callable=load_dim_sector,
    )

    task_fact_iaenp = PythonOperator(
        task_id="cargar_fact_iaenp",
        python_callable=load_fact_iaenp,
    )

    start >> [task_dim_tiempo, task_dim_sector] >> task_fact_iaenp >> end

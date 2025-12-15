import io
import logging
from datetime import datetime
import pandas as pd
from google.cloud import storage, bigquery, dataproc_v1
from google.cloud.dataproc_v1.types import Batch, PySparkBatch, RuntimeConfig
from google.cloud.exceptions import NotFound
from cloudevents.http import CloudEvent
from functions_framework import cloud_event
from config.paths import (
    PROJECT_ID,
    DATASET_BRONCE,
    TABLE_RATIO,
    BUCKET_MONITOREADO
)


# --------------------------------------------------------
# CONFIG PARA EL JOB
# --------------------------------------------------------
REGION = "southamerica-west1"
BUCKET_SCRIPT = f"gs://{BUCKET_MONITOREADO}/resources/jb_ratio_liquidez.py"
BATCH_NAME = "jb-ratio-liquidez"
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
BATCH_NAME = f"{BATCH_NAME}-{timestamp}"



# --------------------------------------------------------
# Función principal
# --------------------------------------------------------

def run_pipeline_ratio_liquidez(bucket_name, file_name):
    logging.info(f"[PIPELINE] Procesando Ratio Liquidez -> {file_name}")

    df_raw = leer_excel_desde_gcs(bucket_name, file_name, "21")
    if df_raw is None:
        logging.error("No se pudo leer el Excel.")
        return

    df_transformado = _transformar_ratio_liquidez(df_raw, file_name)
    if df_transformado is None or df_transformado.empty:
        logging.info("No hay datos transformados.")
        return

    cargar_dataframe_bigquery(df_transformado, DATASET_BRONCE, TABLE_RATIO)

    

    ejecutar_dataproc_ratio_liquidez()




# ----------------------- 
# TRANSFORM 
# -----------------------

def _transformar_ratio_liquidez(df, file_name):
    try:
        parts = file_name.replace(".XLS", "").replace(".xlsx", "").split("-")
        raw_date = parts[-1]
        mes = raw_date[:2]

        try:
            anio = int(raw_date[-4:])
        except:
            anio = None

        df[0] = df[0].astype(str).str.strip()

        inicio = 7
        fin_idx = df[df[0].str.contains("TOTAL BANCA MÚLTIPLE", case=False, na=False)].index
        if len(fin_idx) == 0:
            return None

        fin = fin_idx[0]
        bloque = df.iloc[inicio:fin, :]

        mn = bloque.iloc[:, [0, 1, 2, 3]]
        mn.columns = ["Institución", "Activos_Líquidos", "Pasivos_Líquidos", "Ratio_Liquidez"]
        mn["Moneda"] = "MN"

        me = bloque.iloc[:, [0, 5, 6, 7]]
        me.columns = ["Institución", "Activos_Líquidos", "Pasivos_Líquidos", "Ratio_Liquidez"]
        me["Moneda"] = "ME"

        df_total = pd.concat([mn, me], ignore_index=True)
        df_total["Institución"] = df_total["Institución"].str.strip()

        df_total = df_total[~df_total["Institución"].str.contains("TOTAL BANCA MÚLTIPLE", case=False, na=False)]

        for col in ["Activos_Líquidos", "Pasivos_Líquidos", "Ratio_Liquidez"]:
            df_total[col] = (
                df_total[col]
                .astype(str)
                .str.replace(",", "")
                .str.replace("%", "")
                .str.strip()
            )
            df_total[col] = pd.to_numeric(df_total[col], errors="coerce")

        df_total["anio"] = anio
        df_total["mes"] = mes
        df_total["fecha_carga"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        return df_total

    except Exception as e:
        logging.exception(f"Error transformando {file_name}: {e}")
        return None



# ----------------------- 
# JOB 
# -----------------------

def ejecutar_dataproc_ratio_liquidez():
    logging.info("[DATAPROC] Lanzando PySpark Batch Plata → Oro...")

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    pyspark_batch = PySparkBatch(
        main_python_file_uri=BUCKET_SCRIPT
    )

    runtime_cfg = RuntimeConfig(
        version="2.1",
        properties={
            "spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            "spark.executor.instances": "2",
            "spark.executor.cores": "4",
            "spark.executor.memory": "4g",
            "spark.driver.cores": "4",
            "spark.driver.memory": "4g",
        }
    )

    batch = Batch(
        pyspark_batch=pyspark_batch,
        runtime_config=runtime_cfg,
    )

    operation = client.create_batch(
        parent=f"projects/{PROJECT_ID}/locations/{REGION}",
        batch=batch,
        batch_id=BATCH_NAME,
    )

    logging.info("[DATAPROC] Batch enviado correctamente.")



# --------------------------------------------------------
#Leer Excel
# --------------------------------------------------------

def leer_excel_desde_gcs(bucket_name, file_name, sheet_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_bytes()

        try:
            # XLS (solo funciona con xlrd==1.2.0)
            return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=None, dtype=str, engine="xlrd")
        except Exception:
            # XLSX
            return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=None, dtype=str)

    except Exception as e:
        logging.exception(f"Error al leer archivo {file_name}: {e}")
        return None
    

# --------------------------------------------------------
# CARGA A BIGQUERY
# --------------------------------------------------------
def cargar_dataframe_bigquery(df, dataset_id, table_id):
    bq = bigquery.Client()
    project = bq.project

    dataset_ref = f"{project}.{dataset_id}"

    # Crear dataset si no existe
    try:
        bq.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} ya existe.")
    except Exception:
        logging.info(f"Dataset {dataset_id} no existe. Creando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "southamerica-west1"
        bq.create_dataset(dataset)
        logging.info(f"Dataset {dataset_id} creado.")


    table_ref = f"{project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        autodetect=False,                    
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
    )

    job = bq.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    logging.info(f"Cargados {len(df)} registros en {table_ref}")
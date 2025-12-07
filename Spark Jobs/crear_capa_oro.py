from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lpad, to_date, lit, row_number
from pyspark.sql.window import Window
from google.cloud import bigquery
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# -------------------------------
# CONFIGURACIÓN
# -------------------------------
PROJECT_ID = "prueba1moi"
DATASET = "oro"
DATASET_REGION = "southamerica-west1"



# -------------------------------
# CREAR DATASET SI NO EXISTE
# -------------------------------
def ensure_dataset_exists():
    client = bigquery.Client(project=PROJECT_ID)

    dataset_full_id = f"{PROJECT_ID}.{DATASET}"

    try:
        client.get_dataset(dataset_full_id)
        print(f"Dataset {dataset_full_id} ya existe.")
    except Exception:
        dataset = bigquery.Dataset(dataset_full_id)
        dataset.location = DATASET_REGION
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_full_id} creado correctamente.")




spark = (
    SparkSession.builder
    .appName("crear_capa_oro")
    .getOrCreate()
)


ensure_dataset_exists()



# ---------------------------------------------
# 1. DIM FECHA
# ---------------------------------------------
anios = list(range(2016, 2026))
meses = list(range(1, 13))
data = [(a, m) for a in anios for m in meses]

df_fecha = spark.createDataFrame(data, ["anio", "mes"])

df_fecha = df_fecha.withColumn(
    "fecha",
    to_date(
        concat_ws("-", col("anio"), lpad(col("mes"), 2, "0"), lit("01")),
        "yyyy-MM-dd"
    )
)

df_fecha = df_fecha.withColumn(
    "id_fecha",
    row_number().over(Window.orderBy("anio", "mes"))
)

df_fecha = df_fecha.select("id_fecha", "anio", "mes", "fecha")

(
    df_fecha.write.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.dim_fecha")
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)

# ---------------------------------------------
# 2. DIM MONEDA
# ---------------------------------------------
data_moneda = [
    ("SOL", "Sol Peruano"),
    ("USD", "Dólar Estadounidense"),
    ("CAD", "Dólar Canadiense")
]

df_moneda = spark.createDataFrame(data_moneda, ["abreviacion", "moneda"])

df_moneda = df_moneda.withColumn(
    "id_moneda",
    row_number().over(Window.orderBy(lit(1)))
)

df_moneda = df_moneda.select("id_moneda", "abreviacion", "moneda")

(
    df_moneda.write.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.dim_moneda")
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)

#

# ---------------------------------------------
# 3. DIM INDICADOR
# ---------------------------------------------

data_indicador = [
    ("Gestión Financiera", "Loans to Deposits", "Colocaciones Brutas / Depósitos", "Relación entre fondos disponibles y créditos concedidos", "%", 100.0, 120.0, 0),
    ("Gestión de Riesgo", "Ratio de Capital Global", "Patrimonio Efectivo / Requerimiento Patrimonio Efectivo / 10%", "Porcentaje del patrimonio efectivo sobre los activos ponderados por riesgo", "%", 10.0, 11.0, 1),
    ("Créditos", "Morosidad", "Créditos atrasados / Total Créditos Directos", "Nivel de créditos atrasados", "%", 4.0, 8.0, 0),
    ("Créditos", "Crecimiento de Cartera de Créditos", "(Créditos actuales − Créditos año anterior) / Créditos año anterior", "Variación interanual de créditos", "%", -5.0, 0.0, 1),
    ("Gestión de Riesgo", "Ratio de Liquidez", "Activos Líquidos / Obligaciones corto plazo", "Cobertura de obligaciones MN y ME", "%", 90.0, 100.0, 1),
    ("Tesorería", "Sensibilidad al Tipo de Cambio", "(Activos ME / Pasivos ME) / Patrimonio Neto × %ΔTC", "Exposición del banco a fluctuaciones cambiarias", "%", 2.0, 5.0, 0),
    ("Gestión Comercial", "Participación de Depósitos", "Depósitos banco / Depósitos mercado", "Cuota de mercado en depósitos", "%", 5.0, 10.0, 0),
    ("Gestión Operativa", "Pérdidas Operativas", "Pérdidas Operativas / (Margen + Ingresos No Financieros)", "Impacto de pérdidas operativas", "%", 2.0, 5.0, 0)
]

cols_ind = [
    "area_responsable",
    "nombre",
    "formula",
    "descripcion",
    "unidad",
    "lim_amarillo_min",
    "lim_amarillo_max",
    "flag_limite"
]

df_ind = spark.createDataFrame(data_indicador, cols_ind)

df_ind = df_ind.withColumn(
    "id_indicador",
    row_number().over(Window.orderBy(lit(1)))
)

df_ind = df_ind.select(
    "id_indicador",
    "nombre",
    "area_responsable",
    "formula",
    "descripcion",
    "unidad",
    "lim_amarillo_min",
    "lim_amarillo_max",
    "flag_limite"
)

(
    df_ind.write.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.dim_indicador")
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)


# ========================================
# 4. DIM BANCO
# =======================================
data_banco = [
    ("Scotiabank Perú", "Scotiabank", "Banca Múltiple", "Scotiabank Group (Canadá)"),
    ("Banco de Crédito del Perú", "BCP", "Banca Múltiple", "Grupo Credicorp"),
    ("BBVA Perú", "BBVA", "Banca Múltiple", "Grupo BBVA (España)"),
    ("Interbank", "Interbank", "Banca Múltiple", "Intercorp Perú Ltd."),
    ("Banco Interamericano de Finanzas", "BanBif", "Banca Múltiple", "Grupo Fierro (España)"),
    ("Mibanco", "Mibanco", "Microfinanzas", "Grupo Credicorp")
]

cols_banco = ["nombre", "abreviacion", "tipo_entidad", "grupo_financiero"]

df_banco = spark.createDataFrame(data_banco, cols_banco)

df_banco = df_banco.withColumn(
    "id_banco",
    row_number().over(Window.orderBy(lit(1)))
)

df_banco = df_banco.select(
    "id_banco", "nombre", "abreviacion",
    "tipo_entidad", "grupo_financiero"
)

(
    df_banco.write.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.dim_banco")
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)

spark.stop()
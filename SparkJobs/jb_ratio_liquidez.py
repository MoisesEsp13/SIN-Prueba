from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_date, concat_ws, lpad, lit,
    lower, trim, create_map, coalesce
)
from itertools import chain
from google.cloud import bigquery
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType



# -----------------------------
# Inicializar Spark
# -----------------------------
spark = SparkSession.builder.appName("BroncePlataOro").enableHiveSupport().getOrCreate()


# -----------------------------
# Parámetros BigQuery
# -----------------------------
project_id = "final-espinal-aguilar"
dataset_plata = "plata"
dataset_oro = "oro"
table_plata = "sbs_liquidez"
table_oro = "hecho_riesgo"
# minutos = 10


bq_client = bigquery.Client(project=project_id)



# -----------------------------
# Verificar si dataset Plata existe
# -----------------------------
def create_dataset_if_not_exists(dataset_name):
    dataset_ref = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_ref)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_name} ya existe.")
    except Exception:
        print(f"Dataset {dataset_name} no existe. Creando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "southamerica-west1"
        bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_name} creado.")

create_dataset_if_not_exists(dataset_plata)

# -----------------------------
# Cargar tabla Bronce
# -----------------------------
df_liquidez = (
    spark.read.format("bigquery")
    .option("table", f"{project_id}.bronce.sbs_liquidez")
    .load()
)
# df_liquidez = df_liquidez.filter(
#     F.col("fecha_carga") >= F.current_timestamp() - F.expr(f"INTERVAL {minutos} MINUTE")
# )


print("Columnas originales df_liquidez:", df_liquidez.columns)



# Normalizar nombres de columnas
df_liquidez = df_liquidez.toDF(*[c.lower() for c in df_liquidez.columns])
print("Columnas normalizadas df_liquidez:", df_liquidez.columns)

# Quitar tildes problemáticas
df_liquidez = (
    df_liquidez
        .withColumnRenamed("institución", "institucion")
        .withColumnRenamed("activos_líquidos", "activos_liquidos")
        .withColumnRenamed("pasivos_líquidos", "pasivos_liquidos")
)


# Normalizar institucion
df_liquidez = df_liquidez.withColumn(
    "institucion",
    when(lower(col("institucion")).like("%crédito%"), "BCP")
    .when(lower(col("institucion")).like("%credito%"), "BCP")
    .when(lower(col("institucion")).like("%interbank%"), "Interbank")
    .when((lower(col("institucion")).like("%bbva%")) | (lower(col("institucion")).like("%continental%")), "BBVA")
    .when(lower(col("institucion")).like("%scotiabank%"), "Scotiabank")
    .when(lower(col("institucion")).like("%interamericano%"), "BanBif")
    .when(lower(col("institucion")).like("%mibanco%"), "Mibanco")
    .otherwise("Otros")
)

# Mapeo abreviaturas de mes -> número
mes_map = {
    "en": "01", "fe": "02", "ma": "03", "ab": "04",
    "my": "05", "jn": "06", "jl": "07", "ag": "08",
    "se": "09", "oc": "10", "no": "11", "di": "12"
}
mapping_expr = create_map([lit(x) for x in chain(*mes_map.items())])

df_liquidez = df_liquidez.withColumn(
    "mes",
    coalesce(
        mapping_expr.getItem(lower(trim(col("mes").cast("string")))),
        trim(col("mes").cast("string"))
    )
)

# Filtrado y limpieza
df_filtered = df_liquidez.filter(col("institucion") != "Otros")
if "ratio_liquidez" in df_filtered.columns:
    df_filtered = df_filtered.withColumn("ratio_liquidez", col("ratio_liquidez").cast("double"))
else:
    raise Exception(f"La columna 'ratio_liquidez' no se encontró en df_liquidez; columnas: {df_liquidez.columns}")

# Selección y transformación final Plata
df_plata = df_filtered.select(
    to_date(concat_ws("-", col("anio").cast("string"), lpad(col("mes").cast("string"), 2, "0"), lit("01")), "yyyy-MM-dd").alias("fecha"),
    col("anio"),
    col("mes").cast(IntegerType()).alias("mes"),
    col("institucion"),
    col("activos_liquidos"),
    col("pasivos_liquidos"),
    col("moneda")
).orderBy("institucion", "anio", "mes")

# Guardar tabla Plata
(
    df_plata.write.format("bigquery")
    .option("table", f"{project_id}.{dataset_plata}.{table_plata}")
    .option("writeMethod", "direct")
    .mode("append")
    .save()
)
df_plata.show(5, False)
print("Procesamiento Plata exitoso!")


# -----------------------------
# Transformación Oro
# -----------------------------

df_liquidez_oro = df_plata

bancos_map = {
    "Scotiabank": 1,
    "BCP": 2,
    "Banco de Crédito del Perú": 2,
    "BBVA": 3,
    "Interbank": 4,
    "BanBif": 5,
    "Banco Interamericano de Finanzas": 5,
    "Mibanco": 6
}

banco_udf = F.udf(lambda nombre: bancos_map.get(nombre, None))

df_creditos_prep = (
    df_liquidez_oro
    .filter(F.col("moneda") == "MN")
    .withColumn("id_banco", banco_udf(F.col("institucion")))
    .withColumn("anio", F.col("anio").cast("int"))
    .withColumn("mes", F.col("mes").cast("int"))
    .select(
        "anio",
        "mes",
        "id_banco",
        "moneda",
        F.col("activos_liquidos").cast("double").alias("activos_liquidos"),
        F.col("pasivos_liquidos").cast("double").alias("pasivos_liquidos")
    )
)

df_ratio_prep = (
    df_creditos_prep
    .withColumn("numerador", F.col("activos_liquidos"))
    .withColumn("denominador", F.col("pasivos_liquidos"))
    .withColumn("valor", (F.col("numerador") / F.col("denominador")).cast("double"))
)

df_fecha = (
    spark.read.format("bigquery")
    .option("table", f"{project_id}.{dataset_oro}.dim_fecha")
    .load()
)

df_ratio_prep = (
    df_ratio_prep.alias("c")
    .join(df_fecha.alias("f"), (F.col("c.anio") == F.col("f.anio")) & (F.col("c.mes") == F.col("f.mes")), "left")
    .select(F.col("c.*"), F.col("f.id_fecha"))
)

df_ratio_prep = (
    df_ratio_prep
    .withColumn("id_moneda", F.when(F.col("moneda") == "MN", F.lit(1))
                              .when(F.col("moneda") == "ME", F.lit(2))
                              .otherwise(F.lit(None)))
    .withColumn("id_indicador", F.lit(7))
)

df_hecho_ratio = df_ratio_prep.select(
    F.col("id_banco").cast("int"),
    F.col("id_fecha").cast("int"),
    F.col("id_moneda").cast("int"),
    F.col("id_indicador").cast("int"),
    F.col("numerador").cast("float"),
    F.col("denominador").cast("float"),
    F.col("valor").cast("float")
)


# Mostrar para verificar
df_hecho_ratio.printSchema()
df_hecho_ratio.show(5)

# Guardar en BigQuery
(
    df_hecho_ratio.write.format("bigquery")
    .option("table", f"{project_id}.{dataset_oro}.hecho_riesgo")
    .option("writeMethod", "direct")
    .mode("append") 
    .save()
)

print("Tabla hecho_riesgo creada exitosamente en BigQuery")


spark.stop()
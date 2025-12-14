# -------------------------
# CONFIGURACIÓN DE BUCKETS
# -------------------------

# Proyecto 
PROJECT_ID = "prueba1moi"

# Bucket principal
BUCKET_MONITOREADO = "grupo6_scotiabank_bucket3"

# Prefijo donde se suben los archivos RAW por entidad
ROOT_RAW = "data/raw/"


# -------------------------
# PREFIJOS SBS
# -------------------------

PREFIX_SBS = ROOT_RAW + "SBS/"
PREFIX_RATIO_LIQUIDEZ = PREFIX_SBS + "RATIO_LIQUIDEZ/"


# -----------------------
# BIGQUERY DATASET/TABLE
# -----------------------
DATASET_BRONCE = "bronce"
DATASET_PLATA = "plata"
DATASET_ORO = "oro"
TABLE_RATIO = "sbs_liquidez"
TABLE_ORO = "hecho_riesgo"




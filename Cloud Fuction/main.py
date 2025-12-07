import logging
from cloudevents.http import CloudEvent
from functions_framework import cloud_event
import config.paths as paths
from pipelines.ratio_liquidez import run_pipeline_ratio_liquidez

@cloud_event
def bronce_dispatcher(event: CloudEvent):
    data = event.data
    bucket = data.get("bucket")
    name = data.get("name")

    logging.info(f"[GEN2] Evento recibido. bucket={bucket}, name={name}")

    if bucket != paths.BUCKET_MONITOREADO:
        logging.info(f"Ignorado: evento de bucket distinto: {bucket}")
        return

    # Dispatcher por carpeta
    if name.startswith(paths.PREFIX_RATIO_LIQUIDEZ):
        run_pipeline_ratio_liquidez(bucket, name)
        return
    
    logging.info(f"Ignorado: {name} no coincide con ninguna carpeta monitoreada.")

import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import logging.config

# Cargar variables de entorno
load_dotenv()

class ProjectConfig:

    # Configuración de paths
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_RAW = BASE_DIR / "data" / "raw"
    DATA_PROCESSED = BASE_DIR / "data" / "processed"
    LOGS_DIR = BASE_DIR / "logs"

    # Crear directorios si no existen
    for directory in [DATA_RAW, DATA_PROCESSED, LOGS_DIR]:
        directory.mkdir(parents = True, exist_ok = True)

    # Bounding box para Río Negro, Patagonia (ejemplo)
    BBOX = [-71.8756,-41.5862,-71.3782,-41.3608]  # [Oeste, Sur, Este, Norte]

    # Período de análisis (incendio)
    START_DATE = "2024-12-01"
    END_DATE = "2025-03-31"
    
    # Credenciales (desde variables de entorno)
    CDS_UID = os.getenv("CDS_UID")
    CDS_API_KEY = os.getenv("CDS_API_KEY")
    FIRMS_API_KEY = "dfddee9b9ccda792d06d15dca8ee3cfd"
    
    # Parámetros de análisis
    RESOLUTION = 0.1  # grados para datos meteorológicos
    VEGETATION_INDICES = ["NDVI", "EVI", "NBR", "NDMI"]
    
    # Configuración de APIs
    OPENMETEO_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/{source}/{area}/{day_range}/{start_date}"
    COPERNICUS_API_URL = "https://cds.climate.copernicus.eu/api/v2"

    # Fuentes FIRMS disponibles 
    FIRMS_SOURCES = {
        'VIIRS_SNPP_SP': 'VIIRS_SNPP_SP',
        'VIIRS_NOAA20_SP': 'VIIRS_NOAA20_SP', 
        'MODIS_SP': 'MODIS_SP',
        'MODIS_NRT': 'MODIS_NRT'
    }

    # Columnas esperadas en datos FIRMS
    FIRMS_EXPECTED_COLUMNS = [
        'latitude', 'longitude', 'brightness', 'scan', 'track',
        'acq_date', 'acq_time', 'satellite', 'instrument', 'confidence',
        'version', 'bright_t31', 'frp', 'daynight'
    ]
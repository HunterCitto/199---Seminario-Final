import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import logging.config

# Cargar variables de entorno
load_dotenv()

# Configuración de paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

# Crear directorios si no existen
for directory in [DATA_RAW, DATA_PROCESSED, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

class ProjectConfig:

    # Bounding box para Río Negro, Patagonia (ejemplo)
    BBOX = [-71.884,-41.993,-71.046,-41.031]  # [Oeste, Sur, Este, Norte]

    # Período de análisis (incendio)
    START_DATE = "2024-12-20"
    END_DATE = "2024-12-29"

    # Período de referencia (años anteriores para comparación)
    REF_START_DATE = "2024-12-23"
    REF_END_DATE = "2024-12-28"
    
    # Credenciales (desde variables de entorno)
    COPERNICUS_UID = os.getenv("COPERNICUS_UID")
    COPERNICUS_API_KEY = os.getenv("COPERNICUS_API_KEY")
    FIRMS_API_KEY = "dfddee9b9ccda792d06d15dca8ee3cfd"
    
    # Parámetros de análisis
    RESOLUTION = 0.1  # grados para datos meteorológicos
    VEGETATION_INDICES = ["NDVI", "EVI", "NBR", "NDMI"]
    
    # Configuración de APIs
    # FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
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

# Configurar logging
def setup_logging():
    logging.config.fileConfig(
        BASE_DIR / 'logging_config.ini',
        defaults={'logfilename': str(LOGS_DIR / 'wildfire_analysis.log')},
        disable_existing_loggers=False
    )
    return logging.getLogger(__name__)

# Inicializar logger
logger = setup_logging()
import requests
import pandas as pd
from pathlib import Path
import logging
import time
from datetime import datetime, timedelta
from io import StringIO  # Importar StringIO desde io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class FIRMSTools:

    def __init__(self, config):
        self.config = config
        self.raw_data_path = Path(f"{config.DATA_RAW}/firms")
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        self.session = self._create_session()
    
    def _create_session(self):
        """Crea una sesión con reintentos automáticos"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _validate_api_key(self):
        """Valida que la API key esté presente"""
        if not self.config.FIRMS_API_KEY:
            logger.error("API key de FIRMS no configurada")
            return False
        return True
    
    def _calculate_day_range(self, start_date, end_date):
        """Calcula el número de días entre dos fechas"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        return (end - start).days + 1
    
    def download_fire_data(self, source = 'MODIS_SP'):
        """
        Descarga datos de incendios usando el formato correcto de URL
        """
        if not self._validate_api_key():
            return None
        
        try:
            logger.info(f"Descargando datos de {source} para {self.config.START_DATE} - {self.config.END_DATE}")
            
            # Calcular day_range
            day_range = self._calculate_day_range(self.config.START_DATE, self.config.END_DATE)
            
            # Formatear área
            area_str = f"{self.config.BBOX[0]},{self.config.BBOX[1]},{self.config.BBOX[2]},{self.config.BBOX[3]}"
            
            # Construir URL según documentación oficial
            url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{self.config.FIRMS_API_KEY}/{source}/{area_str}/{day_range}/{self.config.START_DATE}"
            
            logger.info(f"URL: {url}")
            
            # Nombre de archivo para guardar
            filename = f"firms_{source}_{self.config.START_DATE}_{self.config.END_DATE}.csv"
            filepath = self.raw_data_path / filename
            
            # Verificar si el archivo ya existe
            if filepath.exists():
                logger.info(f"Archivo ya existe: {filepath}")
                try:
                    return pd.read_csv(filepath)
                except Exception as e:
                    logger.warning(f"Error leyendo archivo existente: {e}. Re-descargando...")
            
            # Realizar solicitud
            response = self.session.get(url, timeout=30)
            
            # Verificar respuesta
            if response.status_code != 200:
                logger.error(f"Error HTTP {response.status_code}")
                return None
            
            # Verificar que sea CSV válido
            if 'text/html' in response.headers.get('content-type', ''):
                logger.error("La API devolvió HTML en lugar de CSV")
                return None
            
            # Intentar leer el CSV - CORRECCIÓN: Usar StringIO desde io
            try:
                data = pd.read_csv(StringIO(response.text))
                
                # Verificar que tenga las columnas mínimas esperadas
                required_cols = ['latitude', 'longitude', 'acq_date']
                missing_cols = [col for col in required_cols if col not in data.columns]
                
                if missing_cols:
                    logger.error(f"Faltan columnas requeridas: {missing_cols}")
                    logger.debug(f"Columnas disponibles: {list(data.columns)}")
                    return None
                
                # Filtrar por fecha exacta (la API puede devolver más días del solicitado)
                if 'acq_date' in data.columns:
                    data = data[
                        (data['acq_date'] >= self.config.START_DATE) & 
                        (data['acq_date'] <= self.config.END_DATE)
                    ]
                
                # Guardar datos
                data.to_csv(filepath, index=False)
                logger.info(f"Datos guardados: {filepath} ({len(data)} registros)")
                return data
                
            except pd.errors.EmptyDataError:
                logger.info("No se encontraron datos para el período y área especificados")
                return pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Error procesando CSV: {e}")
                # Guardar respuesta para diagnóstico
                debug_path = self.raw_data_path / f"debug_response_{source}.txt"
                with open(debug_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Respuesta guardada en {debug_path} para diagnóstico")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {e}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            return None
    
    def get_multisource_fire_data(self):
        """Obtiene datos de múltiples fuentes"""
        sources = ['MODIS_SP', 'VIIRS_NOAA20_SP', 'VIIRS_SNPP_SP',
                   'MODIS_NRT', 'VIIRS_NOAA20_NRT', 'VIIRS_NOAA21_NRT', 'VIIRS_SNPP_NRT']
        all_data = []
        
        for source in sources:
            try:
                data = self.download_fire_data(source)
                if data is not None and not data.empty:
                    data['source'] = source
                    all_data.append(data)
                    logger.info(f"Datos de {source} obtenidos: {len(data)} registros")
            except Exception as e:
                logger.warning(f"No se pudieron obtener datos de {source}: {e}")
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            # Guardar datos combinados
            combined_path = self.raw_data_path / f"firms_combined_{self.config.START_DATE}_{self.config.END_DATE}.csv"
            combined_data.to_csv(combined_path, index=False)
            logger.info(f"Datos combinados guardados: {combined_path}")
            return combined_data
        else:
            logger.error("No se pudieron obtener datos de ninguna fuente")
            return None

    def process_fire_data(self, raw_data):
        """Procesa los datos brutos de FIRMS para análisis"""
        if raw_data is None or raw_data.empty:
            return None
        
        processed_data = raw_data.copy()
        
        # Convertir fecha y hora
        if 'acq_date' in processed_data.columns and 'acq_time' in processed_data.columns:
            processed_data['acq_datetime'] = pd.to_datetime(
                processed_data['acq_date'] + ' ' + processed_data['acq_time'].astype(str).str.zfill(4),
                format='%Y-%m-%d %H%M',
                errors='coerce'
            )
        
        # Extraer componentes de tiempo
        if 'acq_datetime' in processed_data.columns:
            processed_data['acq_hour'] = processed_data['acq_datetime'].dt.hour
            processed_data['acq_dayofyear'] = processed_data['acq_datetime'].dt.dayofyear
            processed_data['acq_month'] = processed_data['acq_datetime'].dt.month
        
        return processed_data
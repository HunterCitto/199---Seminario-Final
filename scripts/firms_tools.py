import requests
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, timedelta
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from project_config import ProjectConfig as cfg

logger = logging.getLogger(__name__)

class FIRMSTools :

    def __init__(self) :
        self.config = cfg
        self.raw_data_path = Path(f"{cfg.DATA_RAW}/firms")
        self.raw_data_path.mkdir(parents = True, exist_ok = True)
        self.session = self._create_session()
    
    def _create_session(self) :
        """Crea una sesión con reintentos automáticos"""
        session = requests.Session()
        retry_strategy = Retry(
            total = 3,
            backoff_factor = 0.5,
            status_forcelist = [429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _validate_api_key(self) :
        """Valida que la API key esté presente"""
        if not self.config.FIRMS_API_KEY:
            logger.error("API key de FIRMS no configurada")
            return False
        return True
    
    def _daterange_chunks(self, start_date, end_date, max_days = 10) :
        """Genera intervalos de fechas de hasta max_days días"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        while start <= end:
            chunk_end = min(start + timedelta(days=max_days - 1), end)
            yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
            start = chunk_end + timedelta(days=1)

    def _download_chunk(self, source, bbox, start_date, end_date) :
        """Descarga un fragmento de datos para un rango de fechas <= 10 días"""

        day_range = (datetime.strptime(end_date, "%Y-%m-%d") - 
                     datetime.strptime(start_date, "%Y-%m-%d")).days + 1

        area_str = ",".join(map(str, bbox))
        url = (
            f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            f"{self.config.FIRMS_API_KEY}/{source}/{area_str}/{day_range}/{start_date}"
        )

        logger.info(f"Request FIRMS {source} -> {start_date} to {end_date} | URL: {url}")

        response = self.session.get(url, timeout=30)
        if response.status_code != 200:
            logger.error(f"Error HTTP {response.status_code} para {source}")
            return pd.DataFrame()

        try:
            data = pd.read_csv(StringIO(response.text))
            if "acq_date" in data.columns:
                data = data[
                    (data["acq_date"] >= start_date) &
                    (data["acq_date"] <= end_date)
                ]
            return data
        except Exception as e:
            logger.error(f"Error procesando CSV para {source}: {e}")
            return pd.DataFrame()

    def get_fire_data(self, sources) :
        """
        Descarga datos de múltiples fuentes FIRMS en el rango de fechas dado (iterando por chunks de 10 días).
        - sources: lista de fuentes (sin NRT).
        - start_date, end_date: str 'YYYY-MM-DD'
        - bbox: [lon_min, lat_min, lon_max, lat_max]
        """

        start_date = self.config.START_DATE
        end_date = self.config.END_DATE
        bbox = self.config.BBOX

        if not self._validate_api_key() :
            return None

        all_data = []
        for source in sources :
            source_frames = []
            for chunk_start, chunk_end in self._daterange_chunks(start_date, end_date):
                df = self._download_chunk(source, bbox, chunk_start, chunk_end)
                if not df.empty :
                    df["source"] = source
                    source_frames.append(df)

            if source_frames :
                source_data = pd.concat(source_frames, ignore_index=True)

                # Guardar por fuente
                filename = f"firms_{source}_{start_date}_{end_date}.csv"
                filepath = self.raw_data_path / filename
                source_data.to_csv(filepath, index=False)
                logger.info(f"Datos guardados: {filepath} ({len(source_data)} registros)")

                all_data.append(source_data)

        if all_data :
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_path = self.raw_data_path / f"firms_combined_{start_date}_{end_date}.csv"
            combined_data.to_csv(combined_path, index=False)
            logger.info(f"Datos combinados guardados: {combined_path}")
            return combined_data

        logger.warning("No se obtuvieron datos de ninguna fuente")
        return pd.DataFrame()

    def process_fire_data(self, raw_data) :
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
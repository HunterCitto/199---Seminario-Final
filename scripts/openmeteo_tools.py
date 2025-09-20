import pandas as pd
import requests
from datetime import datetime
import logging

import openmeteo_requests as openmeteo
import requests_cache
from retry_requests import retry

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OpenMeteoWeather :

    def __init__(self) :
        """
        Inicializa el fetcher de datos meteorológicos con Open-Meteo.
        """
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        self.retry_session = retry(self.cache_session, retries = 5, backoff_factor = 0.2)
        self.client = openmeteo.Client(session = self.retry_session)
    
    def _validate_dates(self, start_date: str, end_date: str) -> None :
        """
        Valida que las fechas estén en el formato correcto y sean válidas
        
        Args:
            start_date (str): Fecha inicial en formato YYYY-MM-DD
            end_date (str): Fecha final en formato YYYY-MM-DD
            
        Raises:
            ValueError: Si las fechas no son válidas
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            if start_dt > end_dt:
                raise ValueError("La fecha inicial no puede ser mayor que la fecha final")
            
            # Verificar que las fechas no sean futuras
            if start_dt > datetime.now():
                raise ValueError("Las fechas no pueden ser futuras")
                
            # Open-Meteo tiene datos desde 1940 aproximadamente
            if start_dt.year < 1940:
                raise ValueError("Los datos históricos solo están disponibles desde 1940")
                
        except ValueError as e:
            raise ValueError(f"Formato de fecha inválido: {e}")
    
    def get_meteorological_data(self, start_date: str, end_date: str, bbox: str) -> pd.DataFrame :
        """
        Obtiene datos históricos del clima de Open-Meteo y los convierte en DataFrame
        
        Args:
            lat (float): Latitud
            lon (float): Longitud
            start_date (str): Fecha inicial en formato YYYY-MM-DD
            end_date (str): Fecha final en formato YYYY-MM-DD
            bbox (str): Bounding box en formato "min_lon,min_lat,max_lon,max_lat"
            
        Returns:
            pd.DataFrame: DataFrame con los datos meteorológicos
        """
        # Validar fechas
        self._validate_dates(start_date, end_date)
        
        # logger.info(f"Obteniendo datos para coordenadas: ({lat}, {lon})")
        logger.info(f"Período: {start_date} hasta {end_date}")
        
        # Parámetros para la API de Open-Meteo
        params = {
            'latitude': 52.52,
            'longitude': 13.41,
            'start_date': start_date,
            'end_date': end_date,
            'hourly': [
                'temperature_2m', 'relative_humidity_2m', 'pressure_msl',
                'precipitation', 'rain', 'snowfall', 'cloud_cover',
                'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
                'visibility', 'is_day', 'sunshine_duration'
            ],
            "bounding_box": bbox,
            'timezone': 'America/Argentina/Mendoza',
            'models': 'era5'  # Reanálisis ERA5 de ECMWF (datos de alta calidad)
        }
        
        try:
            responses = self.client.weather_api(self.base_url, params = params)

            for response in responses:
                print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
                print(f"Elevation: {response.Elevation()} m asl")
                print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
                
                # Process hourly data. The order of variables needs to be the same as requested.

                hourly = response.Hourly()
                hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
                hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
                hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
                hourly_rain = hourly.Variables(3).ValuesAsNumpy()
                hourly_snow_depth = hourly.Variables(4).ValuesAsNumpy()
                hourly_snowfall = hourly.Variables(5).ValuesAsNumpy()
                hourly_wind_speed_100m = hourly.Variables(6).ValuesAsNumpy()
                hourly_wind_direction_100m = hourly.Variables(7).ValuesAsNumpy()
                hourly_soil_temperature_100_to_255cm = hourly.Variables(8).ValuesAsNumpy()
                hourly_soil_moisture_100_to_255cm = hourly.Variables(9).ValuesAsNumpy()
                hourly_apparent_temperature = hourly.Variables(10).ValuesAsNumpy()
                hourly_dew_point_2m = hourly.Variables(11).ValuesAsNumpy()
                hourly_surface_pressure = hourly.Variables(12).ValuesAsNumpy()
                
                hourly_data = {"date": pd.date_range(
                    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                    end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                    freq = pd.Timedelta(seconds = hourly.Interval()),
                    inclusive = "left"
                )}

                hourly_data["latitude"] = response.Latitude()
                hourly_data["longitude"] = response.Longitude()
                hourly_data["elevation"] = response.Elevation()                
                hourly_data["temperature_2m"] = hourly_temperature_2m
                hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
                hourly_data["precipitation"] = hourly_precipitation
                hourly_data["rain"] = hourly_rain
                hourly_data["snow_depth"] = hourly_snow_depth
                hourly_data["snowfall"] = hourly_snowfall
                hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
                hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
                hourly_data["soil_temperature_100_to_255cm"] = hourly_soil_temperature_100_to_255cm
                hourly_data["soil_moisture_100_to_255cm"] = hourly_soil_moisture_100_to_255cm
                hourly_data["apparent_temperature"] = hourly_apparent_temperature
                hourly_data["dew_point_2m"] = hourly_dew_point_2m
                hourly_data["surface_pressure"] = hourly_surface_pressure

                return pd.DataFrame(data = hourly_data)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en la solicitud a la API: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            raise
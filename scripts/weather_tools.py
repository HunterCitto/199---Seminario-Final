import pandas as pd
import requests
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherDataFetcher:
    def __init__(self):
        """
        Inicializa el fetcher de datos meteorológicos con Open-Meteo (gratuito)
        """
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    def _validate_dates(self, start_date: str, end_date: str) -> None:
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
    
    def fetch_weather_data(self, lat: float, lon: float, 
                          start_date: str, end_date: str) -> pd.DataFrame:
        """
        Obtiene datos históricos del clima de Open-Meteo y los convierte en DataFrame
        
        Args:
            lat (float): Latitud
            lon (float): Longitud
            start_date (str): Fecha inicial en formato YYYY-MM-DD
            end_date (str): Fecha final en formato YYYY-MM-DD
            
        Returns:
            pd.DataFrame: DataFrame con los datos meteorológicos
        """
        # Validar fechas
        self._validate_dates(start_date, end_date)
        
        logger.info(f"Obteniendo datos para coordenadas: ({lat}, {lon})")
        logger.info(f"Período: {start_date} hasta {end_date}")
        
        # Parámetros para la API de Open-Meteo
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'hourly': [
                'temperature_2m', 'relative_humidity_2m', 'pressure_msl',
                'precipitation', 'rain', 'snowfall', 'cloud_cover',
                'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
                'visibility', 'is_day', 'sunshine_duration'
            ],
            'timezone': 'auto',
            'models': 'era5'  # Reanálisis ERA5 de ECMWF (datos de alta calidad)
        }
        
        try:
            # Hacer la solicitud a la API
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Procesar los datos
            df = self._process_api_data(data, lat, lon)
            
            logger.info(f"Datos obtenidos exitosamente: {len(df)} registros horarios")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error en la solicitud a la API: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            raise
    
    def _process_api_data(self, data: Dict, lat: float, lon: float) -> pd.DataFrame:
        """
        Procesa los datos de la API y los convierte en DataFrame
        
        Args:
            data (Dict): Datos brutos de la API
            lat (float): Latitud
            lon (float): Longitud
            
        Returns:
            pd.DataFrame: DataFrame procesado
        """
        hourly_data = data.get('hourly', {})
        
        if not hourly_data:
            raise ValueError("No se encontraron datos horarios en la respuesta")
        
        # Crear DataFrame con los datos horarios
        df = pd.DataFrame({
            'datetime': pd.to_datetime(hourly_data['time']),
            'latitude': lat,
            'longitude': lon,
            'temperature': hourly_data.get('temperature_2m', []),
            'humidity': hourly_data.get('relative_humidity_2m', []),
            'pressure': hourly_data.get('pressure_msl', []),
            'precipitation': hourly_data.get('precipitation', []),
            'rain': hourly_data.get('rain', []),
            'snowfall': hourly_data.get('snowfall', []),
            'cloud_cover': hourly_data.get('cloud_cover', []),
            'wind_speed': hourly_data.get('wind_speed_10m', []),
            'wind_direction': hourly_data.get('wind_direction_10m', []),
            'wind_gust': hourly_data.get('wind_gusts_10m', []),
            'visibility': hourly_data.get('visibility', []),
            'is_day': hourly_data.get('is_day', []),
            'sunshine_duration': hourly_data.get('sunshine_duration', [])
        })
        
        # Agregar columnas derivadas
        df['date'] = df['datetime'].dt.date
        df['time'] = df['datetime'].dt.time
        df['hour'] = df['datetime'].dt.hour
        
        # Convertir sunshine duration de segundos a horas
        df['sunshine_hours'] = df['sunshine_duration'] / 3600
        
        # Ordenar por datetime
        df = df.sort_values('datetime').reset_index(drop=True)
        
        return df
    
    def get_available_variables(self) -> Dict:
        """
        Devuelve información sobre las variables disponibles en la API
        
        Returns:
            Dict: Diccionario con variables disponibles
        """
        return {
            'hourly': [
                'temperature_2m', 'relative_humidity_2m', 'pressure_msl',
                'precipitation', 'rain', 'snowfall', 'cloud_cover',
                'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
                'visibility', 'is_day', 'sunshine_duration'
            ],
            'daily': [
                'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum',
                'rain_sum', 'snowfall_sum', 'wind_speed_10m_max'
            ]
        }
    
    def save_to_csv(self, df: pd.DataFrame, filename: str, data_dir: str = "../data/raw") -> None:
        """
        Guarda el DataFrame en un archivo CSV en la carpeta data/raw
        
        Args:
            df (pd.DataFrame): DataFrame a guardar
            filename (str): Nombre del archivo
            data_dir (str): Directorio de datos
        """
        # Crear directorio si no existe
        os.makedirs(data_dir, exist_ok=True)
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        filepath = os.path.join(data_dir, filename)
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"Datos guardados en {filepath}")
        
        # También guardar metadatos
        self._save_metadata(df, filepath)
    
    def _save_metadata(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Guarda metadatos del dataset
        
        Args:
            df (pd.DataFrame): DataFrame con los datos
            filepath (str): Ruta del archivo CSV
        """
        metadata = {
            'fecha_generacion': datetime.now().isoformat(),
            'total_registros': len(df),
            'periodo_inicio': df['datetime'].min().isoformat(),
            'periodo_fin': df['datetime'].max().isoformat(),
            'columnas': list(df.columns),
            'coordenadas': f"({df['latitude'].iloc[0]}, {df['longitude'].iloc[0]})"
        }
        
        metadata_path = filepath.replace('.csv', '_metadata.json')
        import json
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Metadatos guardados en {metadata_path}")
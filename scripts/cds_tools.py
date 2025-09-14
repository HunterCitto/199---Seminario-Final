import os
import logging
import datetime
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, List, Union, Tuple
from dateutil.relativedelta import relativedelta
from project_config import ProjectConfig as cfg

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('cds_tools')


class CDSTools:
    """Clase para obtener datos del Climate Data Store de Copernicus."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Inicializa el cliente CDS.
        
        Args:
            config_path: Ruta al archivo de configuración .cdsapirc
        """
        try:
            if config_path:
                os.environ['CDSAPI_RC'] = config_path
            
            self.client = cdsapi.Client()
            logger.info("Cliente CDS inicializado correctamente")
            
        except Exception as e:
            logger.error(f"Error al inicializar el cliente CDS: {e}")
            raise
    
    def validate_date_range(self, start_date: str, end_date: str, max_months: int = 12) -> Tuple[str, str]:
        """
        Valida el rango de fechas y asegura que no exceda el límite máximo.
        
        Args:
            start_date: Fecha de inicio (YYYY-MM-DD)
            end_date: Fecha de fin (YYYY-MM-DD)
            max_months: Número máximo de meses permitidos
            
        Returns:
            Tupla con fechas validadas (start_date, end_date)
        """
        try:
            # Convertir a objetos datetime
            start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            
            # Validar que start_date <= end_date
            if start_dt > end_dt:
                raise ValueError("La fecha de inicio debe ser anterior a la fecha de fin")
            
            # Calcular diferencia en meses
            delta = relativedelta(end_dt, start_dt)
            total_months = delta.years * 12 + delta.months
            
            if total_months > max_months:
                # Ajustar end_date al límite máximo
                new_end_dt = start_dt + relativedelta(months=max_months)
                logger.warning(f"Rango de fechas excede el límite de {max_months} meses. "
                              f"Ajustando end_date a {new_end_dt.strftime('%Y-%m-%d')}")
                end_date = new_end_dt.strftime('%Y-%m-%d')
            
            logger.info(f"Rango de fechas validado: {start_date} a {end_date}")
            return start_date, end_date
            
        except ValueError as e:
            logger.error(f"Error en formato de fecha: {e}")
            raise
        except Exception as e:
            logger.error(f"Error al validar rango de fechas: {e}")
            raise
    
    def _generate_date_lists(self, start_date: str, end_date: str) -> Tuple[List[str], List[str], List[str]]:
        """
        Genera listas de años, meses y días para el rango solicitado.
        
        Args:
            start_date: Fecha de inicio
            end_date: Fecha de fin
            
        Returns:
            Tupla con listas de años, meses y días
        """
        start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generar años
        years = list(range(start_dt.year, end_dt.year + 1))
        
        # Generar meses
        if start_dt.year == end_dt.year:
            months = list(range(start_dt.month, end_dt.month + 1))
        else:
            # Para múltiples años, incluir todos los meses
            months = list(range(1, 13))
        
        # Generar días
        if start_dt.month == end_dt.month and start_dt.year == end_dt.year:
            days = list(range(start_dt.day, end_dt.day + 1))
        else:
            # Para múltiples meses, incluir todos los días
            days = list(range(1, 32))
        
        return (
            [str(year) for year in years],
            [f'{month:02d}' for month in months],
            [f'{day:02d}' for day in days]
        )
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia y formatea el DataFrame.
        
        Args:
            df: DataFrame a limpiar
            
        Returns:
            DataFrame limpio
        """
        # Eliminar columnas innecesarias
        columns_to_drop = ['number', 'step', 'surface', 'valid_time', 'heightAboveGround']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        
        # Renombrar columnas comunes
        rename_dict = {
            't2m': 'temperature_2m',
            'time': 'datetime',
            'latitude': 'lat',
            'longitude': 'lon',
            't': 'temperature',
            'tp': 'total_precipitation'
        }
        df = df.rename(columns={col: rename_dict[col] for col in df.columns if col in rename_dict})
        
        # Convertir temperatura de Kelvin a Celsius si existe
        temp_cols = [col for col in df.columns if 'temperature' in col.lower()]
        for col in temp_cols:
            if df[col].max() > 200:  # Asumir que está en Kelvin si los valores son altos
                df[col] = df[col] - 273.15
        
        # Formatear datetime
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = df['datetime'].dt.date
            df['hour'] = df['datetime'].dt.hour
        
        return df

    def build_request_params(
        self,
        dataset: str,
        product_type: str,
        variable: str,
        start_date: str,
        end_date: str,
        latitude: float,
        longitude: float,
        radius: float = 0.5,
        format: str = 'netcdf'  # Cambiado a netcdf por defecto
    ) -> Dict:
        """
        Construye los parámetros para la solicitud CDS.
        
        Args:
            dataset: Nombre del dataset CDS
            product_type: Tipo de producto
            variable: Variable climática
            start_date: Fecha de inicio
            end_date: Fecha de fin
            latitude: Latitud del centroide
            longitude: Longitud del centroide
            radius: Radio alrededor del centroide en grados
            format: Formato de respuesta ('grib' o 'netcdf')
            
        Returns:
            Diccionario con parámetros de la solicitud
        """
        # Validar coordenadas
        if not (-90 <= latitude <= 90):
            raise ValueError("Latitud debe estar entre -90 y 90")
        if not (-180 <= longitude <= 180):
            raise ValueError("Longitud debe estar entre -180 y 180")
        
        # Generar listas de fechas
        years, months, days = self._generate_date_lists(start_date, end_date)
        
        # Calcular área alrededor del centroide
        north = min(latitude + radius, 90)
        south = max(latitude - radius, -90)
        west = max(longitude - radius, -180)
        east = min(longitude + radius, 180)
        
        params = {
            'product_type': product_type,
            'variable': variable,
            'year': years,
            'month': months,
            'day': days,
            'time': [f'{h:02d}:00' for h in range(24)],
            'area': [north, west, south, east],  # North, West, South, East
            'format': format
        }
        
        logger.info(f"Parámetros de solicitud construidos para área: {north}N, {west}W, {south}S, {east}E")
        logger.info(f"Rango temporal: {start_date} a {end_date}")
        return params
    
    def download_data(
        self,
        dataset: str = 'reanalysis-era5-single-levels',
        product_type: str = 'reanalysis',
        variable: str = '2m_temperature',
        start_date: str = '2023-01-01',
        end_date: str = '2023-01-31',
        latitude: float = 40.4168,
        longitude: float = -3.7038,
        radius: float = 0.5,
        output_format: str = 'netcdf'
    ) -> Optional[str]:
        """
        Descarga datos del CDS para un área alrededor de un centroide.
        
        Args:
            dataset: Dataset CDS a consultar
            product_type: Tipo de producto
            variable: Variable climática
            start_date: Fecha de inicio
            end_date: Fecha de fin
            latitude: Latitud del centroide
            longitude: Longitud del centroide
            radius: Radio alrededor del centroide en grados
            output_format: Formato de salida ('grib' o 'netcdf')
            
        Returns:
            Ruta al archivo descargado o None si hay error
        """
        try:
            # Validar rango de fechas
            start_date, end_date = self.validate_date_range(start_date, end_date)
            
            # Construir parámetros
            params = self.build_request_params(
                dataset, product_type, variable, start_date, end_date,
                latitude, longitude, radius, output_format
            )
            
            # Crear directorio de salida

            raw_dir = Path(f"{cfg.DATA_RAW}/copernicus")
            raw_dir.mkdir(parents=True, exist_ok=True)
            
            # Nombre del archivo de salida
            filename = f"{dataset}_{start_date}_{end_date}_{latitude}_{longitude}.{output_format}"
            output_path = raw_dir / filename
            
            logger.info(f"Iniciando descarga de datos para {variable}...")
            logger.info(f"Parámetros de la solicitud: {params}")
            
            # Realizar solicitud
            self.client.retrieve(
                dataset,
                params,
                str(output_path)
            )
            
            logger.info(f"Datos descargados correctamente en: {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error en la descarga de datos: {e}")
            return None
    
    def process_netcdf_to_dataframe(self, netcdf_path: str, latitude: float, longitude: float) -> Optional[pd.DataFrame]:   
        """
        Procesa archivo NetCDF y extrae datos para el punto de coordenadas.
        
        Args:
            netcdf_path: Ruta al archivo NetCDF
            latitude: Latitud del punto de interés
            longitude: Longitud del punto de interés
            
        Returns:
            DataFrame con los datos procesados o None si hay error
        """
        try:
            logger.info(f"Procesando archivo NetCDF: {netcdf_path}")
            
            # Leer archivo NetCDF con xarray
            ds = xr.open_dataset(netcdf_path)
            
            # Seleccionar el punto más cercano a las coordenadas dadas
            ds_point = ds.sel(
                latitude=latitude,
                longitude=longitude,
                method='nearest'
            )
            
            # Convertir a DataFrame
            df = ds_point.to_dataframe().reset_index()
            
            # Limpiar y formatear el DataFrame
            # df = self._clean_dataframe(df)
            
            # Guardar DataFrame procesado
            processed_dir = Path('../data/raw/copernicus')
            processed_dir.mkdir(parents=True, exist_ok=True)
            
            csv_filename = Path(netcdf_path).stem + '.csv'
            csv_path = processed_dir / csv_filename
            
            df.to_csv(csv_path, index=False)
            logger.info(f"Datos procesados guardados en: {csv_path}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error al procesar archivo NetCDF: {e}")
            return None
    
    def process_grib_to_dataframe(self, grib_path: str, latitude: float, longitude: float) -> Optional[pd.DataFrame]:
        """
        Procesa archivo GRIB y extrae datos para el punto de coordenadas.
        
        Args:
            grib_path: Ruta al archio GRIB
            latitude: Latitud del punto de interés
            longitude: Longitud del punto de interés
            
        Returns:
            DataFrame con los datos procesados o None si hay error
        """
        try:
            logger.info(f"Procesando archivo GRIB: {grib_path}")
            
            # Leer archivo GRIB con xarray usando cfgrib
            ds = xr.open_dataset(grib_path, engine='cfgrib')
            
            # Seleccionar el punto más cercano a las coordenadas dadas
            ds_point = ds.sel(
                latitude=latitude,
                longitude=longitude,
                method='nearest'
            )
            
            # Convertir a DataFrame
            df = ds_point.to_dataframe().reset_index()
            
            # Limpiar y formatear el DataFrame
            df = self._clean_dataframe(df)
            
            # Guardar DataFrame procesado
            processed_dir = Path('./data/processed')
            processed_dir.mkdir(parents=True, exist_ok=True)
            
            csv_filename = Path(grib_path).stem + '.csv'
            csv_path = processed_dir / csv_filename
            
            df.to_csv(csv_path, index=False)
            logger.info(f"Datos procesados guardados en: {csv_path}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error al procesar archivo GRIB: {e}")
            # Intentar con NetCDF como fallback
            logger.info("Intentando descargar en formato NetCDF como fallback...")
            return None
    
    def get_climate_data(
        self,
        variable: str = '2m_temperature',
        start_date: str = '2023-01-01',
        end_date: str = '2023-01-31',
        latitude: float = 40.4168,
        longitude: float = -3.7038,
        radius: float = 0.5,
        format: str = 'netcdf'  # Usar NetCDF por defecto
    ) -> Optional[pd.DataFrame]:
        """
        Método principal para obtener y procesar datos climáticos.
        
        Args:
            variable: Variable climática a obtener
            start_date: Fecha de inicio
            end_date: Fecha de fin
            latitude: Latitud del centroide
            longitude: Longitud del centroide
            radius: Radio alrededor del centroide
            format: Formato de descarga ('grib' o 'netcdf')
            
        Returns:
            DataFrame con datos procesados o None si hay error
        """
        try:
            # Descargar datos
            file_path = self.download_data(
                variable=variable,
                start_date=start_date,
                end_date=end_date,
                latitude=latitude,
                longitude=longitude,
                radius=radius,
                output_format=format
            )
            
            if not file_path:
                return None
            
            # Procesar datos según el formato
            if format.lower() == 'grib':
                df = self.process_grib_to_dataframe(file_path, latitude, longitude)
            else:  # netcdf por defecto
                df = self.process_netcdf_to_dataframe(file_path, latitude, longitude)
            
            return df
            
        except Exception as e:
            logger.error(f"Error en get_climate_data: {e}")
            return None

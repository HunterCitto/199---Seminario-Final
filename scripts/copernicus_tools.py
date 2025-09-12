import cdsapi
import xarray as xr
import pandas as pd
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)

class CopernicusTools:
    def __init__(self, config):
        self.config = config
        self.client = cdsapi.Client()
        self.raw_data_path = Path(f"{config.DATA_RAW}/copernicus")
    
    def get_meteorological_data(self):
        """Obtiene datos meteorológicos completos"""
        variables = [
            '2m_temperature' ]
        return self.__download_era5_levels_data(variables)

    def __download_era5_land_data(self, variables):
        """Descarga datos ERA5-Land para variables específicas"""
        try:
            logger.info(f"Iniciando descarga de datos ERA5-Land: {variables}")
            
            filename = f"era5_land_{self.config.START_DATE}_{self.config.END_DATE}.nc"
            filepath = self.raw_data_path / filename
            
            if filepath.exists():
                logger.info(f"Archivo ya existe: {filepath}")
                return self.__load_netcdf_file(filepath)
            
            request_params = {
                'product_type': 'reanalysis',
                'variable': variables,
                'date': f'{self.config.START_DATE}/{self.config.END_DATE}',
                'area': [self.config.BBOX[3], self.config.BBOX[0], 
                            self.config.BBOX[1], self.config.BBOX[2]],  # [N, O, S, E]
                'time': ['00:00', '06:00', '12:00', '18:00'],
                'format': 'netcdf'
            }
            
            logger.debug(f"Parámetros de solicitud: {request_params}")
            self.client.retrieve('reanalysis-era5-land', request_params, str(filepath))
            
            logger.info(f"Datos descargados exitosamente: {filepath}")
            return self.__load_netcdf_file(filepath)

        except Exception as e:
            logger.error(f"Error descargando datos ERA5-Land: {e}")
            return None

    def __download_era5_levels_data(self, variables):
        """Descarga datos ERA5-Land para variables específicas"""
        try:
            logger.info(f"Iniciando descarga de datos ERA5-Land: {variables}")
            
            filename = f"era5_land_{self.config.START_DATE}_{self.config.END_DATE}.nc"
            filepath = self.raw_data_path / filename
            
            if filepath.exists():
                logger.info(f"Archivo ya existe: {filepath}")
                return self.__load_netcdf_file(filepath)
            
            request_params = {
                'product_type': 'reanalysis',
                'variable': variables,
                'date': f'{self.config.START_DATE}/{self.config.END_DATE}',
                'area': [self.config.BBOX[3], self.config.BBOX[0], 
                            self.config.BBOX[1], self.config.BBOX[2]],  # [N, O, S, E]
                'time': ['00:00', '06:00', '12:00', '18:00'],
                'format': 'netcdf'
            }
            
            logger.debug(f"Parámetros de solicitud: {request_params}")
            self.client.retrieve('reanalysis-era5-single-levels', request_params, str(filepath))
            
            logger.info(f"Datos descargados exitosamente: {filepath}")
            return self.__load_netcdf_file(filepath)

        except Exception as e:
            logger.error(f"Error descargando datos ERA5-Land: {e}")
            return None
    
    def __load_netcdf_file(self, filepath):
        """Carga archivo NetCDF o GRIB con el engine apropiado"""
        engines = ['netcdf4', 'h5netcdf', 'scipy', 'cfgrib']
        for eng in engines:
            try:
                ds = xr.open_dataset(filepath, engine=eng)
                logger.info(f"Archivo cargado con engine {eng}: {filepath}")
                return ds
            except Exception as e:
                logger.debug(f"No se pudo abrir con {eng}: {e}")
        logger.error(f"No se pudo abrir el archivo con ninguno de los engines: {filepath}")
        return None

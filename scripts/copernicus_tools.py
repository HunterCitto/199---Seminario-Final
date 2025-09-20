import cdsapi
import xarray as xr
import pandas as pd
import logging
import os
from pathlib import Path
from project_config import ProjectConfig as cfg

logger = logging.getLogger(__name__)

class CopernicusTools:
    def __init__(self) :
        self.config = cfg
        self.client = cdsapi.Client()
        self.raw_data_path = Path(f"{cfg.DATA_RAW}/copernicus")
    
    def get_meteorological_data(self, mode = 'land'):
        """Obtiene datos meteorológicos completos"""

        if mode == 'land':
            return self.__download_era5_land_data()
        elif mode == 'levels':
            return self.__download_era5_levels_data()

    # LAND SIGUE SIN FUNCIONAR.
    def __download_era5_land_data(self):
        """Descarga datos ERA5-Land para variables específicas"""

        variables = [
            "2m_temperature"
        ]

        try:
            logger.info(f"Iniciando descarga de datos ERA5-Land: {variables}")
            
            filename = f"era5_land_{self.config.START_DATE}_{self.config.END_DATE}.nc"
            filepath = self.raw_data_path / filename
            
            if filepath.exists():
                logger.info(f"Archivo ya existe: {filepath}")
                return self.__load_file(filepath)
            
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
            return self.__load_file(filepath)

        except Exception as e:
            logger.error(f"Error descargando datos ERA5-Land: {e}")
            return None

    # EESTE FUNCIONA-
    def __download_era5_levels_data(self):
        """Descarga datos ERA5-Levels para variables específicas"""

        variables = [
            "2m_temperature",
            "snow_depth",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "surface_pressure",
            "leaf_area_index_high_vegetation",
            "leaf_area_index_low_vegetation"
        ]

        try:
            logger.info(f"Iniciando descarga de datos ERA5-Levels: {variables}")

            filename = f"era5_levels_{self.config.START_DATE}_{self.config.END_DATE}.nc"
            filepath = self.raw_data_path / filename
            
            if filepath.exists():
                logger.info(f"Archivo ya existe: {filepath}")
                return self.__load_file(filepath)
            
            request_params = {
                'product_type': 'reanalysis',
                'variable': variables,
                'date': f'{self.config.START_DATE}/{self.config.END_DATE}',
                'area': [self.config.BBOX[3], self.config.BBOX[0], 
                            self.config.BBOX[1], self.config.BBOX[2]],  # [N, O, S, E]
                'time': ['00:00', '06:00', '12:00', '18:00'],
                'format': 'grib'
            }
            
            logger.debug(f"Parámetros de solicitud: {request_params}")
            self.client.retrieve('reanalysis-era5-single-levels', request_params, str(filepath))
            
            logger.info(f"Datos descargados exitosamente: {filepath}")
            return self.__load_file(filepath)

        except Exception as e:
            logger.error(f"Error descargando datos ERA5-Land: {e}")
            return None
    
    def __load_file(self, filepath):
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

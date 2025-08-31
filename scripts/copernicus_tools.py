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
        self.raw_data_path = Path("data/raw/copernicus")
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
    
    def download_era5_land_data(self, variables):
        """Descarga datos ERA5-Land para variables específicas"""
    # try:
        logger.info(f"Iniciando descarga de datos ERA5-Land: {variables}")
        
        filename = f"era5_land_{self.config.START_DATE}_{self.config.END_DATE}.nc"
        filepath = self.raw_data_path / filename
        
        if filepath.exists():
            logger.info(f"Archivo ya existe: {filepath}")
            return self._load_netcdf_file(filepath)
        
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
        return self._load_netcdf_file(filepath)
        
    # except Exception as e:
    #     logger.error(f"Error descargando datos ERA5-Land: {e}")
    #     return None
    
    def _load_netcdf_file(self, filepath):
        """Carga archivo NetCDF con el engine apropiado"""
        try:
            # Intentar con diferentes engines
            try:
                ds = xr.open_dataset(filepath, engine='netcdf4')
                logger.info(f"Archivo cargado con engine netcdf4: {filepath}")
                return ds
            except:
                try:
                    ds = xr.open_dataset(filepath, engine='h5netcdf')
                    logger.info(f"Archivo cargado con engine h5netcdf: {filepath}")
                    return ds
                except:
                    ds = xr.open_dataset(filepath)
                    logger.info(f"Archivo cargado con engine por defecto: {filepath}")
                    return ds
        except Exception as e:
            logger.error(f"Error cargando archivo NetCDF {filepath}: {e}")
            return None
    
    def get_meteorological_data(self):
        """Obtiene datos meteorológicos completos"""
        variables = [
            '2m_temperature', 'total_precipitation',
            '2m_dewpoint_temperature', '10m_u_component_of_wind',
            '10m_v_component_of_wind', 'soil_temperature_level_1',
            'volumetric_soil_water_layer_1', 'surface_pressure'
        ]
        return self.download_era5_land_data(variables)
    
    def calculate_fire_weather_index(self, meteo_data):
        """Calcula índices de riesgo de incendio basados en datos meteorológicos"""
        try:
            logger.info("Calculando índices de riesgo de incendio")
            return meteo_data
        except Exception as e:
            logger.error(f"Error calculando índices de incendio: {e}")
            return None
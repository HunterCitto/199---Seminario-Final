import ee
import geemap
import pandas as pd
from pathlib import Path
import logging
from project_config import ProjectConfig, DATA_RAW

logger = logging.getLogger(__name__)

class GEETools:
    def __init__(self, config):
        self.config = config
        self.raw_data_path = DATA_RAW / "gee"
        self.raw_data_path.mkdir(exist_ok=True)
        
        # Inicializar Earth Engine
        try:
            ee.Initialize()
            logger.info("Google Earth Engine inicializado correctamente")
        except Exception as e:
            logger.warning(f"Error inicializando GEE: {e}. Intentando autenticación...")
            ee.Authenticate()
            ee.Initialize()
        
        # Definir región de interés
        self.region = ee.Geometry.Rectangle([
            self.config.BBOX[0], self.config.BBOX[1],
            self.config.BBOX[2], self.config.BBOX[3]
        ])
    
    def get_vegetation_indices(self, index_name="NDVI", collection='COPERNICUS/S2_SR'):
        """Obtiene índices de vegetación"""
        try:
            logger.info(f"Obteniendo índice {index_name} de {collection}")
            
            # Filtrar colección
            image_collection = ee.ImageCollection(collection) \
                .filterBounds(self.region) \
                .filterDate(self.config.START_DATE, self.config.END_DATE) \
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
            
            # Calcular índice según especificación
            if index_name == "NDVI":
                def calculate_ndvi(img):
                    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
                    return img.addBands(ndvi)
                collection_with_index = image_collection.map(calculate_ndvi)
            
            elif index_name == "NBR":
                def calculate_nbr(img):
                    nbr = img.normalizedDifference(['B8', 'B12']).rename('NBR')
                    return img.addBands(nbr)
                collection_with_index = image_collection.map(calculate_nbr)
            
            else:
                logger.error(f"Índice no soportado: {index_name}")
                return None
            
            return collection_with_index.select(index_name)
            
        except Exception as e:
            logger.error(f"Error obteniendo índice de vegetación: {e}")
            return None
    
    def get_pre_post_fire_analysis(self):
        """Análisis de cambio pre y post incendio"""
        try:
            logger.info("Realizando análisis pre/post incendio")
            
            # Imágenes pre-incendio (antes del período de incendio)
            pre_fire = ee.ImageCollection('COPERNICUS/S2_SR') \
                .filterBounds(self.region) \
                .filterDate('2024-10-01', self.config.START_DATE) \
                .median()
            
            # Imágenes post-incendio (después del período de incendio)
            post_fire = ee.ImageCollection('COPERNICUS/S2_SR') \
                .filterBounds(self.region) \
                .filterDate(self.config.END_DATE, '2025-04-30') \
                .median()
            
            # Calcular dNBR (diferencial Normalized Burn Ratio)
            pre_nbr = pre_fire.normalizedDifference(['B8', 'B12']).rename('pre_nbr')
            post_nbr = post_fire.normalizedDifference(['B8', 'B12']).rename('post_nbr')
            dnbr = pre_nbr.subtract(post_nbr).rename('dnbr')
            
            logger.info("Análisis pre/post incendio completado")
            return dnbr
            
        except Exception as e:
            logger.error(f"Error en análisis pre/post incendio: {e}")
            return None
    
    def export_to_drive(self, image, description, folder="wildfire_analysis"):
        """Exporta resultados a Google Drive"""
        try:
            task = ee.batch.Export.image.toDrive(
                image=image,
                description=description,
                folder=folder,
                region=self.region,
                scale=100  # 100m resolution
            )
            task.start()
            logger.info(f"Tarea de exportación iniciada: {description}")
            return task
        except Exception as e:
            logger.error(f"Error iniciando exportación: {e}")
            return None
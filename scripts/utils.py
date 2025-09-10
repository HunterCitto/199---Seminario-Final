import os
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import logging

from datetime import datetime, timedelta
from shapely.geometry import Point

logger = logging.getLogger(__name__)

def dataframe_to_csv(df, path):
    """Guarda un DataFrame en CSV. Chequea existencia de carpeta."""
    try:
        folder = os.path.dirname(path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info(f"DataFrame guardado en {path}")
    except Exception as e:
        logger.error(f"Error guardando DataFrame en CSV: {e}")

def linear_sep_data(n = 100, p = 2):
    x = np.random.uniform(0, 1, (n, p))
    w = np.ones((p, 1))
    y = np.sign(x @ w)
    return x, y

def create_geodataframe_from_csv(csv_path, lat_col='latitude', lon_col='longitude'):
    """Convierte un CSV con coordenadas a GeoDataFrame"""
    try:
        df = pd.read_csv(csv_path)
        geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        logger.info(f"GeoDataFrame creado desde {csv_path} con {len(gdf)} registros")
        return gdf
    except Exception as e:
        logger.error(f"Error creando GeoDataFrame: {e}")
        return None

def save_plot(fig, filename, subfolder=""):
    """Guarda gráficos en la carpeta docs"""
    from scripts.config import BASE_DIR

    plot_path = BASE_DIR / "docs" / subfolder / filename
    plot_path.parent.mkdir(exist_ok=True)
    fig.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    logger.info(f"Gráfico guardado: {plot_path}")

### ------------------------------------- ###
### HERRAMIENTAS DE ANÁLISIS DE FUEGO.
### ------------------------------------- ###

def calculate_fire_progression(fires_gdf, date_col='acq_date'):
    """Calcula la progresión temporal del incendio"""
    try:
        fires_gdf[date_col] = pd.to_datetime(fires_gdf[date_col])
        daily_progression = fires_gdf.groupby(date_col).agg({
            'frp': ['sum', 'mean', 'count'],  # Radiative power
            'confidence': 'mean'
        }).round(2)
        
        daily_progression.columns = ['frp_total', 'frp_mean', 'hotspot_count', 'confidence_mean']
        logger.info("Progresión de incendio calculada exitosamente")
        return daily_progression
    except Exception as e:
        logger.error(f"Error calculando progresión de incendio: {e}")
        return None
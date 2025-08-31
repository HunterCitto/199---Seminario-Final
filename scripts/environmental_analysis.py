import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

def integrate_environmental_data(fire_data, env_data_path):
    """Integra datos de incendios con variables ambientales"""
    # Cargar datos ambientales (ejemplo: temperatura, humedad, viento)
    env_data = pd.read_csv(env_data_path)
    
    # Unir datos (asumiendo columnas de fecha y ubicación)
    merged_data = pd.merge(fire_data, env_data, 
                          on=['acq_date', 'latitude', 'longitude'],
                          how='left')
    
    return merged_data

def cluster_fire_events(fire_data):
    """Agrupa incendios en clusters para análisis de patrones"""
    # Seleccionar variables para clustering
    cluster_vars = ['brightness', 'frp', 'confidence', 'bright_t31']
    X = fire_data[cluster_vars].dropna()
    
    # Estandarizar
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Clustering
    dbscan = DBSCAN(eps=0.5, min_samples=5)
    fire_data['cluster'] = dbscan.fit_predict(X_scaled)
    
    return fire_data

def analyze_fire_intensity(fire_data):
    """Analiza la intensidad y comportamiento del fuego"""
    # Clasificar intensidad del incendio
    conditions = [
        (fire_data['frp'] < 50),
        (fire_data['frp'] < 200),
        (fire_data['frp'] < 500),
        (fire_data['frp'] >= 500)
    ]
    choices = ['Leve', 'Moderado', 'Intenso', 'Extremo']
    
    fire_data['intensity_category'] = np.select(conditions, choices)
    
    # Estadísticas por categoría
    intensity_stats = fire_data.groupby('intensity_category').agg({
        'frp': ['mean', 'std', 'count'],
        'brightness': 'mean',
        'confidence': 'mean'
    }).round(2)
    
    return fire_data, intensity_stats
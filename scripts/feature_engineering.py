import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder

def create_ml_features(fire_data):
    """Crea features para modelo de ML"""
    features = fire_data.copy()
    
    # 1. Features temporales
    features['acq_datetime'] = pd.to_datetime(
        features['acq_date'] + ' ' + features['acq_time'].astype(str).str.zfill(4),
        format='%Y-%m-%d %H%M'
    )
    features['hour_sin'] = np.sin(2 * np.pi * features['acq_datetime'].dt.hour / 24)
    features['hour_cos'] = np.cos(2 * np.pi * features['acq_datetime'].dt.hour / 24)
    features['day_of_year_sin'] = np.sin(2 * np.pi * features['acq_datetime'].dt.dayofyear / 365)
    features['day_of_year_cos'] = np.cos(2 * np.pi * features['acq_datetime'].dt.dayofyear / 365)
    
    # 2. Features de intensidad
    features['frp_log'] = np.log1p(features['frp'])
    features['brightness_ratio'] = features['brightness'] / features['bright_t31']
    
    # 3. Features espaciales (podrías agregar elevación, uso de suelo, etc.)
    features['spatial_cluster'] = create_spatial_clusters(features)
    
    # 4. Features categóricas
    le = LabelEncoder()
    features['satellite_encoded'] = le.fit_transform(features['satellite'])
    features['is_night'] = (features['daynight'] == 'N').astype(int)
    
    return features

def create_spatial_clusters(fire_data, eps=0.1, min_samples=3):
    """Crea clusters espaciales usando DBSCAN"""
    from sklearn.cluster import DBSCAN
    from sklearn.preprocessing import StandardScaler
    
    coords = fire_data[['latitude', 'longitude']].values
    scaler = StandardScaler()
    coords_scaled = scaler.fit_transform(coords)
    
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    return dbscan.fit_predict(coords_scaled)
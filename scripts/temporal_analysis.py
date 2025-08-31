import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_temporal_patterns(fire_data):
    """Analiza patrones temporales de los incendios"""
    # Convertir a datetime
    fire_data['acq_datetime'] = pd.to_datetime(
        fire_data['acq_date'] + ' ' + fire_data['acq_time'].astype(str).str.zfill(4),
        format='%Y-%m-%d %H%M'
    )
    
    # Extraer características temporales
    fire_data['hour'] = fire_data['acq_datetime'].dt.hour
    fire_data['day_of_year'] = fire_data['acq_datetime'].dt.dayofyear
    fire_data['is_night'] = fire_data['daynight'] == 'N'
    
    # Análisis diario
    daily_analysis = fire_data.groupby('acq_date').agg({
        'frp': ['sum', 'mean', 'count'],
        'brightness': 'mean',
        'confidence': 'mean'
    }).round(2)
    
    return fire_data, daily_analysis

def plot_temporal_analysis(fire_data, save_path=None):
    """Visualiza análisis temporal"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # Evolución diaria de FRP
    daily_frp = fire_data.groupby('acq_date')['frp'].sum()
    daily_frp.plot(ax=ax1, marker='o')
    ax1.set_title('Evolución Diaria de la Energía Radiante Total')
    ax1.set_ylabel('FRP Total (MW)')
    
    # Distribución horaria
    hourly_dist = fire_data.groupby('hour').size()
    hourly_dist.plot(ax=ax2, kind='bar', color='orange')
    ax2.set_title('Distribución de Incendios por Hora del Día')
    ax2.set_xlabel('Hora')
    ax2.set_ylabel('Número de detecciones')
    
    # Relación FRP vs Brightness
    ax3.scatter(fire_data['brightness'], fire_data['frp'], alpha=0.6)
    ax3.set_title('Relación entre Temperatura y Energía Radiante')
    ax3.set_xlabel('Temperatura (Kelvin)')
    ax3.set_ylabel('FRP (MW)')
    
    # Distribución de confianza
    fire_data['confidence'].hist(ax=ax4, bins=20)
    ax4.set_title('Distribución de Niveles de Confianza')
    ax4.set_xlabel('Confianza (%)')
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300)
    plt.show()
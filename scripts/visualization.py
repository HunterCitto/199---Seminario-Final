import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_interactive_fire_map(fire_data):
    """Crea mapa interactivo para la tesis"""
    fig = px.scatter_mapbox(fire_data, 
                           lat="latitude", 
                           lon="longitude", 
                           color="frp",
                           size="frp",
                           hover_data=["acq_date", "confidence", "satellite"],
                           color_continuous_scale="YlOrRd",
                           zoom=8,
                           title="Distribución de Incendios - Patagonia Andina")
    
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
    return fig

def create_time_series_analysis(fire_data):
    """Análisis de series temporales interactivo"""
    daily_summary = fire_data.groupby('acq_date').agg({
        'frp': 'sum',
        'brightness': 'mean',
        'confidence': 'mean',
        'latitude': 'count'
    }).reset_index()
    
    fig = make_subplots(rows=2, cols=2, subplot_titles=(
        'Energía Radiante Total Diaria', 
        'Temperatura Promedio',
        'Confianza Promedio', 
        'Número de Detecciones'
    ))
    
    fig.add_trace(go.Scatter(x=daily_summary['acq_date'], y=daily_summary['frp']), row=1, col=1)
    fig.add_trace(go.Scatter(x=daily_summary['acq_date'], y=daily_summary['brightness']), row=1, col=2)
    fig.add_trace(go.Scatter(x=daily_summary['acq_date'], y=daily_summary['confidence']), row=2, col=1)
    fig.add_trace(go.Bar(x=daily_summary['acq_date'], y=daily_summary['latitude']), row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False)
    return fig
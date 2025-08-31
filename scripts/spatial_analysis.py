import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Point
import contextily as ctx
from project_config import ProjectConfig
import logging
# import scikit-learn modules as needed

logger = logging.getLogger(__name__)

class SpatialAnalysis :

    def __init__(self, fire_data) :
        self.fire_data = fire_data
        self.gdf = None
        
    def create_geodataframe(self) :
        """Convierte los datos a GeoDataFrame"""
        try:
            geometry = [Point(xy) for xy in zip(self.fire_data.longitude, self.fire_data.latitude)]
            self.gdf = gpd.GeoDataFrame(self.fire_data, geometry=geometry, crs="EPSG:4326")
            logger.info(f"GeoDataFrame creado con {len(self.gdf)} registros")
            return self.gdf
        except Exception as e:
            logger.error(f"Error creando GeoDataFrame: {e}")
            return None
    
    def calculate_basic_stats(self) :
        """Calcula estadísticas espaciales básicas"""
        if self.gdf is None:
            self.create_geodataframe()
        
        stats = {
            'total_detections': len(self.gdf),
            'area_covered_km2': self.calculate_area_covered(),
            'mean_frp': self.gdf['frp'].mean(),
            'max_frp': self.gdf['frp'].max(),
            'mean_confidence': self.gdf['confidence'].mean(),
            'detections_per_day': len(self.gdf) / self.gdf['acq_date'].nunique()
        }
        
        return stats
    
    def calculate_area_covered(self) :
        """Calcula el área aproximada cubierta por los incendios"""
        if len(self.gdf) <= 1:
            return 0
        
        # Crear un convex hull y calcular área
        convex_hull = self.gdf.unary_union.convex_hull
        # Convertir a CRS métrico para calcular área en km²
        gdf_metric = self.gdf.to_crs(epsg=5348)  # CRS para Argentina
        area_km2 = gdf_metric.unary_union.convex_hull.area / 1e6
        
        return round(area_km2, 2)
    
    def plot_spatial_distribution(self, save_path = None) :
        """Crea visualización de la distribución espacial"""
        if self.gdf is None:
            self.create_geodataframe()
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Mapa 1: Distribución por FRP
        scatter1 = ax1.scatter(self.gdf.geometry.x, self.gdf.geometry.y, 
                              c=self.gdf['frp'], cmap='YlOrRd', s=50, alpha=0.7)
        ax1.set_title('Distribución por Potencia Radiante (FRP)')
        ax1.set_xlabel('Longitud')
        ax1.set_ylabel('Latitud')
        plt.colorbar(scatter1, ax=ax1, label='FRP (MW)')
        
        # Mapa 2: Distribución por confianza
        scatter2 = ax2.scatter(self.gdf.geometry.x, self.gdf.geometry.y, 
                              c=self.gdf['confidence'], cmap='viridis', s=50, alpha=0.7)
        ax2.set_title('Distribución por Nivel de Confianza')
        ax2.set_xlabel('Longitud')
        ax2.set_ylabel('Latitud')
        plt.colorbar(scatter2, ax=ax2, label='Confianza (%)')
        
        # Mapa 3: Distribución día/noche
        day_night_colors = {'D': 'red', 'N': 'blue'}
        for dn, color in day_night_colors.items():
            mask = self.gdf['daynight'] == dn
            ax3.scatter(self.gdf.geometry.x[mask], self.gdf.geometry.y[mask], 
                       c=color, label=dn, s=40, alpha=0.6)
        ax3.set_title('Distribución Día/Noche')
        ax3.set_xlabel('Longitud')
        ax3.set_ylabel('Latitud')
        ax3.legend()
        
        # Mapa 4: Densidad de puntos
        from scipy.stats import gaussian_kde
        x, y = self.gdf.geometry.x, self.gdf.geometry.y
        xy = np.vstack([x, y])
        z = gaussian_kde(xy)(xy)
        scatter4 = ax4.scatter(x, y, c=z, cmap='plasma', s=40, alpha=0.7)
        ax4.set_title('Densidad de Detecciones')
        ax4.set_xlabel('Longitud')
        ax4.set_ylabel('Latitud')
        plt.colorbar(scatter4, ax=ax4, label='Densidad')
        
        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()

    def plot_interactive_map(self, save_path = None):
        """Crea mapa interactivo con folium"""
        import folium
        from folium.plugins import HeatMap
        
        # Centro del mapa (promedio de coordenadas)
        center_lat = self.gdf.latitude.mean()
        center_lon = self.gdf.longitude.mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=9)
        
        # Añadir heatmap
        heat_data = [[row['latitude'], row['longitude'], row['frp']] 
                    for _, row in self.gdf.iterrows()]
        HeatMap(heat_data, radius=15, blur=10, max_zoom=1).add_to(m)
        
        # Añadir marcadores para los puntos más intensos
        top_fires = self.gdf.nlargest(10, 'frp')
        for _, fire in top_fires.iterrows():
            folium.CircleMarker(
                location=[fire['latitude'], fire['longitude']],
                radius=fire['frp'] / 50,  # Tamaño proporcional al FRP
                popup=f"FRP: {fire['frp']} MW<br>Fecha: {fire['acq_date']}",
                color='red',
                fill=True
            ).add_to(m)
        
        if save_path:
            m.save(save_path)
        
        return m
    
    def cluster_analysis(self) :
        """Análisis de clusters espaciales"""
        from sklearn.cluster import DBSCAN
        from sklearn.preprocessing import StandardScaler
        
        # Coordenadas para clustering
        coords = self.gdf[['latitude', 'longitude']].values
        
        # Estandarizar
        scaler = StandardScaler()
        coords_scaled = scaler.fit_transform(coords)
        
        # DBSCAN clustering
        dbscan = DBSCAN(eps=0.3, min_samples=3)
        clusters = dbscan.fit_predict(coords_scaled)
        
        self.gdf['cluster'] = clusters
        
        # Estadísticas por cluster
        cluster_stats = self.gdf.groupby('cluster').agg({
            'frp': ['mean', 'sum', 'count'],
            'confidence': 'mean',
            'latitude': 'mean',
            'longitude': 'mean'
        }).round(2)
        
        return cluster_stats
    
    def export_results(self, output_path) :
        """Exporta resultados del análisis"""
        if self.gdf is None:
            self.create_geodataframe()
        
        # Guardar GeoJSON
        geojson_path = output_path / 'fire_locations.geojson'
        self.gdf.to_file(geojson_path, driver='GeoJSON')
        
        # Guardar estadísticas
        stats = self.calculate_basic_stats()
        stats_df = pd.DataFrame([stats])
        stats_path = output_path / 'spatial_stats.csv'
        stats_df.to_csv(stats_path, index=False)
        
        logger.info(f"Resultados exportados a {output_path}")
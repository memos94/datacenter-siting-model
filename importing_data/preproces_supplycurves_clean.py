import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path
from shapely.geometry import Point
from geopy.distance import geodesic
from cost_dict import water_price_region_dict

# --- CONFIGURACIÓN DE RUTAS ---
BASE_PATH = Path(r"C:\Users\z004we5c\Desktop\PowerBI\Pythonscripts")
RAW_DATA = BASE_PATH / "raw-data"
TELECOM_DATA = BASE_PATH / "telecom-data"
DELOITTE_DATA = BASE_PATH / "deloitte-data"

# Crear carpetas si no existen
for folder in [TELECOM_DATA, DELOITTE_DATA]:
    folder.mkdir(parents=True, exist_ok=True)

# --- FUNCIONES DE SOPORTE ---

def load_and_merge_supply():
    print("Cargando curvas de oferta...")
    solar = pd.read_csv(RAW_DATA / "open_access_2030_moderate_supply_curve.csv")
    wind = pd.read_csv(RAW_DATA / "open_access_2030_moderate_115hh_170rd_supply_curve.csv")
    geo = pd.read_csv(RAW_DATA / "egs_4500m_supply-curve.csv")

    # Renombrar columnas para consistencia
    solar = solar.rename(columns={'capacity_mw_ac': 'capacity_solar', 'mean_cf_ac': 'cf_solar', 'dist_km': 'dist_km_solar'})
    wind = wind.rename(columns={'capacity_mw': 'capacity_wind', 'mean_cf': 'cf_wind', 'dist_km': 'dist_km_wind'})
    geo = geo.rename(columns={'capacity_ac_mw': 'capacity_geo', 'capacity_factor_ac': 'cf_geo', 'dist_spur_km': 'dist_km_geo'})

    # Unión (Outer Merge) por ID de punto (sc_point_gid)
    df = pd.merge(solar, wind, on=['sc_point_gid'], how='outer', suffixes=('_solar', '_wind'))
    df = pd.merge(df, geo, on=['sc_point_gid'], how='outer')

    # Consolidar metadatos (llenar vacíos entre datasets)
    df['county'] = df['county_solar'].fillna(df['county_wind']).fillna(df['county'])
    df['state'] = df['state_solar'].fillna(df['state_wind']).fillna(df['state'])
    df['latitude'] = df['latitude_solar'].fillna(df['latitude_wind']).fillna(df['latitude'])
    df['longitude'] = df['longitude_solar'].fillna(df['longitude_wind']).fillna(df['longitude'])
    
    # Distancia de transmisión (convertir km a millas)
    df['trans_dist'] = df['dist_km_solar'].fillna(df['dist_km_wind']).fillna(df['dist_km_geo']) / 1.609
    
    return df.fillna(0)

def add_telecom_distance(df):
    print("Calculando distancias a nodos de telecomunicaciones...")
    nodes = pd.read_csv(TELECOM_DATA / "telecom_node_data.csv").dropna(subset=['lat', 'lng'])
    
    df = df.dropna(subset=['latitude', 'longitude'])
    df_points = list(zip(df.latitude, df.longitude))
    node_points = list(zip(nodes.lat, nodes.lng))

    def get_min_dist(p1):
        # Geodesic considera la curvatura de la tierra
        return min(geodesic(p1, p2).kilometers for p2 in node_points)

    df['telecom_dist'] = [get_min_dist(p) for p in df_points]
    return df

def add_water_prices(df):
    print("Asignando precios de agua por cercanía...")
    prices = pd.read_csv(DELOITTE_DATA / "water_prices_loc.csv").rename(columns={'lat': 'latitude', 'lng': 'longitude'})

    def set_price(row):
        coords = (row['latitude'], row['longitude'])
        state = row.get('state_id', row['state'])
        
        # Filtrar por mismo estado
        same_state = prices[prices['state'] == state]
        if not same_state.empty:
            dists = same_state.apply(lambda r: geodesic(coords, (r['latitude'], r['longitude'])).miles, axis=1)
            return same_state.loc[dists.idxmin(), 'price']
        return 0

    df['water_price'] = df.apply(set_price, axis=1)
    return df

def add_climate_zones(df):
    print("Realizando Join Espacial para zonas climáticas...")
    zones = gpd.read_file(RAW_DATA / "Climate_Zones" / "Climate_Zones.shp").dropna()
    
    # Crear zonas (Clima + Humedad)
    zones['clim_zone'] = zones['IECC_Clima'].astype(str) + zones['IECC_Moist']
    zones.loc[zones['IECC_Clima'].isin([7, 8]), 'clim_zone'] = zones['IECC_Clima'].astype(str)
    zones = zones[['geometry', 'clim_zone']]

    # Conversión a GeoDataFrame
    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    
    # Spatial Join (Intersección)
    joined = gpd.sjoin(gdf_points, zones, how="left", predicate="intersects")

    # Manejar puntos fuera de polígonos (asignar el más cercano)
    if joined['clim_zone'].isna().any():
        unmatched = joined[joined['clim_zone'].isna()].copy().drop(columns=['index_right', 'clim_zone'])
        nearest = gpd.sjoin_nearest(unmatched, zones, how="left", distance_col="dist")
        joined.loc[joined['clim_zone'].isna(), 'clim_zone'] = nearest['clim_zone_right'].values

    return joined.drop(columns=['geometry', 'index_right'])

# --- PROCESO PRINCIPAL ---

if __name__ == "__main__":
    # 1. Energía y Telecom
    supply_df = load_and_merge_supply()
    supply_df = add_telecom_distance(supply_df)
    
    # 2. Precios de Agua
    # Nota: Aquí puedes añadir la lógica de FIPS si es necesario antes del precio
    supply_df = add_water_prices(supply_df)
    
    # 3. Zonas Climáticas
    final_df = add_climate_zones(supply_df)

    # 4. Limpieza final y guardado
    final_df = final_df.round(2)
    output_path = TELECOM_DATA / "supply_data_lat_lon_water_clim.csv"
    final_df.to_csv(output_path, index=False)
    
    print(f"¡Proceso completado! Archivo guardado en: {output_path}")
    print(final_df[['sc_point_gid', 'county', 'clim_zone', 'water_price']].tail())
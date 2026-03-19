import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from geopy.distance import geodesic
import matplotlib.pyplot as plt
from cost_dict import water_price_region_dict

# --- CONFIGURACIÓN DE RUTAS ---
# Cambia esta ruta base según tu entorno
BASE_PATH = Path(r"C:\Users\z004we5c\Desktop\PowerBI\Pythonscripts\Yondr\datacenter-siting-model\Data")
BASE_PATH2 = Path(r"C:\Users\z004we5c\Desktop\PowerBI\Pythonscripts\Yondr\datacenter-siting-model")
RAW_DATA = BASE_PATH 
TELECOM_DATA = BASE_PATH
DELOITTE_DATA = BASE_PATH
COUNTY_MAPS = BASE_PATH2 / "CountyMaps"

# Archivos de salida
PATH_SUPPLY_FINAL = TELECOM_DATA / "supply_data_lat_lon_water_clim.csv"

# --- FUNCIONES DE PROCESAMIENTO ---

def load_supply_curves():
    print("Cargando y uniendo curvas de oferta...")
    solar = pd.read_csv(BASE_PATH / "open_access_2030_moderate_supply_curve.csv")
    wind = pd.read_csv(BASE_PATH / "open_access_2030_moderate_115hh_170rd_supply_curve.csv")
    geo = pd.read_csv(BASE_PATH / "egs_4500m_supply-curve.csv")

    # Selección y renombramiento de columnas
    solar = solar[['latitude', 'longitude', 'county', 'sc_point_gid', 'capacity_mw_ac', 'mean_cf_ac', 'dist_km', 'reg_mult', 'state']]
    solar = solar.rename(columns={'capacity_mw_ac': 'capacity_solar', 'mean_cf_ac': 'cf_solar', 'dist_km': 'dist_km_solar'})
    
    wind = wind[['latitude', 'longitude', 'county', 'sc_point_gid', 'capacity_mw', 'mean_cf', 'dist_km', 'reg_mult', 'state']]
    wind = wind.rename(columns={'capacity_mw': 'capacity_wind', 'mean_cf': 'cf_wind', 'dist_km': 'dist_km_wind'})
    
    geo = geo[['latitude', 'longitude', 'county', 'sc_point_gid', 'capacity_ac_mw', 'capacity_factor_ac', 'dist_spur_km', 'state']]
    geo = geo.rename(columns={'capacity_ac_mw': 'capacity_geo', 'capacity_factor_ac': 'cf_geo', 'dist_spur_km': 'dist_km_geo'})

    # Unión de datasets
    df = pd.merge(solar, wind, on=['sc_point_gid'], how='outer', suffixes=('_solar', '_wind'))
    df = pd.merge(df, geo, on=['sc_point_gid'], how='outer')

    # Consolidación de columnas compartidas
    df['county'] = df['county_solar'].fillna(df['county_wind']).fillna(df['county'])
    df['latitude'] = df['latitude_solar'].fillna(df['latitude_wind']).fillna(df['latitude'])
    df['longitude'] = df['longitude_solar'].fillna(df['longitude_wind']).fillna(df['longitude'])
    df['state'] = df['state_solar'].fillna(df['state_wind']).fillna(df['state'])
    df['reg_mult'] = df['reg_mult_solar'].fillna(df['reg_mult_wind'])
    
    # Distancia de transmisión original (convertir km a millas)
    df['trans_dist_orig'] = df['dist_km_solar'].fillna(df['dist_km_wind']).fillna(df['dist_km_geo']) / 1.609

    return df.fillna(0)

def add_transmission_distance(df):
    print("Calculando distancias a líneas de transmisión reales...")
    trans_file = RAW_DATA / "Texas_Electric_Power_Transmission_Lines.geojson"
    if not trans_file.exists():
        print("Aviso: GeoJSON de transmisión no encontrado. Saltando...")
        return df

    transmission_data = gpd.read_file(trans_file).to_crs(epsg=3857)
    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326").to_crs(epsg=3857)

    def get_min_dist(point):
        return transmission_data.geometry.distance(point).min() / 1609.34 # Metros a Millas

    df['transmission_distance'] = gdf_points.geometry.apply(get_min_dist)
    return df

from scipy.spatial import KDTree

def add_telecom_distance(df):
    print("Calculando distancias a nodos de telecomunicaciones (Optimizado con KDTree)...")
    telecom_file = TELECOM_DATA / "telecom_node_data.csv"
    if not telecom_file.exists(): return df

    telecom_nodes = pd.read_csv(telecom_file).dropna(subset=['lat', 'lng'])
    
    # 1. Build a tree of all fiber node coordinates
    # We use (lat, lng) as the coordinate system
    node_coords = telecom_nodes[['lat', 'lng']].values
    tree = KDTree(node_coords)
    
    # 2. Query the tree for the nearest neighbor for every supply point
    supply_coords = df[['latitude', 'longitude']].values
    
    # distances is in "degrees" (Euclidean), which we convert to Kilometers
    # Note: 1 degree latitude is approx 111km
    distances, indices = tree.query(supply_coords)
    
    # For better precision on large distances, use a conversion factor or 
    # re-calculate only the winner with geodesic if needed.
    # Simple approximation:
    df['telecom_dist'] = distances * 111.0 
    
    return df

def add_water_prices(df):
    print("Asignando precios de agua...")
    water_prices = pd.read_csv(DELOITTE_DATA / "water_prices_loc.csv").rename(columns={'lat':'latitude', 'lng':'longitude'})
    
    def get_price(row):
        loc = (row.latitude, row.longitude)
        state = row.get('state_id', row.state)
        # Buscar en mismo estado
        same_state = water_prices[water_prices['state'] == state]
        if not same_state.empty:
            dists = same_state.apply(lambda r: geodesic(loc, (r.latitude, r.longitude)).miles, axis=1)
            return same_state.loc[dists.idxmin(), 'price']
        return 0

    df['water_price'] = df.apply(get_price, axis=1)
    return df

def add_climate_zones(df):
    print("Asignando zonas climáticas (Spatial Join)...")
    clim_path = RAW_DATA / "Climate_Zones" / "Climate_Zones.shp"
    if not clim_path.exists(): return df

    zones = gpd.read_file(clim_path).dropna()
    zones['clim_zone'] = zones['IECC_Clima'].astype(str) + zones['IECC_Moist']
    zones.loc[zones['IECC_Clima'].isin([7, 8]), 'clim_zone'] = zones['IECC_Clima'].astype(str)
    zones = zones[['geometry', 'clim_zone']]

    gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    joined = gpd.sjoin(gdf_points, zones, how="left", predicate="intersects")

    # Lógica para puntos fuera de polígonos (Nearest)
    if joined['clim_zone'].isna().any():
        unmatched = joined[joined['clim_zone'].isna()].copy().drop(columns=['index_right', 'clim_zone'])
        nearest = gpd.sjoin_nearest(unmatched, zones, how="left", distance_col="dist_zone")
        joined.loc[joined['clim_zone'].isna(), 'clim_zone'] = nearest['clim_zone_right'].values

    return joined.drop(columns=['geometry', 'index_right'])

# --- FLUJO PRINCIPAL ---

def main():
    # 1. Cargar energía base
    df = load_supply_curves()
    
    # 2. Agregar dimensiones espaciales e infraestructura
    df = add_transmission_distance(df)
    df = add_telecom_distance(df)
    
    # 3. Precios de agua (Requiere lat/lon)
    df = add_water_prices(df)
    
    # 4. Zonas Climáticas
    df = add_climate_zones(df)
    
    # 5. Limpieza final de columnas y guardado
    df = df.drop(columns=[
        'latitude_solar','latitude_wind', 'longitude_solar', 'longitude_wind', 
        'county_solar', 'county_wind', 'dist_km_solar', 'dist_km_wind', 
        'dist_km_geo', 'state_solar','state_wind'
    ], errors='ignore')
    
    df = df.rename(columns={'sc_point_gid': 'Locations'})
    df = df.round(2)
    
    df.to_csv(PATH_SUPPLY_FINAL, index=False)
    print(f"¡Éxito! Archivo guardado en: {PATH_SUPPLY_FINAL}")

if __name__ == "__main__":
    main()
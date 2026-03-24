import pandas as pd
import geopandas as gpd
import os

# Configuraciones
TEXAS_FIPS = '48'
OUTPUT_FOLDER = 'texas_data'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

file_paths = {
    'state_shapefile': 'cb_2022_us_state_20m.shp',
    'county_csv': 'county_data.csv',
    'supply_data': 'supply_data_lat_lon_water_clim5.csv',
    'merged_cf': 'merged_hourly_solar_wind_cf.csv',
    'demand_data': 'fake_demand.csv',
    'county2zone': 'county2zone.csv',
    'hierarchy': 'hierarchy.csv',
    'electric_prices': 'electric_prices.csv',
    'water_risk': 'water_risk.gpkg',
    'county_shapefile': 'cb_2018_us_county_5m.shp'
}

def filter_texas_data():
    print("Iniciando filtrado para Texas...")

    # 1. Supply Data (El archivo maestro de ubicaciones)
    supply = pd.read_csv(file_paths['supply_data'])
    supply['state_id'] = supply['state_id'].astype(str).str.zfill(2)
    tx_supply = supply[supply['state_id'] == TEXAS_FIPS].copy()
    tx_locations = tx_supply['FIPS'].unique() 
    tx_supply.to_csv(f"{OUTPUT_FOLDER}/tx_supply_data.csv", index=False)
    print(f"- Supply Data: {len(tx_supply)} ubicaciones encontradas.")

    # 2. Merged CF (El más pesado)
    print("- Filtrando Merged CF (esto puede tardar)...")
    cf_iter = pd.read_csv(file_paths['merged_cf'], chunksize=100000)
    tx_cf_list = [chunk[chunk['location'].isin(tx_locations)] for chunk in cf_iter]
    tx_cf = pd.concat(tx_cf_list)
    tx_cf.to_csv(f"{OUTPUT_FOLDER}/tx_merged_cf.csv", index=False)
    print(f"- Merged CF: Filtrado completado.")

    # 3. Archivos de Soporte (CSVs)
    # --- NUEVO: Filtrado de county_data.csv ---
    print("- Filtrando county_data.csv...")
    county_df = pd.read_csv(file_paths['county_csv'])
    # Filtramos por la columna 'state' usando 'TX'
    tx_county = county_df[county_df['state'] == 'TX'].copy()
    tx_county.to_csv(f"{OUTPUT_FOLDER}/tx_county_data.csv", index=False)
    print(f"- County Data: {len(tx_county)} condados encontrados.")

    # County 2 Zone
    c2z = pd.read_csv(file_paths['county2zone'])
    c2z['FIPS'] = c2z['FIPS'].astype(str).str.zfill(5)
    tx_c2z = c2z[c2z['FIPS'].str.startswith(TEXAS_FIPS)]
    tx_c2z.to_csv(f"{OUTPUT_FOLDER}/tx_county2zone.csv", index=False)

    # Hierarchy (BAs que están en Texas)
    valid_bas = tx_c2z['ba'].unique()
    hier = pd.read_csv(file_paths['hierarchy'])
    tx_hier = hier[hier['ba'].isin(valid_bas)]
    tx_hier.to_csv(f"{OUTPUT_FOLDER}/tx_hierarchy.csv", index=False)

    # Electric Prices
    prices = pd.read_csv(file_paths['electric_prices'])
    if 'state_id' in prices.columns:
        tx_prices = prices[prices['state_id'].astype(str).str.zfill(2) == TEXAS_FIPS]
    else:
        tx_prices = prices[prices['state'].str.contains('Texas', case=False)]
    tx_prices.to_csv(f"{OUTPUT_FOLDER}/tx_electric_prices.csv", index=False)

    # 4. Demand Data
    pd.read_csv(file_paths['demand_data']).to_csv(f"{OUTPUT_FOLDER}/tx_demand.csv", index=False)

    print(f"\n¡Éxito! Todos los archivos filtrados están en la carpeta '{OUTPUT_FOLDER}'.")

if __name__ == "__main__":
    filter_texas_data()
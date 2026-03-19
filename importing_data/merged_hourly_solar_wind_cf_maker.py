import h5py
import pandas as pd
import numpy as np
from pathlib import Path

# --- CONFIGURACIÓN DE RUTAS ---
# Ajusta la ruta base a tu carpeta de proyecto
base_path = Path(r"C:\Users\z004we5c\Desktop\PowerBI\Pythonscripts\Yondr\datacenter-siting-model\Data")
raw_data_dir = base_path
output_dir = base_path / "deloitte-data"

# Crear carpeta de salida si no existe
output_dir.mkdir(parents=True, exist_ok=True)

paths = {
    'solar': raw_data_dir / "UPV_COUNTY_SOLAR" /  "upv" /  "upv-limited_county.h5",
    'wind': raw_data_dir / "UPV_COUNTY_WIND" /  "wind-ons" / "wind-ons-limited_county.h5",
    'merged': output_dir / "merged_hourly_solar_wind_cf.csv"
}

def get_season(dt):
    month = dt.month
    if month in [12, 1, 2]: return 'Winter'
    elif month in [3, 4, 5]: return 'Spring'
    elif month in [6, 7, 8]: return 'Summer'
    else: return 'Fall'

def process_h5_to_seasonal(file_path, resource_name):
    print(f"Procesando {resource_name} desde {file_path.name}...")
    
    with h5py.File(file_path, 'r') as f:
        # 1. Leer y filtrar por año 2023
        index_raw = [i.decode('utf-8') for i in f['index_0'][:]]
        full_index = pd.to_datetime(index_raw)
        mask_2023 = full_index.year == 2023
        
        columns = [col.decode('utf-8') for col in f['columns'][:]]
        data_2023 = f['data'][mask_2023.nonzero()[0], :]
        
        df = pd.DataFrame(data_2023, columns=columns, index=full_index[mask_2023])

    # 2. Promediar clases por condado (formato 'clase|FIPS')
    # Extraemos el FIPS (segunda parte del split)
    location_keys = df.columns.str.split('|').str[1]
    df = df.groupby(location_keys, axis=1).mean()
    
    # 3. Agregar por Temporada y Hora
    df['season'] = df.index.map(get_season)
    df['hour_of_day'] = df.index.hour
    
    seasonal_df = df.groupby(['season', 'hour_of_day']).median()
    
    # Limpiar nombres de columnas (quitar prefijos 'p' y asegurar 5 dígitos)
    seasonal_df.columns = [str(col).replace('p', '').zfill(5) for col in seasonal_df.columns]
    
    # Convertir a formato largo (Melt)
    seasonal_df = seasonal_df.reset_index()
    melted = seasonal_df.melt(
        id_vars=['season', 'hour_of_day'], 
        var_name='location', 
        value_name=f'hourly_cf_{resource_name}'
    )
    
    return melted

# --- EJECUCIÓN DEL FLUJO ---

# Procesar ambos recursos
solar_long = process_h5_to_seasonal(paths['solar'], 'solar')
wind_long = process_h5_to_seasonal(paths['wind'], 'wind')

print("Combinando datos solares y eólicos...")

# Unir ambos dataframes
# Usamos 'outer' para no perder condados que solo tengan un tipo de recurso
merged_df = pd.merge(
    solar_long, 
    wind_long, 
    on=['season', 'hour_of_day', 'location'], 
    how='outer'
)

# --- RE-INDEXACIÓN DE HORAS ---
# El modelo espera una columna 'hour' continua (0, 1, 2... N)
# Ordenamos para asegurar que las estaciones sigan la secuencia lógica
season_order = {'Winter': 0, 'Spring': 1, 'Summer': 2, 'Fall': 3}
merged_df['season_rank'] = merged_df['season'].map(season_order)
merged_df = merged_df.sort_values(['location', 'season_rank', 'hour_of_day'])

# Crear el índice de hora secuencial por ubicación
merged_df['hour'] = merged_df.groupby('location').cumcount()

# --- LIMPIEZA FINAL ---
# Asegurar que la localización sea Entero (o String de 5 dígitos según prefieras)
merged_df['location'] = merged_df['location'].astype(int)

# Rellenar nulos con 0 (donde no hay viento o no hay sol)
merged_df.fillna(0, inplace=True)

# Seleccionar columnas finales
final_columns = ['location', 'hour', 'hourly_cf_solar', 'hourly_cf_wind']
merged_df = merged_df[final_columns]

# Exportar
merged_df.to_csv(paths['merged'], index=False)

print(f"¡Éxito! Archivo exportado en: {paths['merged']}")
print(f"Total de ubicaciones únicas: {merged_df['location'].nunique()}")
print(f"Total de horas por ubicación: {merged_df.groupby('location')['hour'].count().iloc[0]}")
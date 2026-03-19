import h5py
import pandas as pd
import numpy as np
from pathlib import Path

# Configuración de rutas
base_path = Path(r"C:\Users\z004we5c\Desktop\PowerBI\Pythonscripts")
wind_path = base_path / "Yondr" / "datacenter-siting-model"/ "Data"/ "UPV_COUNTY_WIND" /  "wind-ons" /"wind-ons-limited_county.h5"
wind_output_path = base_path / "Yondr" / "datacenter-siting-model"/ "Data" /"wind_hourly_2023.csv"

def get_season(dt):
    month = dt.month
    if month in [12, 1, 2]: return 'Winter'
    elif month in [3, 4, 5]: return 'Spring'
    elif month in [6, 7, 8]: return 'Summer'
    else: return 'Fall'

# 1. Carga y filtrado de datos brutos desde el .h5
print(f"Abriendo {wind_path}...")
with h5py.File(wind_path, 'r') as f:
    # Extraer timestamps e índices para 2023
    index_raw = [i.decode('utf-8') for i in f['index_0'][:]]
    full_index = pd.to_datetime(index_raw)
    mask_2023 = full_index.year == 2023
    
    # Extraer nombres de columnas (FIPS con prefijos)
    columns = [col.decode('utf-8') for col in f['columns'][:]]
    
    # Leer solo los datos de 2023
    data_2023 = f['data'][mask_2023.nonzero()[0], :]
    df_wind = pd.DataFrame(data_2023, columns=columns, index=full_index[mask_2023])

# 2. Limpieza de columnas (Promediar múltiples clases por condado)
# El formato suele ser 'clase|FIPS', tomamos el FIPS (posición 1 tras split)
print("Promediando clases por ubicación...")
location_keys = df_wind.columns.str.split('|').str[1]
df_wind = df_wind.groupby(location_keys, axis=1).mean()

# Guardar intermedio si es necesario
# df_wind.to_csv(wind_output_path, index=True)

# 3. Agregación Estacional
print("Calculando medianas estacionales...")
df_wind['season'] = df_wind.index.map(get_season)
df_wind['hour'] = df_wind.index.hour

# Agrupar por temporada y hora, calcular la mediana
seasonal_df = df_wind.groupby(['season', 'hour']).median()

# Ordenar estaciones lógicamente
season_order = ['Winter', 'Spring', 'Summer', 'Fall']
seasonal_df = seasonal_df.loc[season_order]

# Limpiar nombres de columnas (asegurar FIPS de 5 dígitos)
seasonal_df.columns = [str(col).replace('p', '').zfill(5) for col in seasonal_df.columns]

# 4. Guardar resultado final
seasonal_df.to_csv(wind_output_path, index=True)
print(f"Proceso completado. Archivo creado en: {wind_output_path}")
print(f"Ubicaciones procesadas: {len(seasonal_df.columns)}")
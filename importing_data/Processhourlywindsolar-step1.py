# Hourly County solar data
# https://catalog.data.gov/dataset/2024-county-level-hourly-renewable-capacity-factor-dataset-for-the-reeds-model
import h5py
import pandas as pd
from pathlib import Path

# Puedes usar barras normales / incluso en Windows, pathlib lo entiende perfectamente
base_path = Path(r"C:\Users\z004we5c\Desktop\PowerBI\Pythonscripts")
solar_path = base_path / "Yondr" / "datacenter-siting-model"/ "Data"/ "UPV_COUNTY_SOLAR" /  "upv" /"upv-limited_county.h5"
wind_path = base_path / "Yondr" / "datacenter-siting-model"/ "Data"/ "UPV_COUNTY_WIND" /  "wind-ons" /"wind-ons-limited_county.h5"
solar_output_path = base_path / "Yondr" / "datacenter-siting-model"/ "Data" /"solar_hourly_2023.csv"
solar_output_path = base_path / "Yondr" / "datacenter-siting-model"/ "Data" /"solar_hourly_2023.csv"

# Load the .h5 file
with h5py.File(solar_path, 'r') as f:
    # List top-level groups/datasets
    print("Keys in file:")
    print(list(f.keys()))

    # Load the datasets
    index_raw = [i.decode('utf-8') for i in f['index_0'][:]] # date time

    # Convert index to datetime
    index = pd.to_datetime(index_raw)
    #index_names = [i.decode('utf-8') for i in f['index_names'][:]] # just says --> ['datetime']
   
    # Filter only rows from 2023
    mask_year = index.year == 2023
    row_indices_year = mask_year.nonzero()[0]

    # Read column names
    columns = [col.decode('utf-8') for col in f['columns'][:]]  # the 1-5 | p FIPS code location column

    # Read data from specified year
    data = f['data'][row_indices_year, :]

#print(columns)
#print(index_raw)
#print(index_names)

# Filter only rows from 2023
index_year = index[mask_year]

# Build the full DataFrame
solar_data = pd.DataFrame(data, columns=columns, index=index_year)
solar_data.index.name = 'datetime'  # Assign name to the index so it stays in dataframe

 # 8760 long so perfect one day

# Show result
print(solar_data.tail())

location_keys = solar_data.columns.str.split('|').str[1]
solar_data = solar_data.groupby(location_keys, axis=1).mean()
#print(solar_data.head(10))

solar_data.to_csv(solar_output_path, index=True) # 8760 long so perfect one day
#solar_data.to_csv("solar_hourly_2023.csv")


'''
'''
#### Getting the seasonal aggregation
solar_data = pd.read_csv("/Users/maria/Documents/Research/deloitte-proj/deloitte-data/wind_hourly_2023.csv")
print(len(solar_data.columns.unique()))

# Next get representative week for each season / month lets do season for now so less computation
print(solar_data.columns[0])
print(solar_data.index)

# Start at Jan 1, 2023, 00:00 (central time)
start = pd.Timestamp("2023-01-01 00:00:00", tz="America/Chicago")

# Generate datetime index for 8760 hours (non-leap year)
datetime_index = pd.date_range(start=start, periods=8760, freq='H')
solar_data.index = datetime_index  # replace index with real datetimes
print(solar_data.index)

# Label seasons
def get_season(dt):
    month = dt.month
    if month in [1, 2, 12]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    else:
        return 'Fall'
    
# Add season and hour columns
solar_data['season'] = solar_data.index.map(get_season)
solar_data['hour'] = solar_data.index.hour

# Group by season and hour, take median (so you have an average hourly value for each season)
season_solar_data = solar_data.groupby(['season', 'hour']).median() # do first or 3rd quartile for testing the extremess

# Sort season order
season_order = ['Winter', 'Spring', 'Summer', 'Fall']
season_solar_data = season_solar_data.loc[season_order]
season_solar_data = season_solar_data.drop(columns=["Unnamed: 0"])
season_solar_data.columns = season_solar_data.columns.str.replace('^p', '', regex=True) # FIPS code
season_solar_data.columns = [
    int(str(col).zfill(5)) if col not in ['season', 'hour'] else col
    for col in season_solar_data.columns
]

# Hour being just 1-len(df) so not two index for season and hour
#season_solar_data['hour'] = range(len(season_solar_data))

# Save results
print(season_solar_data.tail(10))
print(len(season_solar_data.columns.unique()))
season_solar_data.to_csv("/Users/maria/Documents/Research/deloitte-proj/deloitte-data/season_wind_hourly_2023.csv", index=True) 
#print(type(season_solar_data.columns[0]))
'''
'''
# IT MAKES THE HOURS 0-23 for each season so change that

# do the seasonal aggregation for CDD 
cdd_county_monthly_data = pd.read_csv("/Users/maria/Documents/Research/deloitte-proj/raw-data/climdiv-cddccy-v1.0.0-20250806.txt", delim_whitespace=True, header=None)

cdd_county_monthly_data['FIPS'] = cdd_county_monthly_data[0].astype(str).str[:5]
cdd_county_monthly_data['year'] = cdd_county_monthly_data[0].astype(str).str[-4:]
print(cdd_county_monthly_data.head())
year_of_choice = "2022"

cdd_data = cdd_county_monthly_data[cdd_county_monthly_data['year'] == year_of_choice]
cdd_county_monthly_data = cdd_county_monthly_data.drop(columns=[0])

print(cdd_data.head())

seasons = {
    'Winter': [12, 1, 2],
    'Spring': [3, 4, 5],
    'Summer': [6, 7, 8],
    'Fall': [9, 10, 11]
}

seasonal_averages = {}

for season, months in seasons.items():
    # Because month 12 comes before 1, 2, order doesn’t matter for averaging
    seasonal_averages[season] = cdd_data[months].mean(axis=1)

# Add seasonal averages as new columns:
for season in seasons.keys():
    cdd_data[season] = seasonal_averages[season]

print(cdd_data.head())


solar_data = pd.read_csv("/Users/maria/Documents/Research/deloitte-proj/deloitte-data/wind_hourly_2023.csv")
season_solar_data = pd.read_csv("/Users/maria/Documents/Research/deloitte-proj/deloitte-data/season_wind_hourly_2023.csv")

print(solar_data.columns)
print(solar_data['p48111'])
print(season_solar_data.columns)
print(season_solar_data['48111'])

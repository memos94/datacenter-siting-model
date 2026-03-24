import pandas as pd
import geopandas as gpd
from typing import Dict, Tuple, Optional, List
from cost_dict import *

class DataForModel:
    '''
    Load, process, and prepare the data for the optimization model

    Args:
            base_path: Base directory path for data files
    '''

    def __init__(self, base_path: str = ""):
        '''
        Initialize file paths
        '''

        self.base_path = base_path
        self.raw_data = {}
        self.processed_data = {}
        self.model_dictionaries = {}
    
    def load_data(self, file_paths: Dict[str,str]) -> None:
        '''
        Load the data files 

        Args:
            file_paths: Dictionary mapping data type to file path
        '''

        print("Loading raw data files...")
        
        if 'state_shapefile' in file_paths:
            self.raw_data['state_data'] = gpd.read_file(file_paths['state_shapefile'])
            
        if 'county_csv' in file_paths:
            self.raw_data['county_data'] = gpd.read_file(file_paths['county_csv'])
            
        if 'supply_data' in file_paths:
            self.raw_data['supply_data'] = pd.read_csv(file_paths['supply_data'])
            
        if 'merged_cf' in file_paths:
            self.raw_data['merged_cf_df'] = pd.read_csv(file_paths['merged_cf'])
            
        if 'demand_data' in file_paths:
            self.raw_data['demand_data'] = pd.read_csv(file_paths['demand_data'])
            
        if 'county2zone' in file_paths:
            self.raw_data['county2zone'] = pd.read_csv(file_paths['county2zone'])
            
        if 'hierarchy' in file_paths:
            self.raw_data['hierarchy'] = pd.read_csv(file_paths['hierarchy'])
            
        if 'electric_prices' in file_paths:
            self.raw_data['electric_price_data'] = pd.read_csv(file_paths['electric_prices'])
            
        if 'water_risk' in file_paths:
            self.raw_data['water_risk_data'] = gpd.read_file(file_paths['water_risk'])
            
        print(f"Loaded {len(self.raw_data)} data files.")
    
    def process_supply_data(self, 
                            min_total_capacity: float = 0,
                            state_filter: Optional[str] = None,
                            county_filter: Optional[List[int]] = None) -> pd.DataFrame:
        """
        Process supply data to get one location per county with highest renewable capacity.
        
        Args:
            min_total_capacity: Minimum total renewable capacity threshold (MW)
            state_filter: State ID to filter by (e.g., '12' for Florida)
            
        Returns:
            Processed supply data DataFrame
        """

        print("Processing supply data...")
        
        supply_data = self.raw_data['supply_data'].copy()
        
        # Calculate total renewable capacity
        supply_data["total_capacity"] = (
            supply_data["capacity_solar"] + 
            supply_data["capacity_wind"] + 
            supply_data["capacity_geo"]
        )
        
        # Apply filters
        if min_total_capacity >= 0:
            supply_data = supply_data[supply_data["total_capacity"] > min_total_capacity]
            
        if state_filter:
            supply_data = supply_data[supply_data['state_id'] == state_filter]
    
        if county_filter:
            print(f"Filtering to {len(county_filter)} specific counties")
            supply_data['FIPS'] = supply_data['FIPS'].astype('Int64')
            supply_data = supply_data[supply_data['FIPS'].isin(county_filter)]
            
        print(f"After filtering: {supply_data['FIPS'].nunique()} unique locations")
        
        # Pick one location per county with highest total capacity
        supply_data['FIPS'] = supply_data['FIPS'].astype('Int64')
        idx = supply_data.groupby("FIPS")["total_capacity"].idxmax()
        supply_data = supply_data.loc[idx].reset_index(drop=True)
        
        # Clean and standardize location column
        supply_data = supply_data.rename(columns={'FIPS': 'location'})
        supply_data['location'] = supply_data['location'].astype('Int64')
        
        # Handle empty values
        supply_data = supply_data.replace(r'^\s*$', 0, regex=True).dropna(how="all") # only drop if whole row 
        supply_data['location'] = supply_data['location'].astype('Int64')
        
        print(f"Final processed locations: {supply_data['location'].nunique()}")

        #supply_data = supply_data[["location", "capacity_solar", "capacity_wind", 'latitude', 'longitude']] 

        self.processed_data['supply_data'] = supply_data
        return supply_data
    
    def merge_generation_data(self) -> pd.DataFrame:
        """
        Merge hourly capacity factor data for wind and solar with supply data to calculate max possible generation.
        
        Returns:
            Merged generation DataFrame
        """
        print("Merging generation data...")
        
        supply_data = self.processed_data['supply_data']
        merged_cf_df = self.raw_data['merged_cf_df'].copy()
        
        # Filter capacity factor data to match supply locations
        merged_cf_df = merged_cf_df[merged_cf_df['location'].isin(supply_data['location'])]
        merged_cf_df['hour'] = merged_cf_df['hour'].astype('Int64')
        merged_cf_df['location'] = merged_cf_df['location'].astype('Int64')
        
        # Ensure supply data matches capacity factor locations
        supply_subset= supply_data[
            supply_data['location'].isin(merged_cf_df['location'])
        ][["location", "capacity_solar", "capacity_wind", 'latitude', 'longitude']]
        
        # Merge and calculate generation
        merged_gen = merged_cf_df.merge(supply_subset, on='location', how='inner')
        merged_gen['solar_gen'] = merged_gen['hourly_cf_solar'] * merged_gen['capacity_solar']
        merged_gen['wind_gen'] = merged_gen['hourly_cf_wind'] * merged_gen['capacity_wind']
        
        print(f"Merged generation data: {merged_gen['location'].nunique()} unique locations")
        
        self.processed_data['merged_gen'] = merged_gen

        supply_data = supply_data[supply_data['location'].isin(merged_cf_df['location'])]
        self.processed_data['supply_data'] = supply_data

        return merged_gen, supply_data
    

    def load_water_risk(self, max_water_risk: Optional[float] = None) -> pd.DataFrame:
        """
        Load water risk data and can exclude baed on what you deem is max acceptable water risk.
        
        Args:
            max_water_risk: Maximum acceptable water risk category
            
        Returns:
            Water risk filtered data
        """

        print("Applying water risk filter...")
        
        supply_data = self.processed_data['supply_data']
        water_risk_data = self.raw_data['water_risk_data']
        
        # Create GeoDataFrame from supply locations
        gdf_points = gpd.GeoDataFrame(
            supply_data[['longitude', 'latitude', 'location']],
            geometry=gpd.points_from_xy(supply_data['longitude'], supply_data['latitude']),
            crs="EPSG:4326"
        )
        
        # Spatial join with water risk data
        gdf_joined = gpd.sjoin(
            gdf_points,
            water_risk_data,
            how='left',
            predicate='within' # if points on polygon edges should also count.
        )
        
        # Apply water risk filter if specified
        if max_water_risk is not None:
            gdf_joined = gdf_joined[gdf_joined["w_awr_elp_tot_cat"] < max_water_risk]
            print(f"After water risk filter: {len(gdf_joined)} locations")
        
        # Update processed data to only include water-risk-filtered locations
        valid_locations = gdf_joined['location'].tolist()
        
        self.processed_data['supply_data'] = self.processed_data['supply_data'][
            self.processed_data['supply_data']['location'].isin(valid_locations)
        ]
        
        if 'merged_gen' in self.processed_data:
            self.processed_data['merged_gen'] = self.processed_data['merged_gen'][
                self.processed_data['merged_gen']['location'].isin(valid_locations)
            ]
        
        # Create water risk dictionary
        self.processed_data['water_risk_dict'] = dict(
            zip(gdf_joined['location'], gdf_joined['w_awr_elp_tot_cat'])
        )
        
        return gdf_joined
    
    def create_dictionaries(self, 
                            pue_climate_dict: Dict, 
                            wue_climate_dict: Dict, 
                            trans_mult_dict: Dict, 
                            telecom_cost_dict: Dict) -> Dict[str, Dict]:
        
        '''
        Create dictionaries for the optimization

        Args:
            pue_climate_dict: PUE values by climate zone
            wue_climate_dict: WUE values by climate zone  
            trans_mult_dict: Transmission multipliers by region
            telecom_cost_dict: Telecom costs by distance
            
        Returns:
            Dictionary of model parameter dictionaries
        '''

        supply_data = self.processed_data['supply_data']
        merged_gen = self.processed_data.get('merged_gen')
        demand_data = self.raw_data['demand_data'].iloc[:96].copy()
        county2zone = self.raw_data['county2zone']
        hierarchy = self.raw_data['hierarchy']
        electric_price_data = self.raw_data['electric_price_data'].copy()

        # Load dictionary from demand data
        demand_data['hour'] = demand_data['hour'].astype('Int64')
        load_dict = demand_data.set_index('hour')['load'].to_dict()
        
        # Capacity dictionaries
        # Map dataframe column names → model dictionary keys
        col_map = {
            'capacity_solar': 'solar_capacity',
            'capacity_wind': 'wind_capacity',
            'capacity_geo': 'geo_capacity'
        }

        capacity_dicts = {
            new_key: supply_data.set_index('location')[old_col].fillna(0).astype(float).to_dict()
            for old_col, new_key in col_map.items()
        }
        #print(capacity_dicts)

        # Generation dictionaries (MW)
        solar_hourly_dict, wind_hourly_dict = {}, {}
        if merged_gen is not None and not merged_gen.empty:
            merged_gen['hour'] = merged_gen['hour'].astype('Int64')
            solar_hourly_dict = merged_gen.set_index(['hour', 'location'])['solar_gen'].fillna(0).to_dict()
            wind_hourly_dict = merged_gen.set_index(['hour', 'location'])['wind_gen'].fillna(0).to_dict()

        # Geothermal generation (constant across hours) (MWh)
        all_hours = demand_data['hour'].unique()
        geo_gen_df = pd.DataFrame(
            [(hr, loc, row['cf_geo'] * row['capacity_geo'])
            for hr in all_hours
            for loc, row in supply_data.set_index('location').iterrows()],
            columns=['hour', 'location', 'geo_gen']
        )
        geo_hourly_dict = geo_gen_df.set_index(['hour', 'location'])['geo_gen'].to_dict()

        # Energy and water loads  
        pue_series = supply_data['clim_zone'].map(pue_climate_dict).fillna(1.2)
        wue_series = supply_data['clim_zone'].map(wue_climate_dict).fillna(0.3)
        pue_dict = dict(zip(supply_data['location'], pue_series))  # (kWh/kWh)
        wue_dict = dict(zip(supply_data['location'], wue_series)) # (L/kWh)

        load_df = demand_data.assign(key=1)
        loc_df = pd.DataFrame({'location': supply_data['location'], 'key': 1})
        load_cross = load_df.merge(loc_df, on='key').drop(columns='key')

        load_cross['energy_load'] = load_cross.apply(lambda x: x['load'] * pue_dict.get(x['location'], 0), axis=1) # MWh
        load_cross['water_load'] = load_cross.apply(lambda x: x['load'] * wue_dict.get(x['location'], 0) * 1000, axis=1) # L

        energy_load_dict = load_cross.set_index(['hour', 'location'])['energy_load'].to_dict()
        
        water_load_dict = load_cross.set_index(['hour', 'location'])['water_load'].to_dict()
        load_dict = load_df.set_index('hour')['load'].to_dict()

        # Infrastructure 
        trans_dist_dict = supply_data.set_index('location')['trans_dist'].fillna(0).to_dict()
        telecom_dist_dict = supply_data.set_index('location')['telecom_dist'].fillna(0).to_dict()
        water_price_dict = (supply_data.set_index('location')['water_price'].fillna(0) / 1000).to_dict()
        coords_dict = supply_data.set_index('location')[['latitude', 'longitude']].T.apply(tuple).to_dict()

        # Costs 
        county2zone['FIPS'] = county2zone['FIPS'].astype('Int64')
        fips_to_row = county2zone.set_index("FIPS").to_dict(orient="index") # 53007: {'ba': 'p1', 'county_name': 'chelan', 'state': 'WA'},
        ba_to_row = hierarchy.set_index('ba').to_dict(orient="index")
        abbr_to_state_name = supply_data[['state_id', 'state']].drop_duplicates().set_index('state_id')['state'].to_dict()

        # Electric price dict for county level instead of state (cents/kWh)
        electric_price_data['electric_price'] = electric_price_data['electric_price'] / 10 # Average retail price ($ / MWh)
        electric_price_dict = electric_price_data.set_index('state')['electric_price'].to_dict()

        cost_per_county_dict = {
            loc: electric_price_dict.get(abbr_to_state_name.get(fips_to_row.get(loc, {}).get('state', ''), ''), 0)
            for loc in supply_data['location']
        }

        trans_multiplier_dict = {
            loc: trans_mult_dict.get(ba_to_row.get(fips_to_row.get(loc, {}).get('ba', ''), {}).get('nercr', ''), 0)
            for loc in supply_data['location']
        }

        telecom_cost_final_dict = {
            loc: (telecom_cost_dict[round(dist)] if 0 <= round(dist) <= telecom_cost_dict.breaks[-1] else 0)
            for loc, dist in telecom_dist_dict.items()
        }


        self.model_dictionaries = {
            # Capacity
 
            'solar_capacity': capacity_dicts['solar_capacity'],
            'wind_capacity': capacity_dicts['wind_capacity'],
            'geo_capacity': capacity_dicts['geo_capacity'],

            # Generation (hour, location) -> MW
            'solar_generation': solar_hourly_dict,
            'wind_generation': wind_hourly_dict,
            'geo_generation': geo_hourly_dict,
            
            # Demand (hour, location) -> MW or Gallons
            'energy_load': energy_load_dict,
            'water_load': water_load_dict,
            'base_load': load_dict,  # hour -> MW
            
            # Costs and prices
            'electric_price': cost_per_county_dict,  # location -> $/MWh
            'water_price': water_price_dict,         # location -> $/L
            'telecom_cost': telecom_cost_final_dict, # location -> $
            
            # Infrastructure
            'trans_dist': trans_dist_dict,           # location -> miles
            'telecom_dist': telecom_dist_dict,       # location -> miles
            'trans_multiplier': trans_multiplier_dict, # location -> ratio
            
            # Climate and efficiency
            'pue': pue_dict,                         # location -> ratio
            'wue': wue_dict,                         # location -> L/kWh
            
            # Geographic data
            'location_coordinates': coords_dict,
            'water_risk': self.processed_data.get('water_risk_dict', {}),
            'fips_to_region': fips_to_row,
            'ba_to_region': ba_to_row,
        }
        
        print(f"Created {len(self.model_dictionaries)} model dictionaries")
        return self.model_dictionaries
    

    def get_summary_stats(self) -> Dict:
        """
        Get summary statistics of the processed data.
        
        Returns:
            Dictionary of summary statistics
        """
        stats = {}
        
        if 'supply_data' in self.processed_data:
            supply_data = self.processed_data['supply_data']
            stats['num_locations'] = len(supply_data)
            stats['total_solar_capacity'] = supply_data['capacity_solar'].sum()
            stats['total_wind_capacity'] = supply_data['capacity_wind'].sum()
            stats['avg_total_capacity'] = supply_data['total_capacity'].mean()
        
        if 'merged_gen' in self.processed_data:
            merged_gen = self.processed_data['merged_gen']
            stats['num_hours'] = merged_gen['hour'].nunique()
            stats['avg_solar_cf'] = merged_gen['hourly_cf_solar'].mean()
            stats['avg_wind_cf'] = merged_gen['hourly_cf_wind'].mean()
        
        return stats
    

def create_data_processor(base_path: str = "") -> DataForModel:
    """
    Convenience function to create and return an EnergyDataProcessor instance.

    Args:
        base_path: Base directory path for data files
        
    Returns:
        Configured EnergyDataProcessor instance
    """
    return DataForModel(base_path)


def process_data_pipeline(file_paths: Dict[str, str],
                            pue_climate_dict: Dict,
                            wue_climate_dict: Dict,
                            trans_mult_dict: Dict,
                            telecom_cost_dict: Dict,
                            min_capacity: float = 200,
                            state_filter: Optional[str] = None,
                            max_water_risk: Optional[float] = 4.0,
                            county_filter: Optional[List[int]] = None 
                            ) -> Tuple[DataForModel, Dict]:
    """
    Complete pipeline to process energy data.

    Args:
        file_paths: Dictionary of file paths
        pue_climate_dict: PUE values by climate zone
        wue_climate_dict: WUE values by climate zone
        trans_mult_dict: Transmission multipliers by region
        telecom_cost_dict: Telecom costs by distance
        min_capacity: Minimum total renewable capacity (MW)
        state_filter: State ID to filter by
        max_water_risk: Maximum water risk category
        county_filter: Optional list of specific counties to include
        
    Returns:
        Tuple of (processor instance, model dictionaries)
    """
    processor = DataForModel()

    # Load raw data
    processor.load_data(file_paths)

    # Process supply data with county filter
    processor.process_supply_data(
        min_total_capacity=min_capacity,
        state_filter=state_filter,
        county_filter=county_filter  
    )

    # Merge generation data
    processor.merge_generation_data()

    # Apply water risk filter
    if max_water_risk is not None:
        processor.load_water_risk(max_water_risk)

    # Create model dictionaries
    model_dicts = processor.create_dictionaries(
        pue_climate_dict=pue_climate_dict,
        wue_climate_dict=wue_climate_dict,
        trans_mult_dict=trans_mult_dict,
        telecom_cost_dict=telecom_cost_dict
    )

    
    # Print summary
    stats = processor.get_summary_stats()
    print("\nData Processing Summary:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    return processor, model_dicts

'''

# Example usage
if __name__ == "__main__":
    # Define your file paths
    file_paths = {
        'state_shapefile': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/cb_2022_us_state_20m/cb_2022_us_state_20m.shp',
        'county_csv': 'CountyMaps/county_data.csv',
        'supply_data': '/Users/maria/Documents/Research/deloitte-proj/telecom-data/supply_data_lat_lon_water_clim.csv',
        'merged_cf': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/merged_hourly_solar_wind_cf.csv',
        'demand_data': 'fake_demand.csv',
        'county2zone': 'CountyMaps/county2zone.csv',
        'hierarchy': 'CountyMaps/hierarchy.csv',
        'electric_prices': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/electric_prices.csv',
        'water_risk': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/water_risk.gpkg'
    }


    # Use the complete pipeline
    processor, model_dictionaries = process_data_pipeline(
        file_paths=file_paths,
        pue_climate_dict=pue_climate_region_same,
        wue_climate_dict=wue_climate_region_same,
        trans_mult_dict=trans_mult_dict,
        telecom_cost_dict=telecom_cost,
        min_capacity=200,  # MW
        state_filter='FL',  # Set to 'FL' --> '12' for Florida, '48' for Texas, etc.
        max_water_risk=4.0
    )

    
    # Access all the dictionaries for your Pyomo model
    print("\nAvailable model dictionaries:")
    for key in model_dictionaries.keys():
        print(f"  - {key}")
    
    # Example of accessing specific dictionaries
    solar_capacity = model_dictionaries['solar_capacity']
    wind_capacity = model_dictionaries['wind_capacity']
    energy_load = model_dictionaries['energy_load']  # (hour, location) -> MW
    water_load = model_dictionaries['water_load']    # (hour, location) -> Gallons
    electric_price = model_dictionaries['electric_price']  # location -> $/MWh
    
    print(f"\nData sizes:")
    print(f"Solar capacity dict: {len(solar_capacity)} locations")
    print(f"Energy load dict: {len(energy_load)} (hour, location) pairs")
    print(f"Water load dict: {len(water_load)} (hour, location) pairs")

'''
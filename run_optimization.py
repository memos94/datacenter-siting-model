'''
The actual optimization function and script to run one iteration
'''

from config import config
from cost_dict import *

from data_loader import process_data_pipeline
from siting_model import run_datacenter_optimization
from results_visualization import visualize_optimization_results
from components.storage import *
from components.plant import *

def main():

    # Define the file paths
    # file_paths = {
    #     'state_shapefile': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/cb_2022_us_state_20m/cb_2022_us_state_20m.shp',
    #     'county_csv': 'CountyMaps/county_data.csv',
    #     'supply_data': '/Users/maria/Documents/Research/deloitte-proj/telecom-data/supply_data_lat_lon_water_clim.csv',
    #     'merged_cf': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/merged_hourly_solar_wind_cf.csv',
    #     'demand_data': 'fake_demand.csv',
    #     'county2zone': 'CountyMaps/county2zone.csv',
    #     'hierarchy': 'CountyMaps/hierarchy.csv',
    #     'electric_prices': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/electric_prices.csv',
    #     'water_risk': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/water_risk.gpkg',
    #     'county_shapefile': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/cb_2018_us_county_5m/cb_2018_us_county_5m.shp'
    # }
    # file_paths = {
    #     'state_shapefile': 'Data/cb_2022_us_state20m/cb_2022_us_state_20m.shp',
    #     'county_csv': 'CountyMaps/county_data.csv',
    #     'supply_data': 'Data/supply_data_lat_lon_water_clim5.csv',
    #     'merged_cf': 'Data/merged_hourly_solar_wind_cf.csv',
    #     'demand_data': 'fake_demand.csv',
    #     'county2zone': 'CountyMaps/county2zone.csv',
    #     'hierarchy': 'CountyMaps/hierarchy.csv',
    #     'electric_prices': 'Data/electric_prices.csv',
    #     'water_risk': 'Data/water_risk.gpkg',
    #     'county_shapefile': 'Data/cb_2018_us_county_5m/cb_2018_us_county_5m.shp'
    # }

    file_paths = {
        'state_shapefile': 'Data/cb_2022_us_state20m/cb_2022_us_state_20m.shp',
        'county_csv': 'TexasData/texas_data/tx_county_data.csv',
        'supply_data': 'TexasData/texas_data/tx_supply_Data.csv',
        'merged_cf': 'TexasData/texas_data/tx_merged_cf.csv',
        'demand_data': 'TexasData/texas_data/tx_demand.csv',
        'county2zone': 'TexasData/texas_data/tx_county2zone.csv',
        'hierarchy': 'TexasData/texas_data/tx_hierarchy.csv',
        'electric_prices': 'TexasData/texas_data/tx_electric_prices.csv',
        'water_risk': 'Data/water_risk.gpkg',
        'county_shapefile': 'Data/cb_2018_us_county_5m/cb_2018_us_county_5m.shp'
    }

    # Process the data
    print("\nStep 1: Processing energy data...")
    processor, model_dictionaries = process_data_pipeline(
        file_paths=file_paths,
        pue_climate_dict=pue_climate_region_5,
        wue_climate_dict=wue_climate_region_5,
        trans_mult_dict=trans_mult_dict,
        telecom_cost_dict=telecom_cost,
        min_capacity=200,        # Minimum total renewable capacity (MW)
        state_filter=None,       # Set to specific state if desired (e.g., 'TX' for Texas)
        max_water_risk=None
        # max_water_risk=5.0       # Maximum acceptable water risk
    )
    import pandas as pd
    import os

    print("Exportando diccionarios de entrada para revisión...")
    output_dir = "debug_data"
    os.makedirs(output_dir, exist_ok=True)

    for name, data_dict in model_dictionaries.items():
        if not data_dict: continue
        
        # Intentamos crear un DataFrame. 
        # Si la clave es una tupla (hora, loc), Pandas creará un MultiIndex automáticamente.
        df = pd.Series(data_dict).reset_index()
        
        # Nombrar columnas según el tipo de índice
        if len(df.columns) == 3:
            df.columns = ['hour', 'location', 'value']
        else:
            df.columns = ['location', 'value']
            
        df.to_csv(f"{output_dir}/{name}.csv", index=False)

    print(f"Archivos exportados en la carpeta '{output_dir}/'")
    # Exportar el DataFrame maestro antes de que se convierta en diccionarios
    processor.processed_data['supply_data'].to_csv("debug_supply_full.csv")
    processor.processed_data['merged_gen'].to_csv("debug_generation_hourly.csv")

    # Apply load multiplier to energy and water loads
    if config['load_multiplier'] != 1.0:
        print(f"Applying load multiplier: {config['load_multiplier']}")
        
        # Multiply energy load
        if 'energy_load' in model_dictionaries:
            model_dictionaries['energy_load'] = {
                (h, loc): value * config['load_multiplier']
                for (h, loc), value in model_dictionaries['energy_load'].items()
            }
        
        # Multiply water load
        if 'water_load' in model_dictionaries:
            model_dictionaries['water_load'] = {
                (h, loc): value * config['load_multiplier']
                for (h, loc), value in model_dictionaries['water_load'].items()
            }
    
    # Run the Optimization
    print("\nStep 2: Running optimization model...")
    try:
        opt_model, solution = run_datacenter_optimization(
            model_dictionaries=model_dictionaries,
            config=config,
            cost_params=cost_params,
            trans_rating = trans_rating,
            trans_cost = trans_cost,
            solver_name='scip',
            processor=processor,
            storage_system = StorageTemplates.create_lithium_ion("lithium-battery"),
            plant_systems = {'smr': PlantTemplates.create_smr_plant("my_smr", 1000),
            #                 'gas': PlantTemplates.create_gas_turbine("my_gas_turbine", 1000)}
            }
            # You can add solver options here:
            # MIPGap=0.01,      # 1% optimality gap
            # TimeLimit=3600    # 1 hour time limit
        )

        # 8. ANALYZE RESULTS
        print("\nStep 3: Analyzing results...")
        analyze_results(solution, model_dictionaries, opt_model)

    except Exception as e:
        print(f"Error during optimization: {e}")
        print("Trying with different solver or check your data...")

    print("\n📈 STEP 3: CREATING VISUALIZATIONS")
    print("-" * 40)

    
    try:
        viz_results = visualize_optimization_results(
            model=opt_model.model,
            cost_params=cost_params,
            supply_data=processor.processed_data['supply_data'],
            output_dir="optimization_output",
            county_shapefile_path=file_paths['county_shapefile']
        )

        max_load = max(model_dictionaries['energy_load'].values())
        print(f"  Data Center Max Load: {max_load:.1f} MW")
        
        print(f"✅ Visualizations complete!")
        print(f"   Generated plots and analysis in 'optimization_output/' directory")
        
        # Print detailed summary
        #print_detailed_summary(viz_results, solution)
        
    except Exception as e:
        print(f"❌ Error in visualization: {e}")
        print("Optimization completed successfully, but visualization failed")
    

def analyze_results(solution, model_dictionaries, opt_model):
    """Analyze and print detailed results."""
            
    if not solution['selected_locations']:
        print("No feasible solution found!")
        return
        
    #selected_loc = solution['selected_locations'][0]  # Assuming single location
    selected_locs = solution['selected_locations']
    
    print(f"\nDETAILED ANALYSIS FOR LOCATION {selected_locs}")
    print("-" * 50)
    
    # Location characteristics
    if 'location_coordinates' in model_dictionaries:
        coords = model_dictionaries['location_coordinates'].get(selected_locs, (None, None))
        print(f"Coordinates: {coords[0]:.4f}, {coords[1]:.4f}")
    
    # Renewable capacity at location
    solar_cap = model_dictionaries['solar_capacity'].get(selected_locs, 0)
    wind_cap = model_dictionaries['wind_capacity'].get(selected_locs, 0) 
    geo_cap = model_dictionaries.get('geo_capacity', {}).get(selected_locs, 0)
    max_load = max(model_dictionaries['energy_load'].values())
    
    print(f"Available Capacity:")
    print(f"  Solar: {solar_cap:.1f} MW")
    print(f"  Wind: {wind_cap:.1f} MW")
    print(f"  Geothermal: {geo_cap:.1f} MW")
    print(f"  Total: {solar_cap + wind_cap + geo_cap:.1f} MW")
    print(f"  Data Center Max Load: {max_load:.1f} MW")

if __name__ == "__main__":
    main()
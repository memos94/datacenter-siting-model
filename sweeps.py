
"""
Sweep system for data center optimization sensitivity analysis.
"""

# Any key here MUST match a field name in Config :D.
# DO NOT USE RESERVED NAMES.
# DO NOT USE KEYS THAT ARE NOT IN Config.

# Example:
'''
SWEEP = {
    "ramp_up_battery": [i for i in range(1337, 1340)],
    "lr": [1e-3, 3e-4],
    "batch_size": [32, 64],
    "model_name": ["chronos", "timesfm"],
}
'''

import pandas as pd
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass, asdict, replace
import itertools
import json
from pathlib import Path

from config import config
from cost_dict import *
from itertools import product

from data_loader import process_data_pipeline
from siting_model import run_datacenter_optimization
from components.storage import *
from components.plant import *

#Selected Locations: [12037, 22075, 22087, 22089, 22095, 50013, 51650, 51710, 8047, 8079]


# Define sweep configurations
SWEEP = {
    # Economic sensitivity
    'economic_sweep': {
        'curtail_penalty': [5.0, 10.0, 20.0, 50.0],
        'ren_export_price': [10.0, 20.0, 30.0, 40.0],
    },
    
    # Capacity sensitivity  
    'capacity_sweep': {
        'datacenter_capacity': [200.0, 250.0, 300.0, 400.0],
        'max_storage_cap': [100.0, 200.0, 400.0, 800.0],
        'min_capacity': [150.0, 200.0, 300.0, 500.0]
    },
    
    # Regional sensitivity
    'regional_sweep': {
        #'state_filter': [None, '48', '06', '12', '36'],  # All, TX, CA, FL, NY
        'state_filter': ['FL'],
        'max_water_risk': [2.0, 3.0, 4.0, 5.0]
    },
    
    # Technology cost sensitivity
    'technology_sweep': {
        'solar_cap_cost_multiplier': [0.8, 1.0, 1.2],
        'wind_cap_cost_multiplier': [0.8, 1.0, 1.2],
        'geo_cap_cost_multiplier': [0.8, 1.0, 1.2],
        'smr_cap_cost_multiplier': [0.8, 1.0, 1.2],
        'electric_price_multiplier': [0.8, 1.0, 1.2],
    },

    'load_size_sweep': {
        'load_multiplier': [0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    },
    
    'water_risk_sweep': {
        'water_risk_penalty': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    },

    # For Paper ... 
    # Renewable penetration amount
    'ren_pen_sweep': {
        #'include_transmission_cost': [True, False],
        'ren_penetration': [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    },

    'discount_rate_sweep': {
        #'discount_rate': [0.01, 0.03, 0.05, 0.08, 0.1, 0.13, 0.15, 0.18, 0.2, 0.23]
        'discount_rate': [0.005, 0.007, 0.009, 0.01, 0.012, 0.015, 0.017]
    },

    'project_life_sweep': {
        'project_lifetime': [1, 3, 6, 10, 15, 20, 25, 30]
    },

    # Network sweep
    'network_type_sweep': {
        'include_transmission_cost': [True, False],
        #'discount_rate': [0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.15, 0.18, 0.2, 0.24, 0.3]
        #'include_transmission_cost': [True, False],
        'include_telecom_cost': [True, False],
        'include_water_cost': [True, False]
    }
}

@dataclass
class OptimizationConfig:
    """Configuration class for optimization experiments."""
    
    # Experiment metadata
    exp_name: str = "datacenter_opt"
    state_filter: str = None  # e.g., '48' for Texas, None for all states'
    county_filter: List[int] = None 
    
    # Data processing parameters  
    min_capacity: float = 200.0        # MW minimum renewable capacity
    max_water_risk: float = 4.0        # Maximum water risk threshold
    
    # Data center parameters
    datacenter_capacity: float = 250.0  # MW peak demand
    planning_horizon: int = 5           # years
    single_location: bool = False        
    min_locations: int = 10
    max_locations: int = 10
    
    # Storage parameters
    max_charge_rate: float = 50.0      # MW
    max_discharge_rate: float = 50.0   # MW
    max_storage_cap: float = 200.0     # MWh
    min_storage_cap: float = 0.0       # MWh
    
    # Economic parameters
    curtail_penalty: float = 10.0      # $/MWh
    ren_export_price: float = 20.0     # $/MWh
    ren_penetration: float = 1         # fraction
    smr_capacity: float = 250.0        # MW

    # Cost multipliers (to allow sensitivity analysis)
    solar_cap_cost_multiplier: float = 1.0
    wind_cap_cost_multiplier: float = 1.0
    geo_cap_cost_multiplier: float = 1.0
    smr_cap_cost_multiplier: float = 1.0

    # Load size multipliers
    load_multiplier: float = 1

    # Discount rate
    discount_rate: float = 0.3 # 0.012 for main result
    project_lifetime: float = 5 # 20 for main results
    
    # Solver parameters
    solver_name: str = 'scip'
    solver_time_limit: float = 1000.0  # seconds
    solver_mip_gap: float = 0.03       # 1%

    # Cost component toggles
    include_transmission_cost: bool = True
    include_telecom_cost: bool = True
    include_water_cost: bool = True
    include_water_risk: bool = True  # For filtering by water risk
    
    
    def with_overrides(self, **kwargs):
        """Create a new config with specified overrides."""
        return replace(self, **kwargs)
    
    def to_cost_params(self) -> Dict[str, Dict]:
        """Convert config to cost_params format for optimization model using existing cost_dict."""
        return {
            'variable_gen_cost': variable_gen_cost.copy(),  # Use your existing variable costs
            'capital_gen_cost': {
                'solar': capital_gen_cost['solar'] * self.solar_cap_cost_multiplier,
                'wind': capital_gen_cost['wind'] * self.wind_cap_cost_multiplier,
                'geo_45': capital_gen_cost['geo_45'] * self.geo_cap_cost_multiplier,
                'smr': capital_gen_cost['smr'] * self.smr_cap_cost_multiplier
            },
            'fixed_gen_cost': fixed_gen_cost.copy(),  # Use your existing fixed costs
            'subs_new_cost': subs_new_cost.copy(),  
            'trans_capacity': trans_capacity,
            # Pass toggles to the model
            'include_transmission_cost': self.include_transmission_cost,
            'include_telecom_cost': self.include_telecom_cost,
            'include_water_cost': self.include_water_cost,
        }
    
    
    def to_model_config(self) -> Dict[str, Any]:
        """Convert config to model_config format for optimization model."""
        return {
            'datacenter_capacity': self.datacenter_capacity,
            'max_charge_rate': self.max_charge_rate,
            'max_discharge_rate': self.max_discharge_rate,
            'max_storage_cap': self.max_storage_cap,
            'min_storage_cap': self.min_storage_cap,
            'curtail_penalty': self.curtail_penalty,
            'ren_export_price': self.ren_export_price,
            'planning_horizon': self.planning_horizon,
            'single_location': self.single_location,
            'smr_capacity': self.smr_capacity,
            'ren_penetration': self.ren_penetration,
            'discount_rate': self.discount_rate,
            'project_lifetime': self.project_lifetime
        }

class OptimizationExperimentRunner:
    """Class to run optimization experiments with sweep configurations."""
    
    def __init__(self, file_paths: Dict[str, str]):
        """Initialize with data file paths."""
        self.file_paths = file_paths
        self.results = []
        
    def run_experiment(self, cfg: OptimizationConfig) -> Dict[str, Any]:
        """
        Run a single optimization experiment.
        
        Args:
            cfg: Configuration for this experiment
            
        Returns:
            Dictionary of results and metrics
        """
        print(f"\n[RUN] {cfg.exp_name}")
        print(f"Parameters: datacenter_cap={cfg.datacenter_capacity}, "
              f"curtail_penalty={cfg.curtail_penalty}, state={cfg.state_filter}")
        
        # ADD THIS DEBUG:
        print(f"DEBUG CONFIG: include_water_cost = {cfg.include_water_cost}")
        
        try:
            # 1. Process data with current configuration
            processor, model_dictionaries = process_data_pipeline(
                file_paths=self.file_paths,
                pue_climate_dict=pue_climate_region_same,
                wue_climate_dict=wue_climate_region_same,
                trans_mult_dict=trans_mult_dict,
                telecom_cost_dict=telecom_cost,
                min_capacity=cfg.min_capacity,
                state_filter='FL',    #cfg.state_filter,
                max_water_risk=cfg.max_water_risk,
                #county_filter = [12037, 22075, 22087, 36047, 36061, 44001, 44005, 50013, 51650, 51710]
                #county_filter = [12037, 22075, 22087, 36047, 36061, 44001, 44005, 50013, 51650, 51710]
                #county_filter= [12037, 22075, 22087, 22089, 22095, 36061, 44005, 50013, 51650, 51710] #cfg.county_filter
            )

            # Apply load multiplier to energy and water loads
            if cfg.load_multiplier != 1.0:
                print(f"Applying load multiplier: {cfg.load_multiplier}")
                
                # Multiply energy load
                if 'energy_load' in model_dictionaries:
                    model_dictionaries['energy_load'] = {
                        (h, loc): value * cfg.load_multiplier
                        for (h, loc), value in model_dictionaries['energy_load'].items()
                    }
                
                # Multiply water load
                if 'water_load' in model_dictionaries:
                    model_dictionaries['water_load'] = {
                        (h, loc): value * cfg.load_multiplier
                        for (h, loc), value in model_dictionaries['water_load'].items()
                    }
            
            # Create config by merging original with experiment overrides
            experiment_config = config.copy()  # Start with your working config
            experiment_config.update(cfg.to_model_config())  # Override with sweep parameters
            
            print(f"Using config: {experiment_config}")  # Debug print
        
            # 2. Run optimization
            opt_model, solution = run_datacenter_optimization(
                model_dictionaries=model_dictionaries,
                config=cfg.to_model_config(),
                cost_params=cfg.to_cost_params(),
                trans_rating = trans_rating,
                trans_cost = trans_cost,
                solver_name=cfg.solver_name,
                processor=processor,
                TimeLimit=cfg.solver_time_limit,
                MIPGap=cfg.solver_mip_gap,
                storage_system = StorageTemplates.create_lithium_ion("my_battery"),
                plant_systems = {'smr': PlantTemplates.create_smr_plant("my_smr", 2500000)}
            )
            
            # 3. Extract metrics
            metrics = self.extract_metrics(solution, model_dictionaries, cfg)
            metrics['status'] = 'success'
            
        except Exception as e:
            print(f"Error in experiment: {e}")
            metrics = {
                'status': 'failed',
                'error': str(e),
                'objective_value': None,
                'selected_location': None,
                'total_renewable_capacity': None,
                'renewable_utilization': None,
                'grid_dependence': None
            }
        
        return metrics

    def extract_metrics(self, solution: Dict, model_dictionaries: Dict, cfg: OptimizationConfig) -> Dict[str, Any]:
        """Extract key metrics from solution."""
        
        if not solution['selected_locations']:
            return {
                'status': 'failed',
                'objective_value': float('inf'),
                'num_locations': 0
            }
        
        # Basic metrics
        metrics = {
            'status': 'success',
            'objective_value': solution['objective_value'],
            'solver_status': solution['status'],
            'num_locations': len(solution['selected_locations']),
            'all_locations': str(solution['selected_locations'])  # Convert to string for CSV
        }
        
        # Aggregate metrics across ALL locations
        total_solar_cap = 0
        total_wind_cap = 0
        total_geo_cap = 0
        total_renewable_gen = 0
        total_grid_purchases = 0
        # Add water cost calculation
        total_water_cost = 0
        total_water_consumption = 0
        
        for loc in solution['selected_locations']:
            # Capacity
            total_solar_cap += model_dictionaries['solar_capacity'].get(loc, 0)
            total_wind_cap += model_dictionaries['wind_capacity'].get(loc, 0)
            total_geo_cap += model_dictionaries.get('geo_capacity', {}).get(loc, 0)
            
            # Generation
            if loc in solution['generation_dispatch']:
                dispatch = solution['generation_dispatch'][loc]
                grid_purchases = solution['grid_purchases'][loc]
                
                total_renewable_gen += sum(
                    dispatch[h]['solar_to_load'] + dispatch[h]['wind_to_load']
                    for h in dispatch.keys()
                )
                total_grid_purchases += sum(grid_purchases.values())

            # Check water
            if loc in solution.get('water_consumption', {}):
                water_data = solution['water_consumption'][loc]
                total_water_consumption += sum(water_data.values())
                # Estimate water cost if you have price data
                if loc in model_dictionaries.get('water_price', {}):
                    water_price = model_dictionaries['water_price'][loc]
                    total_water_cost += sum(water_data.values()) * water_price
        
        total_load = total_renewable_gen + total_grid_purchases
        
        metrics.update({
            'total_solar_capacity': total_solar_cap,
            'total_wind_capacity': total_wind_cap,
            'total_geo_capacity': total_geo_cap,
            'total_renewable_capacity': total_solar_cap + total_wind_cap + total_geo_cap,
            'total_renewable_generation': total_renewable_gen,
            'total_grid_purchases': total_grid_purchases,
            'total_load': total_load,
            'renewable_utilization': total_renewable_gen / total_load if total_load > 0 else 0,
            'grid_dependence': total_grid_purchases / total_load if total_load > 0 else 0,
            'avg_solar_per_location': total_solar_cap / len(solution['selected_locations']),
            'avg_wind_per_location': total_wind_cap / len(solution['selected_locations']),
            'total_water_consumption': total_water_consumption,
            'estimated_water_cost': total_water_cost * 25 * 91.25  # Scale to match objective
        })
        
        return metrics
        
    def run_sweep(self, base_config: OptimizationConfig, sweep_dict: Dict[str, List]) -> List[Tuple[OptimizationConfig, Dict]]:
        """Run a parameter sweep."""
        
        configs = make_configs(base_config, sweep_dict)
        print(f"Running sweep with {len(configs)} configurations")
        
        results = []
        for i, cfg in enumerate(configs, start=1):
            # Make unique experiment name
            name_bits = [f"{k}={getattr(cfg, k)}" for k in sweep_dict.keys()]
            cfg = cfg.with_overrides(exp_name=f"{base_config.exp_name}__{i:03d}__" + "__".join(name_bits))
            
            metrics = self.run_experiment(cfg)
            results.append((cfg, metrics))
        
        self.results.extend(results)
        return results
    
    def save_results(self, results: List[Tuple[OptimizationConfig, Dict]], output_dir: str = "results"):
        """Save results to CSV files."""
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Combined results file
        rows = []
        for cfg, metrics in results:
            row = to_dict(cfg)
            row.update(metrics)
            rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(output_path / "all_results.csv", index=False)
        print(f"Saved combined results to {output_path / 'all_results.csv'}")
        
        # Individual result files
        for cfg, metrics in results:
            row = to_dict(cfg)
            row.update(metrics)
            df_single = pd.DataFrame([row])
            df_single.to_csv(output_path / f"{cfg.exp_name}_results.csv", index=False)
        
        # Summary statistics
        self.generate_summary(df, output_path)
    
    def generate_summary(self, df: pd.DataFrame, output_path: Path):
        """Generate summary statistics."""
        
        summary = {
            'total_experiments': len(df),
            'successful_experiments': len(df[df['status'] == 'success']),
            'failed_experiments': len(df[df['status'] == 'failed']),
        }
        
        if len(df[df['status'] == 'success']) > 0:
            success_df = df[df['status'] == 'success']
            summary.update({
                'best_objective': success_df['objective_value'].min(),
                'worst_objective': success_df['objective_value'].max(),
                'avg_objective': success_df['objective_value'].mean(),
                'avg_renewable_utilization': success_df['renewable_utilization'].mean(),
                'avg_grid_dependence': success_df['grid_dependence'].mean()
            })
        
        # Save summary
        with open(output_path / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Summary: {summary}")

def to_dict(cfg: OptimizationConfig) -> Dict[str, Any]:
    """Convert config to dictionary."""
    return asdict(cfg)

def make_configs(base_config: OptimizationConfig, sweep_dict: Dict[str, List]) -> List[OptimizationConfig]:
    """
    Generate all combinations of configurations from sweep parameters.
    
    Args:
        base_config: Base configuration
        sweep_dict: Dictionary of parameter names to lists of values to sweep
        
    Returns:
        List of configurations covering all parameter combinations
    """
    # Get all parameter combinations
    keys = list(sweep_dict.keys())
    values = list(sweep_dict.values())
    
    configs = []
    for combination in itertools.product(*values):
        # Create parameter dictionary for this combination
        params = dict(zip(keys, combination))
        
        # Create new config with these parameters
        config = base_config.with_overrides(**params)
        configs.append(config)
    
    return configs


# Example usage
if __name__ == "__main__":
    
    # Define file paths
    file_paths = {
        'state_shapefile': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/cb_2022_us_state_20m/cb_2022_us_state_20m.shp',
        'supply_data': '/Users/maria/Documents/Research/deloitte-proj/telecom-data/supply_data_lat_lon_water_clim.csv',
        'merged_cf': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/merged_hourly_solar_wind_cf.csv',
        'demand_data': 'fake_demand.csv',
        'county2zone': 'CountyMaps/county2zone.csv',
        'hierarchy': 'CountyMaps/hierarchy.csv',
        'electric_prices': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/electric_prices.csv',
        'water_risk': '/Users/maria/Documents/Research/deloitte-proj/deloitte-data/water_risk.gpkg'
    }
    
    # Create experiment runner
    runner = OptimizationExperimentRunner(file_paths)
    
    # Define base configuration
    #base_config = OptimizationConfig(exp_name="1_county_networks_sweep")
    base_config = OptimizationConfig(exp_name="1_renpen_sweep")
    #base_config = OptimizationConfig(exp_name="1_county_dr_sweep")
    
    # Choose sweep type
    #sweep = SWEEP['test_sweep']  # Start with small test
    #sweep = SWEEP['network_type_sweep']
    sweep = SWEEP['ren_pen_sweep']
    #sweep = SWEEP['discount_rate_sweep']
    
    # Run sweep
    results = runner.run_sweep(base_config, sweep)
    
    # Save results
    runner.save_results(results)
    
    print(f"\n=== EXPERIMENT COMPLETE ===")
    print(f"Ran {len(results)} experiments")
    print("Check 'results/' directory for output files")

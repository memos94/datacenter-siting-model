import pyomo.environ as pyo
from pyomo.environ import (ConcreteModel, Set, Param, Var, Objective, Constraint, 
                          Binary, NonNegativeReals, PositiveReals, SolverFactory, 
                          Piecewise, minimize, value)
from typing import Dict, Any, Optional, Tuple
import pandas as pd
from config import config

from data_loader import process_data_pipeline
from cost_dict import *
from pyomo.environ import *
from components.storage import *
from components.constraints import *
from components.plant import *


class SitingModel:
    '''
    Pyomo model for data center siting and energy collocation optimization
    '''

    def __init__(self, config: Dict[str, Any], processor, storage_system: Optional[Storage] = None, plant_systems: Optional[Plant] = None):
        """
        Initialize the optimization model.
        
        Args:
            config: Configuration dictionary with model parameters
            processor: Data processor instance
            storage_system: Storage object from Storage class
            plant_systems: Plant object from Plant class
        """
        self.config = config
        self.model = ConcreteModel()
        self.processor = processor
        self.model_dictionaries = {}
        self.results = None

        # Initialize storage system
        if storage_system is None:
            # Default to lithium-ion if not specified
            self.storage = StorageTemplates.create_lithium_ion("default_battery")
        else:
            self.storage = storage_system
            
        print(f"Initialized with {self.storage.storage_type} storage system")

        # Initialize plant systems
        self.plants = plant_systems or {}
        self.use_plants = len(self.plants) > 0
        if self.use_plants:
            print(f"Initialized with {len(self.plants)} plant types: {list(self.plants.keys())}")

    def load_data(self, model_dictionaries: Dict[str, Dict]):
        """
        Load processed data dictionaries into the model.
        
        Args:
            model_dictionaries: Dictionary of model parameters from EnergyDataProcessor
        """
        self.model_dictionaries = model_dictionaries
        print(f"Loaded {len(model_dictionaries)} data dictionaries")

    def build_sets(self, processor):
        """Build Pyomo sets from data."""
    
        merged_gen = self.processor.processed_data.get('merged_gen')

        if merged_gen is None:
            raise ValueError("No merged generation data found")
        
        gen_df = self.processor.processed_data['merged_gen']
        locations = gen_df['location'].astype(int).unique()
        hours = gen_df['hour'].astype(int).unique()

        self.model.LOCATIONS = Set(initialize=locations)
        self.model.HOURS = Set(initialize=hours)

        # Plant types set
        if self.use_plants:
            plant_types = list(self.plants.keys())
            self.model.PLANTS = Set(initialize=plant_types)
        
        print(f"Built sets: {len(locations)} locations, {len(hours)} hours")
        print(f"Location range: {min(locations)} to {max(locations)}")
        print(f"Hour range: {min(hours)} to {max(hours)}")
        if self.use_plants:
            print(f"Plant types: {list(self.model.PLANTS)}")

    def build_parameters(self):
        """Build Pyomo parameters from the data dictionaries."""
        print("Building model parameters...")

        # Renewable capacity parameters (MW)
        self.model.solar_cap = Param(
            self.model.LOCATIONS, 
            initialize=self.model_dictionaries['solar_capacity']
        )
        self.model.wind_cap = Param(
            self.model.LOCATIONS, 
            initialize=self.model_dictionaries['wind_capacity']
        )
        self.model.geo_45_cap = Param(
            self.model.LOCATIONS, 
            initialize=self.model_dictionaries.get('geo_capacity', {})
        )
        
        # Generation parameters (MWh)
        self.model.solar_gen = Param(
            self.model.HOURS, self.model.LOCATIONS,
            initialize=self.model_dictionaries['solar_generation']
        )
        self.model.wind_gen = Param(
            self.model.HOURS, self.model.LOCATIONS,
            initialize=self.model_dictionaries['wind_generation']
        )
        self.model.geo_45_gen = Param(
            self.model.HOURS, self.model.LOCATIONS,
            initialize=self.model_dictionaries.get('geo_generation', {})
        )
        
        # Demand parameters
        self.model.load_datacenter = Param(
            self.model.HOURS, self.model.LOCATIONS,
            initialize=self.model_dictionaries['energy_load']
        )
        self.model.water_load = Param(
            self.model.HOURS, self.model.LOCATIONS,
            initialize=self.model_dictionaries['water_load']
        )

        # Calculate max datacenter load from energy_load dictionary
        max_load = max(self.model_dictionaries['energy_load'].values())

        self.model.cap_datacenter = Param(initialize=max_load, mutable=False)

        # Storage parameters from Storage class
        storage_capacity_mwh = self.config.get("storage_capacity_mwh", 100)

        self.model.storage_capacity = Param(initialize=storage_capacity_mwh, mutable=False)
        self.model.storage_charge_eff = Param(initialize=self.storage.charge_efficiency)
        self.model.storage_discharge_eff = Param(initialize=self.storage.discharge_efficiency)
        self.model.storage_self_discharge = Param(initialize=self.storage.self_discharge)
        self.model.storage_min_soc = Param(initialize=self.storage.min_soc)
        self.model.storage_max_soc = Param(initialize=self.storage.max_soc)
        
         # Price parameters
        self.model.grid_price = Param( self.model.LOCATIONS, initialize=self.model_dictionaries['electric_price'])

        # Water price (need to create hourly version)
        def init_hourly_water_price(m, h, loc):
            return self.model_dictionaries['water_price'].get(loc, 0)
        
        self.model.water_price_hourly = Param(
            self.model.HOURS, self.model.LOCATIONS, initialize=init_hourly_water_price)
        
        # Infrastructure parameters
        self.model.trans_dist = Param(
            self.model.LOCATIONS,
            initialize=self.model_dictionaries.get('trans_dist', {})
        )
        self.model.trans_multiplier = Param(
            self.model.LOCATIONS,
            initialize=self.model_dictionaries.get('trans_multiplier', {})
        )
        self.model.telecom_dist = Param(
            self.model.LOCATIONS,
            initialize=self.model_dictionaries.get('telecom_dist', {})
        )
        self.model.telecom_cost = Param(
            self.model.LOCATIONS,
            initialize=self.model_dictionaries.get('telecom_cost', {})
        )
        
        # Risk parameters
        self.model.water_risk = Param(
            self.model.LOCATIONS,
            initialize=self.model_dictionaries.get('water_risk', {})
        )
        
        # Model configuration parameters
        self.model.curtail_penalty = Param(initialize=self.config.get('curtail_penalty', 20))
        self.model.ren_export_price = Param(initialize=self.config.get('ren_export_price', 2))
        self.model.ren_penetration = Param(initialize=self.config.get('ren_penetration', 1))
        self.model.slack_penalty = Param(default=1e5, mutable=False)

    
        # Plant parameters (if using plant systems) - indexed by plant type
        if self.use_plants:
            def init_plant_capacity(m, p):
                return self.plants[p].capacity_kw / 1000  # Convert kW to MW
            
            def init_plant_min_output(m, p):
                return self.plants[p].get_min_output_kw() / 1000
            
            def init_plant_max_ramp(m, p):
                return self.plants[p].get_max_ramp_kw() / 1000
            
            # Indexed  by plant type
            self.model.plant_capacity = Param(
                self.model.PLANTS, initialize=init_plant_capacity, mutable=False
            )
            self.model.plant_min_output = Param(
                self.model.PLANTS, initialize=init_plant_min_output, mutable=False
            )
            self.model.plant_max_ramp = Param(
                self.model.PLANTS, initialize=init_plant_max_ramp, mutable=False
            )
        

    def build_variables(self):
            """Build decision variables."""
            print("Building decision variables...")
            
            # Location decision variable
            self.model.x = Var(self.model.LOCATIONS, within=pyo.Binary)
            
            # Power routing variables (MW)
            self.model.wind_to_load = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            self.model.wind_to_storage = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            self.model.wind_to_grid = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            self.model.wind_curtailed = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            
            self.model.solar_to_load = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            self.model.solar_to_storage = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            self.model.solar_to_grid = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            self.model.solar_curtailed = Var(
                self.model.HOURS, self.model.LOCATIONS, domain=NonNegativeReals)
            
            # Grid variable
            self.model.P_g = Var(
                self.model.HOURS, self.model.LOCATIONS, within=NonNegativeReals)
            

            # Storage variables - using Storage class attributes
            self.model.storage_charge = Var(self.model.HOURS, self.model.LOCATIONS, within=NonNegativeReals)
            self.model.storage_discharge = Var(self.model.HOURS, self.model.LOCATIONS, within=NonNegativeReals)
            self.model.storage_energy = Var(self.model.HOURS, self.model.LOCATIONS, within=NonNegativeReals)
                
            # Transmission capacity variable
            self.model.transmission_capacity = Var(
                self.model.LOCATIONS, domain=NonNegativeReals, bounds=(0, self.model.cap_datacenter))
            self.model.trans_cap_cost = Var(self.model.LOCATIONS, domain=NonNegativeReals)
            
            # Plant variables (if using plant system)
            if self.use_plants:
                self.model.plant_output = Var(self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, within=NonNegativeReals)
                self.model.plant_online = Var(self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, within=Binary)
                self.model.plant_startup = Var(self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, within=Binary)
            
    def build_transmission_cost_piecewise(self, trans_rating: Dict, trans_cost: Dict):
        """
        Build piecewise linear transmission cost constraints.
        
        Args:
            trans_rating: Dictionary mapping capacity to rating
            trans_cost: Dictionary mapping rating to cost per mile
        """
        breakpoints = sorted(trans_rating.keys())
        costs_per_mw = [trans_cost[trans_rating[b]] for b in breakpoints]
        
        self.model.trans_cost_piecewise = Piecewise(
            self.model.LOCATIONS,                              # index
            self.model.trans_cap_cost,                         # output y
            self.model.transmission_capacity,                  # input x
            pw_pts=breakpoints,                                # list of x-values (capacity tiers)                       # ($)
            f_rule=costs_per_mw,                               # cost per MW (not multiplied by cap!)
            pw_constr_type='EQ',                               # has to be equal I guess
            pw_repn='INC'                                      # step-function, constant until next breakpoint
        ) 
        '''
        else
        self.model.trans_cost_piecewise = Constraint(
            self.model.LOCATIONS,
            rule=lambda m, i: pyo.Constraint.Skip
        )'''
        
        self.model.substation_cost = Var(self.model.LOCATIONS, domain=NonNegativeReals)

        self.model.subs_piecewise = Piecewise(
            self.model.LOCATIONS,
            self.model.substation_cost,
            self.model.transmission_capacity,
            pw_pts=list(subs_new_cost.keys()),
            f_rule=list(subs_new_cost.values()),
            pw_constr_type="EQ",
            pw_repn="INC"
        )

    def build_grid_transmission_constraints(self):
        """
        Add constraints to disable grid purchases and exports if transmission is not included.
        This should be called AFTER build_objective since it uses self.model.include_transmission_cost
        """
        if hasattr(self.model, 'include_transmission') and not self.model.include_transmission:
            print("Adding constraints: No grid purchases or exports (no transmission)")
            
            # Constraint: No grid purchases if no transmission
            def no_grid_purchase_rule(m, h, loc):
                return m.P_g[h, loc] == 0
            self.model.no_grid_purchase = Constraint(
                self.model.HOURS, self.model.LOCATIONS, 
                rule=no_grid_purchase_rule
            )
            
            # Constraint: No renewable exports if no transmission
            def no_renewable_export_rule(m, h, loc):
                return m.wind_to_grid[h, loc] + m.solar_to_grid[h, loc] == 0
            self.model.no_renewable_export = Constraint(
                self.model.HOURS, self.model.LOCATIONS,
                rule=no_renewable_export_rule
            )
            
            '''
            # Constraint: Transmission capacity must be zero
            def no_transmission_capacity_rule(m, loc):
                return m.transmission_capacity[loc] == 0
            self.model.no_transmission_capacity = Constraint(
                self.model.LOCATIONS,
                rule=no_transmission_capacity_rule
            )
            '''
            
            print("  - Grid purchases set to 0")
            print("  - Renewable exports set to 0")
            print("  - Transmission capacity set to 0")
        else:
            print("Transmission included: Grid purchases and exports enabled")
            
    def build_constraints(self):
        """Build model constraints."""
        print("Building constraints...")
        
        # Renewable energy balance constraints
        def wind_balance_rule(m, h, loc):
            return (
                m.wind_to_load[h, loc] +
                m.wind_to_storage[h, loc] +
                m.wind_to_grid[h, loc] +
                m.wind_curtailed[h, loc]
                <= m.wind_gen[h, loc]
            )
        self.model.wind_balance = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=wind_balance_rule)
        
        def solar_balance_rule(m, h, loc):
            return (
                m.solar_to_load[h, loc] +
                m.solar_to_storage[h, loc] +
                m.solar_to_grid[h, loc] +
                m.solar_curtailed[h, loc]
                <= m.solar_gen[h, loc]
            )
        self.model.solar_balance = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=solar_balance_rule)
        
        # Load constraints
        def ren_penetration_load_rule(m, h, loc):
            return m.wind_to_load[h, loc] + m.solar_to_load[h, loc] <= m.load_datacenter[h, loc] * m.ren_penetration
        self.model.ren_penetration_to_load_constraint = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=ren_penetration_load_rule)
    
        # # of location constraints
        # Location selection constraint - FIXED VERSION
        min_locations = self.config.get('min_locations', 10)
        max_locations = self.config.get('max_locations', 10)
        
        def min_location_rule(m):
            return sum(m.x[loc] for loc in m.LOCATIONS) >= min_locations
        self.model.min_location_constraint = Constraint(rule=min_location_rule)
        
        if max_locations is not None:
            def max_location_rule(m):
                return sum(m.x[loc] for loc in m.LOCATIONS) <= max_locations
            self.model.max_location_constraint = Constraint(rule=max_location_rule)
        
        # Load balance constraint
        def load_balance_rule(m, h, loc):
            generation = (m.solar_to_load[h, loc] + 
                         m.wind_to_load[h, loc] + 
                         m.geo_45_gen[h, loc] + 
                         m.storage_discharge[h, loc] )
            
            # Only include grid import if transmission is active
            if hasattr(m, 'P_g') and getattr(m, 'include_transmission', True):
                generation += m.P_g[h, loc]
                         
            if self.use_plants:
                # Sum across all plant types
                generation += sum(m.plant_output[h, loc, p] for p in m.PLANTS)
            
            return generation >= m.load_datacenter[h, loc]
        self.model.load_balance = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=load_balance_rule)

        # Power Plant Constraints
        # Plant constraints (if using plant systems) - using PlantConstraints class
        if self.use_plants:
            # Build plant_params dictionary structure expected by PlantConstraints
            plant_params = {}
            for loc in self.model.LOCATIONS:
                for p in self.model.PLANTS:
                    # Create composite key (loc, p) for the dictionary
                    plant_params[(loc, p)] = {
                        'capacity_kw': self.plants[p].capacity_kw,
                        'min_output_kw': self.plants[p].get_min_output_kw(),
                        'max_ramp_kw': self.plants[p].get_max_ramp_kw(),
                        'availability': {h: 1.0 for h in self.model.HOURS}  # 100% availability
                    }
            
            # Wrapper to adapt indices: PlantConstraints expects (t, p) but we have (h, loc, p)
            def adapted_min_output(m, h, loc, p):
                key = (loc, p)
                capacity_kw = plant_params[key]['capacity_kw']
                min_output_kw = plant_params[key]['min_output_kw']
                if h == m.HOURS.first():
                    return m.plant_output[h, loc, p] >= (min_output_kw/ 1000) * m.x[loc]
    
                return m.plant_output[h, loc, p] >= (min_output_kw / 1000) * m.plant_online[h, loc, p]
            
            def adapted_max_output(m, h, loc, p):
                key = (loc, p)
                capacity_kw = plant_params[key]['capacity_kw']
                availability = plant_params[key]['availability'][h]
                return m.plant_output[h, loc, p] <= (capacity_kw / 1000) * availability * m.plant_online[h, loc, p]
            
            def adapted_ramp_up(m, h, loc, p):
                if h == m.HOURS.first():
                    return pyo.Constraint.Skip
                key = (loc, p)
                max_ramp_kw = plant_params[key]['max_ramp_kw']
                prev_h = h - 1
                return m.plant_output[h, loc, p] - m.plant_output[prev_h, loc, p] <= max_ramp_kw / 1000
            
            def adapted_ramp_down(m, h, loc, p):
                if h == m.HOURS.first():
                    return pyo.Constraint.Skip
                key = (loc, p)
                max_ramp_kw = plant_params[key]['max_ramp_kw']
                prev_h = h - 1
                return m.plant_output[prev_h, loc, p] - m.plant_output[h, loc, p] <= max_ramp_kw / 1000
            
            def adapted_startup(m, h, loc, p):
                if h == m.HOURS.first():
                    # At first hour, plant is online if location is selected (warm start for baseload plants)
                    return m.plant_startup[h, loc, p] == m.x[loc]
                    #return m.plant_startup[h, loc, p] >= m.plant_online[h, loc, p]
                prev_h = h - 1
                return m.plant_startup[h, loc, p] >= m.plant_online[h, loc, p] - m.plant_online[prev_h, loc, p]
            
            # Apply adapted constraints
            self.model.plant_min_output_constraint = Constraint(
                self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, rule=adapted_min_output)
            
            self.model.plant_max_output_constraint = Constraint(
                self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, rule=adapted_max_output)
            
            self.model.plant_ramp_up_constraint = Constraint(
                self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, rule=adapted_ramp_up)
            
            self.model.plant_ramp_down_constraint = Constraint(
                self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, rule=adapted_ramp_down)
            
            self.model.plant_startup_constraint = Constraint(
                self.model.HOURS, self.model.LOCATIONS, self.model.PLANTS, rule=adapted_startup)
        
        
        # Renewable export and transmission limits
        def ren_export_limit_rule(m, h, loc):
            if hasattr(m, 'P_g') and getattr(m, 'include_transmission', True):
                return (m.wind_to_grid[h, loc] + m.solar_to_grid[h, loc] +  m.P_g[h, loc]) * 1.05  <= m.transmission_capacity[loc]
            else:
                return m.wind_to_grid[h, loc] + m.solar_to_grid[h, loc] == 0
        self.model.ren_export_limit = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=ren_export_limit_rule)
    
        # Grid capacity constraint
        '''
        def grid_capacity_rule(m, h, loc):
            return m.P_g[h, loc] * 1.05 <= m.transmission_capacity[loc]
        self.model.grid_capacity_constraint = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=grid_capacity_rule)
        '''
        
        #### STORAGE CONSTRAINTS
        # Storage charge balance
        def storage_charge_balance_rule(m, h, loc):
            return (m.wind_to_storage[h, loc] + m.solar_to_storage[h, loc]) == m.storage_charge[h, loc]
        self.model.storage_charge_balance = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=storage_charge_balance_rule)
        
        # Load constraints
        def storage_load_rule(m, h, loc):
            return m.storage_discharge[h,loc] <= m.load_datacenter[h, loc] * 1.05
        self.model.storage_to_load_constraint = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=storage_load_rule)

        # Max charge rate (from Storage class)
        def max_charge_rule(m, h, loc):
            return m.storage_charge[h, loc] <= self.storage.max_c_rate * self.storage.storage_capacity
        self.model.storage_max_charge = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=max_charge_rule)
        
        # Max discharge rate (from Storage class)
        def max_discharge_rule(m, h, loc):
            return m.storage_discharge[h, loc] <= self.storage.max_d_rate * self.storage.storage_capacity
        self.model.storage_max_discharge = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=max_discharge_rule)
        
        # Min SOC (from Storage class)
        def min_soc_rule(m, h, loc):
            return m.storage_energy[h, loc] >= self.storage.min_soc * self.storage.storage_capacity
        self.model.storage_min_soc = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=min_soc_rule)
        
        # Max SOC (from Storage class)
        def max_soc_rule(m, h, loc):
            return m.storage_energy[h, loc] <= self.storage.max_soc * self.storage.storage_capacity
        self.model.storage_max_soc = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=max_soc_rule)

        # Energy balance - subsequent hours with self-discharge
        def storage_energy_dynamics_rule(m, h, loc):
            if h == m.HOURS.first():
                return m.storage_energy[h, loc] == 0.5 * self.storage.storage_capacity
            else:
                prev_h = h - 1
                return m.storage_energy[h, loc] == (1 - self.storage.self_discharge) * m.storage_energy[prev_h, loc] + \
                    self.storage.charge_efficiency * m.storage_charge[h, loc] - m.storage_discharge[h, loc] * self.storage.discharge_efficiency
        self.model.storage_energy_dynamics = Constraint(self.model.HOURS, self.model.LOCATIONS, rule=storage_energy_dynamics_rule)


    def build_objective(self, cost_params: Dict[str, Dict]):
        """
        Build the objective function.
        
        Args:
            cost_params: Dictionary containing cost parameters:
                - variable_gen_cost: Variable generation costs by technology
                - capital_gen_cost: Capital costs by technology  
                - fixed_gen_cost: Fixed O&M costs by technology
                - subs_new_cost: Substation costs
                - trans_cap_amount: Transmission capacity amount
        """
        print("Building objective function...")
        
        variable_gen_cost = cost_params.get('variable_gen_cost', {})
        capital_gen_cost = cost_params.get('capital_gen_cost', {})
        fixed_gen_cost = cost_params.get('fixed_gen_cost', {})
        subs_new_cost = cost_params.get('subs_new_cost', {})
        trans_cap_amount = trans_capacity[self.model.cap_datacenter]

        # Calculate CRF (Capital Recovery Factor) for ALL capital costs
        project_lifetime = self.config.get('project_lifetime', 20)
        discount_rate = self.config.get('discount_rate', 0.012) # its a %
        crf = discount_rate * (1 + discount_rate)**project_lifetime / \
              ((1 + discount_rate)**project_lifetime - 1)
        
        print(f"Using discount rate: {discount_rate:.1%}, CRF: {crf:.4f}")
        
        # Network toggles
        include_transmission = cost_params.get('include_transmission_cost', True)
        include_telecom = cost_params.get('include_telecom_cost', True)
        include_water = cost_params.get('include_water_cost', True)

        # Store toggle in model for use in constraints
        self.model.include_transmission = include_transmission
        self.model.include_telecom = include_telecom
        self.model.include_water = include_water
        
        # ADD THIS DEBUG:
        print(f"DEBUG: cost_params keys = {cost_params.keys()}")
        print(f"DEBUG: include_water_cost in cost_params = {cost_params.get('include_water_cost')}")

        print(f"  Transmission costs: {'INCLUDED' if include_transmission else 'EXCLUDED'}")
        print(f"  Telecom costs: {'INCLUDED' if include_telecom else 'EXCLUDED'}")
        print(f"  Water costs: {'INCLUDED' if include_water else 'EXCLUDED'}")
        
        def objective_rule(m):

            # Annual operating costs (20 years * year conversion factor)
            operating_costs = project_lifetime * 91.25 * (
                # Variable generation costs
                sum(m.x[loc] * (
                    m.solar_to_load[h, loc] * variable_gen_cost['solar'] + 
                    m.wind_to_load[h, loc] * variable_gen_cost['wind'] +
                    m.geo_45_gen[h, loc] * variable_gen_cost['geo_45'] + 
                    (m.storage_charge[h,loc]+m.storage_discharge[h,loc]) * self.storage.opex_fraction *1000
                ) for h in m.HOURS for loc in m.LOCATIONS) +
                
                # Curtailment penalties
                sum(m.x[loc] * (
                    m.curtail_penalty * (m.wind_curtailed[h, loc] + m.solar_curtailed[h, loc]) 
                ) for h in m.HOURS for loc in m.LOCATIONS)
            ) 

            # Water costs (conditional)
            if m.include_water:
                water_costs = 25 * 91.25 * sum(
                    m.x[loc] * m.water_load[h, loc] * m.water_price_hourly[h, loc]  #+ m.water_risk[loc]* self.config.get('water_risk_penalty')
                    for h in m.HOURS for loc in m.LOCATIONS
                )
                print(f"Water costs term INCLUDED in objective")
            else:
                water_costs = 0
                print(f"Water costs EXCLUDED from objective")
            
            # Capital costs (one-time)
            capital_costs = crf * sum(m.x[loc] * (
                m.solar_cap[loc] * capital_gen_cost['solar'] + 
                m.wind_cap[loc] * capital_gen_cost['wind']+
                m.geo_45_cap[loc] * capital_gen_cost['geo_45'] + 
                self.storage.storage_capacity * self.storage.capex_per_kw * 1000 +
                subs_new_cost[trans_cap_amount] # since modelling large load assuming always need more substation even if not more transmission for the sake of this analysis
            ) for loc in m.LOCATIONS)
            
            # Fixed yearly O&M costs
            fixed_costs = project_lifetime * sum(m.x[loc] * (
                m.solar_cap[loc] * fixed_gen_cost['solar'] + 
                m.wind_cap[loc] * fixed_gen_cost['wind'] +
                m.geo_45_cap[loc] * fixed_gen_cost['geo_45'] +
                self.storage.storage_capacity * 1000 * self.storage.capex_per_kwh * self.storage.opex_fraction
            ) for loc in m.LOCATIONS) 

            # Transmission capital costs (conditional)
            if m.include_transmission:
                transmission_costs = crf * sum(m.x[loc] * (
                    m.trans_dist[loc] * 1.60934 * m.trans_cap_cost[loc] * m.trans_multiplier[loc]
                ) for loc in m.LOCATIONS)
                grid_costs = sum(m.x[loc] * (100 * m.P_g[h, loc] * m.grid_price[loc] -
                    m.ren_export_price * (m.wind_to_grid[h, loc] + m.solar_to_grid[h, loc])) for h in m.HOURS for loc in m.LOCATIONS)
                print(f"Transmission costs term INCLUDED in objective")
            else:
                transmission_costs = 0
                grid_costs = 0
                print(f"Transmission costs term EXCLUDED in objective")
            
            # Telecom costs (conditional)
            if m.include_telecom:
                telecom_costs = crf * sum(
                    m.x[loc] * 1.60934 * m.telecom_cost[loc] 
                    for loc in m.LOCATIONS
                )
                print(f"Telecom costs term INCLUDED in objective")
            else:
                telecom_costs = 0
                print(f"Telecom costs term EXCLUDED in objective")
            
            
            # Plant costs (if using plant systems)
            if self.use_plants:
                # Plant variable costs (fuel cost) - sum across all plant types
                plant_var_costs = project_lifetime * 91.25 * sum(
                    m.x[loc] * m.plant_output[h, loc, p] * (self.plants[p].get_fuel_cost_per_kwh() * 1000)
                    for h in m.HOURS for loc in m.LOCATIONS for p in m.PLANTS
                )
                
                # Plant startup costs - sum across all plant types
                plant_startup_costs = project_lifetime * 91.25 * sum(
                    m.x[loc] * m.plant_startup[h, loc, p] * self.plants[p].get_startup_cost()
                    for h in m.HOURS for loc in m.LOCATIONS for p in m.PLANTS
                )
                
                # Plant capital costs (annualized) - sum across all plant types
                plant_capital = crf * sum(
                    m.x[loc] * self.plants[p].get_annual_capex(1.0) 
                    for loc in m.LOCATIONS for p in m.PLANTS
                )
                
                # Plant fixed O&M - sum across all plant types
                plant_fixed = project_lifetime * sum(
                    m.x[loc] * self.plants[p].get_annual_opex() 
                    for loc in m.LOCATIONS for p in m.PLANTS
                )
                
                operating_costs += plant_var_costs + plant_startup_costs
                capital_costs += plant_capital
                fixed_costs += plant_fixed

            return operating_costs + capital_costs + fixed_costs + telecom_costs+  water_costs + transmission_costs + telecom_costs + grid_costs
        
        self.model.objective = Objective(rule=objective_rule, sense=minimize)
    
        
    def build_complete_model(self, cost_params: Dict, 
                            trans_rating: Dict,
                            trans_cost: Dict, 
                            ):
        """
        Build the complete optimization model.
        
        Args:
            cost_params: Cost parameter dictionaries
            trans_rating: Transmission rating dictionary (optional)
            trans_cost: Transmission cost dictionary (optional)
        """
        print("Building complete optimization model...")
        
        self.build_sets(self.processor)
       
        include_transmission = cost_params.get('include_transmission_cost', True)
        include_telecom = cost_params.get('include_telecom_cost', True)
        include_water = cost_params.get('include_water_cost', True)

        self.build_parameters()
        self.model.include_transmission = include_transmission
        self.model.include_telecom = include_telecom
        self.model.include_water = include_water

        self.build_variables()
        
        # Build transmission piecewise costs
        self.build_transmission_cost_piecewise(trans_rating, trans_cost)
        
        self.build_constraints()
        self.build_objective(cost_params)

        # IMPORTANT: Add grid/transmission constraints AFTER objective since it needs self.model.include_transmission to be set
        self.build_grid_transmission_constraints()
        
        print("Model build complete!")
        print(f"Storage system: {self.storage.storage_type}")
        
    # def solve(self, solver_name: str = 'cbc', **solver_options) -> Dict:
    #     """
    #     Solve the optimization model.
        
    #     Args:
    #         solver_name: Name of the solver to use
    #         **solver_options: Additional solver options
            
    #     Returns:
    #         Dictionary with solution results
    #     """
    #     print(f"Solving model with {solver_name}...")
        
    #     # Check solver availability
    #     if not SolverFactory(solver_name).available():
    #         raise ValueError(f"Solver {solver_name} is not available")
            
    #     solver = SolverFactory(solver_name)
        
    #     # Set solver options
    #     # 2. cbc
    #     if solver_name == 'cbc':
    #         # 'ratio' es el equivalente al 'mipgap' (0.05 = 5% de margen de error)
    #         solver.options['ratio'] = solver_options.get('mipgap', 0.05)
    #         # 'sec' es el tiempo máximo en segundos antes de detenerse
    #         solver.options['sec'] = solver_options.get('timeout', 600)
    #         # 'threads' para usar múltiples núcleos de tu CPU
    #         solver.options['threads'] = 4
    #     else:
    #         # Mantener compatibilidad con otros solvers
    #         for option, val in solver_options.items():
    #             solver.options[option] = val
            
    #     # Solve the model
    #     self.results = solver.solve(self.model, tee=True)

    #     # Check solution status
    #     termination = self.results.solver.termination_condition
        
    #     if termination == pyo.TerminationCondition.infeasibleOrUnbounded:
    #         print("\n" + "="*60)
    #         print("MODEL IS INFEASIBLE OR UNBOUNDED")
    #         print("="*60)
    #         self.diagnose_infeasibility()
    #         raise ValueError("Model is infeasible or unbounded. See diagnostics above.")
        
    #     if termination == pyo.TerminationCondition.infeasible:
    #         print("\n" + "="*60)
    #         print("MODEL IS INFEASIBLE")
    #         print("="*60)
    #         self.diagnose_infeasibility()
    #         raise ValueError("Model is infeasible. See diagnostics above.")
        
    #     if termination != pyo.TerminationCondition.optimal:
    #         print(f"\nWARNING: Solver terminated with condition: {termination}")

    #     if termination in [pyo.TerminationCondition.infeasible, 
    #                    pyo.TerminationCondition.infeasibleOrUnbounded]:
    #         print("\n" + "="*60)
    #         print("MODEL IS INFEASIBLE")
    #         print("="*60)
            
    #         # Use Pyomo's infeasibility logger
    #         from pyomo.util.infeasible import log_infeasible_constraints
    #         log_infeasible_constraints(self.model, log_expression=True, log_variables=True)
            
    #         self.diagnose_infeasibility()
    #         raise ValueError("Model is infeasible. See diagnostics above.")
        
    #     # Extract solution
    #     solution = self.extract_solution()
        
    #     return solution
    def solve(self, solver_name: str = 'gurobi', **solver_options) -> Dict:
        """
        Solve the optimization model.
        
        Args:
            solver_name: Name of the solver to use
            **solver_options: Additional solver options
            
        Returns:
            Dictionary with solution results
        """
        print(f"Solving model with {solver_name}...")
        
        # Check solver availability
        if not SolverFactory(solver_name).available():
            raise ValueError(f"Solver {solver_name} is not available")
            
        solver = SolverFactory(solver_name)
        
        # Set solver options
        for option, val in solver_options.items():
            solver.options[option] = val
            
        # Solve the model
        self.results = solver.solve(self.model, tee=True)

        # Check solution status
        termination = self.results.solver.termination_condition
        
        if termination == pyo.TerminationCondition.infeasibleOrUnbounded:
            print("\n" + "="*60)
            print("MODEL IS INFEASIBLE OR UNBOUNDED")
            print("="*60)
            self.diagnose_infeasibility()
            raise ValueError("Model is infeasible or unbounded. See diagnostics above.")
        
        if termination == pyo.TerminationCondition.infeasible:
            print("\n" + "="*60)
            print("MODEL IS INFEASIBLE")
            print("="*60)
            self.diagnose_infeasibility()
            raise ValueError("Model is infeasible. See diagnostics above.")
        
        if termination != pyo.TerminationCondition.optimal:
            print(f"\nWARNING: Solver terminated with condition: {termination}")

        if termination in [pyo.TerminationCondition.infeasible, 
                       pyo.TerminationCondition.infeasibleOrUnbounded]:
            print("\n" + "="*60)
            print("MODEL IS INFEASIBLE")
            print("="*60)
            
            # Use Pyomo's infeasibility logger
            from pyomo.util.infeasible import log_infeasible_constraints
            log_infeasible_constraints(self.model, log_expression=True, log_variables=True)
            
            self.diagnose_infeasibility()
            raise ValueError("Model is infeasible. See diagnostics above.")
        
        # Extract solution
        solution = self.extract_solution()
        
        return solution
    
    def diagnose_infeasibility(self):
        """Diagnose model infeasibility by checking constraints and bounds."""
        print("\nDIAGNOSTIC INFORMATION:")
        print("-" * 60)
        
        # Check data availability
        print("\n1. Data Availability:")
        sample_loc = list(self.model.LOCATIONS)[0]
        sample_hour = list(self.model.HOURS)[0]
        
        print(f"   Sample location: {sample_loc}")
        print(f"   Solar capacity: {value(self.model.solar_cap[sample_loc]):.2f} MW")
        print(f"   Wind capacity: {value(self.model.wind_cap[sample_loc]):.2f} MW")
        print(f"   Geo capacity: {value(self.model.geo_45_cap[sample_loc]):.2f} MW")
        print(f"   Sample hour load: {value(self.model.load_datacenter[sample_hour, sample_loc]):.2f} MW")
        
        # Check for zero/negative capacities
        print("\n2. Checking for problematic capacities:")
        zero_solar = sum(1 for loc in self.model.LOCATIONS if value(self.model.solar_cap[loc]) == 0)
        zero_wind = sum(1 for loc in self.model.LOCATIONS if value(self.model.wind_cap[loc]) == 0)
        print(f"   Locations with zero solar: {zero_solar}/{len(self.model.LOCATIONS)}")
        print(f"   Locations with zero wind: {zero_wind}/{len(self.model.LOCATIONS)}")
        
        # Check generation availability
        print("\n3. Checking generation vs load:")
        total_load = sum(value(self.model.load_datacenter[h, sample_loc]) for h in self.model.HOURS)
        total_solar_gen = sum(value(self.model.solar_gen[h, sample_loc]) for h in self.model.HOURS)
        total_wind_gen = sum(value(self.model.wind_gen[h, sample_loc]) for h in self.model.HOURS)
        total_geo_gen = sum(value(self.model.geo_45_gen[h, sample_loc]) for h in self.model.HOURS)
        
        print(f"   Total load (sample loc): {total_load:.2f} MWh")
        print(f"   Total solar gen available: {total_solar_gen:.2f} MWh")
        print(f"   Total wind gen available: {total_wind_gen:.2f} MWh")
        print(f"   Total geo 45 gen available: {total_geo_gen:.2f} MWh")
        
        # Check storage parameters
        print("\n4. Storage parameters:")
        print(f"   Storage capacity: {value(self.model.storage_capacity):.2f} MWh")
        print(f"   Min SOC energy: {self.storage.min_soc * value(self.model.storage_capacity):.2f} MWh")
        print(f"   Max SOC energy: {self.storage.max_soc * value(self.model.storage_capacity):.2f} MWh")
        print(f"   Max charge rate: {self.storage.max_c_rate * value(self.model.storage_capacity):.2f} MW")
        print(f"   Max discharge rate: {self.storage.max_d_rate * value(self.model.storage_capacity):.2f} MW")
        
        # Check location constraints
        print("\n5. Location constraints:")
        min_locs = self.config.get('min_locations', 10)
        max_locs = self.config.get('max_locations', 10)
        print(f"   Min locations required: {min_locs}")
        print(f"   Max locations allowed: {max_locs if max_locs else 'unlimited'}")
        print(f"   Total locations available: {len(self.model.LOCATIONS)}")
        
        # Check plant constraints
        if self.use_plants:
            print("\n6. Plant parameters:")
            for p in self.model.PLANTS:
                print(f"   {p.upper()}:")
                print(f"     Capacity: {value(self.model.plant_capacity[p]):.2f} MW")
                print(f"     Min output: {value(self.model.plant_min_output[p]):.2f} MW")
                print(f"     Max ramp: {value(self.model.plant_max_ramp[p]):.2f} MW/h")
        
        print("\n" + "="*60)
        print("SUGGESTED FIXES:")
        print("1. Check if renewable generation is sufficient to meet load")
        print("2. Verify storage parameters are reasonable")
        print("3. Check if min_locations is achievable with available data")
        print("4. Ensure plant capacities are sufficient if using plants")
        print("5. Try relaxing ren_penetration constraint (currently {:.0%})".format(
            value(self.model.ren_penetration)))
        print("="*60)
            
    def extract_solution(self) -> Dict:
        """Extract and return the solution."""
        if self.results is None:
            raise ValueError("Model has not been solved yet")
        
        print(self.results.solver.termination_condition)
        print(self.results.solver.status)

            
        solution = {
            'status': str(self.results.solver.termination_condition),
            'objective_value': value(self.model.objective),
            'selected_locations': [],
            'generation_dispatch': {},
            'storage_operation': {},
            'grid_purchases': {},
            'curtailment': {}
        }
        
        # Extract selected locations
        for loc in self.model.LOCATIONS:
            if value(self.model.x[loc]) > 0.5:  # Binary variable
                solution['selected_locations'].append(loc)
                
        # Extract dispatch for selected locations
        for loc in solution['selected_locations']:
            solution['generation_dispatch'][loc] = {}
            solution['storage_operation'][loc] = {}
            solution['grid_purchases'][loc] = {}
            solution['curtailment'][loc] = {}
            
            for h in self.model.HOURS:
                solution['generation_dispatch'][loc][h] = {
                    'solar_to_load': value(self.model.solar_to_load[h, loc]),
                    'wind_to_load': value(self.model.wind_to_load[h, loc]),
                    'solar_to_grid': value(self.model.solar_to_grid[h, loc]),
                    'wind_to_grid': value(self.model.wind_to_grid[h, loc])
                }

                if self.use_plants:
                    # Add plant outputs for each plant type
                    for p in self.model.PLANTS:
                        solution['generation_dispatch'][loc][h][f'{p}_output'] = value(self.model.plant_output[h, loc, p])
                        solution['generation_dispatch'][loc][h][f'{p}_online'] = value(self.model.plant_online[h, loc, p])
                        solution['generation_dispatch'][loc][h][f'{p}_startup'] = value(self.model.plant_startup[h, loc, p])

                solution['storage_operation'][loc][h] = {
                    'charge': value(self.model.storage_charge[h, loc]),
                    'discharge': value(self.model.storage_discharge[h, loc]),
                    'energy': value(self.model.storage_energy[h, loc]),
                    'soc': value(self.model.storage_energy[h, loc]) / value(self.storage.storage_capacity)
                }
            
                solution['grid_purchases'][loc][h] = value(self.model.P_g[h, loc])
                
                solution['curtailment'][loc][h] = {
                    'solar': value(self.model.solar_curtailed[h, loc]),
                    'wind': value(self.model.wind_curtailed[h, loc])
                }
        
        return solution
        
    def print_solution_summary(self, solution: Dict):
        """Print a summary of the solution."""
        print("\n" + "="*50)
        print("SOLUTION SUMMARY")
        print("="*50)
        print(f"Status: {solution['status']}")
        print(f"Objective Value: ${solution['objective_value']:,.2f}")
        print(f"Selected Locations: {solution['selected_locations']}")
        print(f"\nStorage System: {self.storage.storage_type}")
        
        for loc in solution['selected_locations']:
            print(f"\nLocation {loc}:")
            total_solar = sum(solution['generation_dispatch'][loc][h]['solar_to_load'] 
                            for h in self.model.HOURS)
            total_wind = sum(solution['generation_dispatch'][loc][h]['wind_to_load'] 
                            for h in self.model.HOURS)
            total_grid = sum(solution['grid_purchases'][loc][h] 
                            for h in self.model.HOURS)
            total_storage_charged = sum(solution['storage_operation'][loc][h]['charge']
                                       for h in self.model.HOURS)
            total_storage_discharged = sum(solution['storage_operation'][loc][h]['discharge']
                                          for h in self.model.HOURS)
            
            print(f"  Total Solar to Load: {total_solar:.2f} MWh")
            print(f"  Total Wind to Load: {total_wind:.2f} MWh") 
            print(f"  Total Grid Purchases: {total_grid:.2f} MWh")
            print(f"  Total Storage Charged: {total_storage_charged:.2f} MWh")
            print(f"  Total Storage Discharged: {total_storage_discharged:.2f} MWh")
            if total_storage_charged > 0:
                print(f"  Storage Round-trip Energy: {total_storage_discharged/total_storage_charged*100:.1f}%")

            # Report metrics for each plant type
            if self.use_plants:
                for p in self.model.PLANTS:
                    total_plant = sum(solution['generation_dispatch'][loc][h][f'{p}_output'] 
                                    for h in self.model.HOURS)
                    total_startups = sum(solution['generation_dispatch'][loc][h][f'{p}_startup']
                                        for h in self.model.HOURS)
                    print(f"  Total {p.upper()} Output: {total_plant:.2f} MWh")
                    print(f"  Total {p.upper()} Startups: {int(total_startups)}")
        
# Function to run the complete optimization
def run_datacenter_optimization(model_dictionaries: Dict,
                            config: Dict,
                            cost_params: Dict,
                            trans_rating: Dict,
                            trans_cost: Dict,
                            solver_name: str = 'gurobi',
                            processor = None,
                            storage_system: Optional[Storage] = None,
                            plant_systems: Optional[Plant] = None,
                            **solver_options) -> Tuple[SitingModel, Dict]:
    """
    Run the complete data center optimization.
    
    Args:
        model_dictionaries: Processed data from EnergyDataProcessor
        config: Model configuration parameters
        cost_params: Cost parameter dictionaries
        solver_name: Solver to use
        processor: Data processor instance
        storage_system: Storage object (optional, defaults to lithium-ion)
        plant_systems: Dictionary of Plant objects {plant_type: Plant} (optional)
        **solver_options: Additional solver options
        
    Returns:
        Tuple of (model instance, solution dictionary)
    """
    # Create and build model
    opt_model = SitingModel(config, processor, storage_system, plant_systems)
    opt_model.load_data(model_dictionaries)
    opt_model.build_complete_model(cost_params, trans_rating, trans_cost)
    
    # Solve model
    solution = opt_model.solve(solver_name, **solver_options)
    
    # Print summary
    opt_model.print_solution_summary(solution)
    
    return opt_model, solution


# Example usage showing complete workflow
if __name__ == "__main__":
# This would typically be in a separate script that imports both modules
    from data_loader import process_data_pipeline

    # 1. Define file paths and cost parameters
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

    # 2. Process the data
    processor, model_dictionaries = process_data_pipeline(
        file_paths=file_paths,
        pue_climate_dict=pue_climate_region_same,
        wue_climate_dict=wue_climate_region_same,
        trans_mult_dict=trans_mult_dict,
        telecom_cost_dict=telecom_cost,
        min_capacity=200,
        state_filter='FL',
        max_water_risk=5.0,
        county_filter = None
    )

    # 5. Run the optimization
    opt_model, solution = run_datacenter_optimization(
        model_dictionaries=model_dictionaries,
        config=config,
        cost_params=cost_params,
        trans_rating = trans_rating,
        trans_cost = trans_cost,
        solver_name='gurobi',
        processor=processor,
        storage_system = StorageTemplates.create_lithium_ion("my_battery"),
        plant_systems = {
        'smr': PlantTemplates.create_smr_plant("my_smr", 250000)
        #'gas': PlantTemplates.create_gas_plant("gas1", 50000, 8760),
        }
    )
    
    # 6. The solution is now available
    print(f"\nOptimal location(s): {solution['selected_locations']}")
    print(f"Total cost: ${solution['objective_value']:,.2f}")

# Add values based on region/state
#https://stackoverflow.com/questions/51881503/assign-a-dictionary-value-to-a-dataframe-column-based-on-dictionary-key

### For dictionary d, and want to populate column "date" based on the "member"
#df["Date"] = df["Member"].apply(lambda x: d.get(x))
#df["Date"] = df["Member"].map(d)

import bisect


# capital cost for renewables ($/kW)
capital_gen_cost = {
    'solar': 1448, 
    'wind': 2098,
    'geo_35': 4479.83, 
    'geo_45': 4638.36,
    'geo_55': 4638.36,
    'geo_65': 2756.22,
    'nuclear': 7406,
    'smr': 7590,
    'gas': 0.00
}
capital_gen_cost = {k: v * 1000 for k, v in capital_gen_cost.items()} # ($/ MW)

# fixed cost ($/kW-yr)
fixed_gen_cost = {
    'solar': 17.16, 
    'wind': 26.94,
    'geo_35': 126, 
    'geo_45': 119,
    'geo_55': 119,
    'geo_65': 99,
    'nuclear': 136.91,
    'smr': 106.92,
    'gas': 0.00
}
fixed_gen_cost = {k: v * 1000 for k, v in fixed_gen_cost.items()} # ($/ MW)

# Variable cost for renewables (2022$/MWh)
variable_gen_cost = {
    'solar': 0, 
    'wind': 0,
    'geo_35': 1.31, 
    'geo_45': 1.31,
    'geo_55': 1.31,
    'geo_65': 1.31,
    'nuclear': 2.67,
    'smr': 3.38,
    'gas': 0.00
}
#variable_gen_cost = {k: v / 1000 for k, v in variable_gen_cost.items()} # ($/kWh)

# Lifetime of generation
lifetime_gen_yrs = {
    'solar': 25, 
    'wind': 25,
    'geo_35': 25, 
    'geo_45': 25,
    'geo_55': 25,
    'geo_65': 25,
    'nuclear': 60,
    'smr': 60,
    'gas': 0.00
}

# For ranges of values for keys
class RangeDict:
    def __init__(self, breaks, values):
        self.breaks = breaks
        self.values = values

    def items(self):
        return zip(self.breaks, self.values)

    def __getitem__(self, key):
        idx = bisect.bisect_left(self.breaks, key)
        if idx >= len(self.values):
            return self.values[-1]  # handles keys beyond last break
        return self.values[idx]    # handles keys within break range
    
    #return self.values[idx]
     #   idx = bisect.bisect_left(self.breaks, key)
      #  return self.values[idx] if idx < len(self.values) else None

# Example use 
#ranges = RangeDict([10, 20, 40], ["low", "medium", "high", "very high"])
#print(ranges[5])    # "low"

# Substation cpacity (kV) based on Capacity of datacenter (MW) needed to transmit (MW: kV)
trans_capacity = RangeDict([15, 50, 145, 425, 1075, 2300], [69, 138, 161, 230, 500, 765]) 
#print(trans_cap[5])  

trans_rating = {
    15: 69,
    50: 138,
    145: 161,
    425: 230,
    1075: 500,
    2300: 765
}

# Create Dictionary for transmission cost per line (USD 2017 /mile)
trans_cost = {
    69: 1696200 , 
    138: 1642200,
    161: 1536400,
    230: 2150300,
    500: 3071750,
    765: 3071750 # approximate
    }

#trans_cost = {k: v / 1000 for k, v in trans_cost.items()}

#transmission location multiplier
trans_mult_dict = {}
trans_mult_dict = dict.fromkeys(['SERC', 'ERCOT', 'SERC_C', 'SERC_SE', 'SERC_E', 'SERC_F', ], 1)
trans_mult_dict.update(dict.fromkeys(['MISO', 'SPP'], 1.4695))
trans_mult_dict.update(dict.fromkeys(['CAISO', 'NYISO', 'ISONE', 'PJM', 'NPCC', 'NPCC_NY', 'NPCC_NE', 'WECC_NW', 'WECC_CA', 'WECC_SW', 'nrn5'], 2.1179))
trans_mult_dict.update(dict.fromkeys(['TEPPC'], 1.1992))
#print(trans_mult_dict)

              
# Create Dictionary for Substation upgrade line 
# based on MISO: https://cdn.misoenergy.org/Transmission-and-Substation-Project-Cost-Estimation-Guide-for-MTEP-2018144804.pdf 
# voltage class (kV) : Cost (millions)
subs_upgrade_cost = {
    69: 1.6,
    115: 1.9,
    138: 2.3,
    161: 2.6,
    230: 3.1,
    345: 5.4,
    500: 8.8
}

subs_upgrade_cost = {k: v * 10**6 for k, v in subs_upgrade_cost.items()}

subs_new_cost = {
    0: 0, 
    69: 5.7,
    115: 6.8,
    138: 7.8,
    161: 8.9,
    230: 10.8,
    345: 18.7,
    500: 31.4
}

subs_new_cost = {k: v * 10**6 for k, v in subs_new_cost.items()}

# TELECOM cost {distance (miles) : cost per mile}
telecom_cost = RangeDict([5, 20, 50], [122760, 77035.2, 77246.4, 66000]) # where are these numbers from
#telecom_cost = RangeDict([5, 20, 50], [23.25, 14.59, 14.63, 12.5]) # in feet

#telecom_cost.values = [v / 1000 for v in telecom_cost.values] # use this method for the rangedicts

cost_params = {
    'capital_gen_cost': capital_gen_cost,
    'variable_gen_cost': variable_gen_cost,
    'fixed_gen_cost': fixed_gen_cost,
    'subs_new_cost': subs_new_cost,
    'susb_upgrade_cost': subs_upgrade_cost,
    'trans_cap_amount': None, 
}


# Water price region dict
water_price_region_dict = {}
water_price_region_dict = dict.fromkeys(['WA', 'OR', 'CA', 'HI', 'AK' ], "West-Pacific")
water_price_region_dict.update(dict.fromkeys(['MT', 'ID', 'WY', 'CO', 'UT', 'NV', 'AZ', 'NM'], 'West-Mountain'))
water_price_region_dict.update(dict.fromkeys(['ND', 'SD', 'MN', 'IA', 'NE', 'MO', 'KS'], 'Midwest-West Central North'))
water_price_region_dict.update(dict.fromkeys(['WI', 'MI', 'OH', 'IN', 'IL'], 'Midwest-East Central North'))
water_price_region_dict.update(dict.fromkeys(['PA', 'NJ', 'NY', 'VT', 'NH', 'ME', 'MA', 'RI', 'CT'], 'Northeast'))
water_price_region_dict.update(dict.fromkeys(['MD', 'VA', 'WV', 'NC', 'SC' 'KY', 'TN', 'GA', 'FL', 'AL', 'MS'], 'South-South East'))
water_price_region_dict.update(dict.fromkeys(['AR', 'OK', 'LA', 'TX'], 'South-West South Central'))
#print(water_price_region_dict)


# PUE per climate area (95th percentile - inefficient data center) (Kwh/Kwh) 
# 0 - hot, A humid, B dry, C marine
pue_climate_region_95 = {
    '0A': 1.45, 
    '0B': 1.24,
    '1A': 1.26,
    '1B': 1.28,
    '2A': 1.29,
    '2B': 1.21,
    '3A': 1.22,
    '3B': 1.21,
    '3C': 1.2,
    '4A': 1.2,
    '4B': 1.19,
    '4C': 1.18,
    '5A': 1.14,
    '5B': 1.18,
    '5C': 1.18,
    '6A': 1.19,
    '6B': 1.18,
    '7' : 1.19,
    '8' : 1.18
}

pue_climate_region_5 = {
    '0A': 1.2,  
    '0B': 1.125,
    '1A': 1.2,
    '1B': 1.14,
    '2A': 1.13,
    '2B': 1.105,
    '3A': 1.09,
    '3B': 1.065,
    '3C': 1.075,
    '4A': 1.085,
    '4B': 1.05,
    '4C': 1.065,
    '5A': 1.06,
    '5B': 1.075,
    '5C': 1.06,
    '6A': 1.075,
    '6B': 1.075,
    '7' : 1.075,
    '8' : 1.07
}

pue_climate_region_same = {
    '0A': 1.08, 
    '0B': 1.08,
    '1A': 1.08,
    '1B': 1.08,
    '2A': 1.08,
    '2B': 1.08,
    '3A': 1.08,
    '3B': 1.08,
    '3C': 1.08,
    '4A': 1.08,
    '4B': 1.08,
    '4C': 1.08,
    '5A': 1.08,
    '5B': 1.08,
    '5C': 1.08,
    '6A': 1.08,
    '6B': 1.08,
    '7' : 1.08,
    '8' : 1.08
}

# WUE per climate area (L/KWh)
wue_climate_region_95 = {
    '0A': 3.0,  # none so made it same as 1A
    '0B': 1.35,
    '1A': 2.1,
    '1B': 1.65,
    '2A': 2.0,
    '2B': 0.95,
    '3A': 1.15,
    '3B': 0.75,
    '3C': 0.8,
    '4A': 1.1,
    '4B': 0.4,
    '4C': 0.6,
    '5A': 0.7,
    '5B': 0.35,
    '5C': 0.45,
    '6A': 0.65,
    '6B': 0.5,
    '7' : 0.6,
    '8' : 0.25
}

wue_climate_region_5 = {
    '0A': 1.1,  
    '0B': 0.7,
    '1A': 0.75,
    '1B': 0.85,
    '2A': 0.65,
    '2B': 0.4,
    '3A': 0.4,
    '3B': 0.15,
    '3C': 0.05,
    '4A': 0.3,
    '4B': 0.15,
    '4C': 0.0,
    '5A': 0.15,
    '5B': 0.05,
    '5C': 0.0,
    '6A': 0.2,
    '6B': 0.05,
    '7' : 0.15,
    '8' : 0.0
}

wue_climate_region_same = {
    '0A': 1.08, 
    '0B': 1.08,
    '1A': 1.08,
    '1B': 1.08,
    '2A': 1.08,
    '2B': 1.08,
    '3A': 1.08,
    '3B': 1.08,
    '3C': 1.08,
    '4A': 1.08,
    '4B': 1.08,
    '4C': 1.08,
    '5A': 1.08,
    '5B': 1.08,
    '5C': 1.08,
    '6A': 1.08,
    '6B': 1.08,
    '7' : 1.08,
    '8' : 1.08
}


#wue_climate_region_95 = {k: v * 10**6 for k, v in wue_climate_region_95.items()} # for (L/MWh)
#wue_climate_region_5 = {k: v * 10**6 for k, v in wue_climate_region_5.items()} # for (L/MWh) # do i need gallons actually for the price
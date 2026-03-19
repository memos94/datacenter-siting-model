import pyomo.environ as pyo

# 1. Crear un modelo mini (2x + 3y -> min, s.t. x+y >= 1)
model = pyo.ConcreteModel()
model.x = pyo.Var(within=pyo.NonNegativeReals)
model.y = pyo.Var(within=pyo.NonNegativeReals)
model.obj = pyo.Objective(expr= 2*model.x + 3*model.y)
model.con = pyo.Constraint(expr= model.x + model.y >= 1)

# 2. Intentar llamar a CBC
try:
    solver = pyo.SolverFactory('cbc')
    results = solver.solve(model)
    
    if results.solver.termination_condition == pyo.TerminationCondition.optimal:
        print("✅ ¡ÉXITO! Pyomo encontró CBC y resolvió el modelo.")
        print(f"Resultado: x={pyo.value(model.x)}, y={pyo.value(model.y)}")
    else:
        print("❓ El solver respondió, pero no encontró una solución óptima.")
except Exception as e:
    print("❌ ERROR: Pyomo no pudo ejecutar CBC.")
    print(f"Detalle: {e}")
from pyomo.environ import SolverFactory
print(SolverFactory('scip').available())

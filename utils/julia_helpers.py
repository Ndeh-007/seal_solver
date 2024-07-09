from .julia_import import JULIA

# define application Modules from julia

JULIA.seval("using SealSolver")
JULIA.seval("using PythonCall: PythonCall")

# prepare them to be used by python
SealSolver = JULIA.SealSolver
PythonCall = JULIA.PythonCall

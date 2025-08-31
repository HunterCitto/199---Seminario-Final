#!/usr/bin/env python3
"""
Script para verificar dependencias necesarias
"""
import importlib
import sys

def check_dependency(module_name, package_name=None):
    """Verifica si una dependencia está instalada"""
    try:
        if package_name is None:
            package_name = module_name
        importlib.import_module(module_name)
        print(f"✓ {package_name} está instalado")
        return True
    except ImportError:
        print(f"✗ {package_name} NO está instalado")
        return False

# Lista de dependencias críticas
dependencies = [
    ('xarray', 'xarray'),
    ('netCDF4', 'netCDF4'),
    ('h5netcdf', 'h5netcdf'),
    ('cdsapi', 'cdsapi'),
    ('geopandas', 'geopandas'),
    ('ee', 'earthengine-api')
]

print("Verificando dependencias...")
all_ok = True

for module, package in dependencies:
    if not check_dependency(module, package):
        all_ok = False

if not all_ok:
    print("\nAlgunas dependencias faltan. Ejecuta:")
    print("pip install netCDF4 h5netcdf xarray cdsapi geopandas earthengine-api")
else:
    print("\n✓ Todas las dependencias están instaladas correctamente")
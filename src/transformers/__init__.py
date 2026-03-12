import pkgutil
import importlib
from pathlib import Path

pkg_path = str(Path(__file__).parent)
pkg_name = __package__

# To wypisze nam w logach, czy transformery w ogóle się ruszają
print(f"DEBUG: Skanuję folder {pkg_path} dla paczki {pkg_name}")

for _, module_name, _ in pkgutil.iter_modules([pkg_path]):
    if module_name != "base":
        print(f"DEBUG: Próbuję załadować transformer: {module_name}")
        importlib.import_module(f".{module_name}", package=pkg_name)
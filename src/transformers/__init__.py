import pkgutil
import importlib
from pathlib import Path

pkg_path = str(Path(__file__).parent)
pkg_name = __package__

for _, module_name, _ in pkgutil.iter_modules([pkg_path]):
    if module_name != "base":
        importlib.import_module(f".{module_name}", package=pkg_name)

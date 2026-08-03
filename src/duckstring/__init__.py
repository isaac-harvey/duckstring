from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from .core import (
    Catchment,
    Pond,
    Puddle,
    puddle,
    ripple,
)

try:
    __version__ = _pkg_version("duckstring")
except PackageNotFoundError:  # running from a source tree without an installed dist
    __version__ = "0.0.0"

__all__ = [
    "Catchment",
    "Pond",
    "Puddle",
    "puddle",
    "ripple",
    "__version__",
]

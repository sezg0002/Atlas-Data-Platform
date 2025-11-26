"""
Import helper module.
Centralizes path management and config imports for all components.
"""

import sys
import os

# Setup paths
_current_dir = os. path.dirname(os.path.abspath(__file__))
_dashboard_dir = os. path.dirname(_current_dir)
_project_dir = os.path. dirname(_dashboard_dir)

for _path in [_dashboard_dir, _project_dir]:
    if _path not in sys. path:
        sys.path.insert(0, _path)

# Import and re-export config values
from ..config_dash import (
    COLORS,
    CHART_CONFIG,
    PAGE_CONFIG,
    DEFAULTS,
    DOMAIN_LABELS,
)

# Import database functions
from ..database import (
    get_engine,
    check_connection,
    load_domain_data,
    get_available_countries,
    get_data_summary,
    clear_cache,
)

# Import formatters
from ..utils.formatters import (
    format_number,
    format_percentage,
    format_date,
    format_currency,
)

__all__ = [
    # Config
    'COLORS',
    'CHART_CONFIG',
    'PAGE_CONFIG',
    'DEFAULTS',
    'DOMAIN_LABELS',
    # Database
    'get_engine',
    'check_connection',
    'load_domain_data',
    'get_available_countries',
    'get_data_summary',
    'clear_cache',
    # Formatters
    'format_number',
    'format_percentage',
    'format_date',
    'format_currency',
]
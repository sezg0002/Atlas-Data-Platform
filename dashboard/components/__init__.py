"""Dashboard components."""

from .header import render_header
from .sidebar import render_sidebar
from .kpis import render_kpis
from .charts import render_historical_chart, render_comparison_chart
from . statistics import render_statistics
from .forecast import render_forecast_section
from .footer import render_footer

# Export config for convenience
import sys
import os
sys.path.insert(0, os. path.dirname(os.path. dirname(os.path. abspath(__file__))))
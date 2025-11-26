"""Dashboard components."""

from .header import render_header, render_connection_error, render_no_data_warning
from .sidebar import render_sidebar
from .kpis import render_kpis
from .charts import render_historical_chart, render_yoy_growth_chart
from .statistics import render_statistics
from .forecast import render_forecast_section
from .footer import render_footer
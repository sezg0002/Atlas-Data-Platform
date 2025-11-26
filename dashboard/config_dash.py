"""Configuration and constants for the dashboard."""

PAGE_CONFIG = {
    "page_title": "Atlas Data Platform",
    "page_icon": "🌍",
    "layout": "wide",
}

COLORS = {
    "primary": "#2E86DE",
    "secondary": "#5D6D7E",
    "success": "#1E8449",
    "warning": "#F39C12",
    "danger": "#E74C3C",
    "dark": "#2C3E50",
    "light": "#F9FBFD",
    "muted": "#95A5A6",
}

CHART_CONFIG = {
    "plot_bgcolor": COLORS["light"],
    "paper_bgcolor": COLORS["light"],
    "font_color": COLORS["dark"],
}

DEFAULTS = {
    "countries": ["FRA", "USA", "DEU"],
    "domains": ["economy", "finance"],
    "cache_ttl": 3600,
    "forecast_periods": 5,
}

DOMAIN_LABELS = {
    "economy": "Économie",
    "finance": "Finance",
}
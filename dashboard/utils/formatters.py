"""Formatting utilities."""

from datetime import datetime
from typing import Union
import pandas as pd


def format_number(value: Union[int, float], decimals: int = 0) -> str:
    """Format number with thousand separators."""
    if pd.isna(value):
        return "N/A"
    return f"{value:,. {decimals}f}"


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format percentage with sign."""
    if pd. isna(value):
        return "N/A"
    return f"{value:+.{decimals}f}%"


def format_date(date: Union[str, datetime, pd.Timestamp], fmt: str = "%Y-%m-%d") -> str:
    """Format date to string."""
    if pd.isna(date):
        return "N/A"
    if isinstance(date, str):
        return date
    return date. strftime(fmt)


def format_currency(value: float, currency: str = "USD", decimals: int = 0) -> str:
    """Format value as currency."""
    if pd. isna(value):
        return "N/A"
    symbols = {"USD": "$", "EUR": "€", "GBP": "£"}
    symbol = symbols.get(currency, currency)
    return f"{symbol}{value:,.{decimals}f}"
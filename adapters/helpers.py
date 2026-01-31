"""
Shared helper functions for the framework.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

def calculate_completeness(df: pd.DataFrame) -> float:
    """Calculate data completeness percentage."""
    total_cells = df.size
    non_null_cells = df.count().sum()
    return (non_null_cells / total_cells) * 100 if total_cells > 0 else 0

def calculate_uniqueness(df: pd.DataFrame, id_column: str = 'customer_id') -> float:
    """Calculate uniqueness percentage for identifier column."""
    if id_column not in df.columns:
        return 0.0
    total = len(df)
    unique = df[id_column].nunique()
    return (unique / total) * 100 if total > 0 else 0

def detect_outliers(series: pd.Series, method: str = 'iqr') -> pd.Series:
    """
    Detect outliers in a series.
    
    Args:
        series: Pandas Series
        method: 'iqr' for interquartile range, 'zscore' for z-score
    
    Returns:
        Boolean series indicating outliers
    """
    if method == 'iqr':
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return (series < lower_bound) | (series > upper_bound)
    
    elif method == 'zscore':
        mean = series.mean()
        std = series.std()
        if std == 0:
            return pd.Series(False, index=series.index)
        z_scores = (series - mean) / std
        return abs(z_scores) > 3
    
    return pd.Series(False, index=series.index)

def format_phone_number(phone: str) -> Optional[str]:
    """Format phone number to standard format."""
    if pd.isna(phone):
        return None
    
    phone_str = str(phone)
    # Remove all non-digit characters
    digits = ''.join(filter(str.isdigit, phone_str))
    
    if len(digits) == 9:
        return f"+254{digits}"
    elif len(digits) == 12 and digits.startswith('254'):
        return f"+{digits}"
    elif len(digits) == 10 and digits.startswith('0'):
        return f"+254{digits[1:]}"
    else:
        return None

def generate_timestamp() -> str:
    """Generate a timestamp string for filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def summarize_dataframe(df: pd.DataFrame) -> dict:
    """Generate summary statistics for dataframe."""
    summary = {
        'shape': df.shape,
        'columns': list(df.columns),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
        'missing_values': df.isnull().sum().to_dict(),
        'memory_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
    }
    
    # Add numeric column statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        summary[f'{col}_stats'] = {
            'mean': df[col].mean(),
            'median': df[col].median(),
            'std': df[col].std(),
            'min': df[col].min(),
            'max': df[col].max()
        }
    
    return summary
"""
Shared validation functions.
"""

import re
import pandas as pd
from typing import Optional, List

def validate_email_pattern(email: str) -> bool:
    """Validate email pattern using regex."""
    if pd.isna(email) or not isinstance(email, str):
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_phone_pattern(phone: str) -> bool:
    """Validate phone number pattern."""
    if pd.isna(phone) or not isinstance(phone, str):
        return False
    
    # Accept multiple formats
    patterns = [
        r'^\+254[0-9]{9}$',  # +254XXXXXXXXX
        r'^254[0-9]{9}$',     # 254XXXXXXXXX
        r'^0[0-9]{9}$',       # 0XXXXXXXXX
        r'^07[0-9]{8}$',      # 07XXXXXXXX
    ]
    
    return any(re.match(pattern, phone) for pattern in patterns)

def validate_date_format(date_str: str, format: str = "%Y-%m-%d") -> bool:
    """Validate date format."""
    if pd.isna(date_str):
        return False
    
    try:
        if isinstance(date_str, str):
            pd.to_datetime(date_str, format=format)
            return True
        return True  # Already a datetime object
    except (ValueError, TypeError):
        return False

def validate_numeric_range(value: float, min_val: float, max_val: float) -> bool:
    """Validate numeric value is within range."""
    if pd.isna(value):
        return False
    return min_val <= value <= max_val

def validate_in_list(value: str, valid_list: List[str]) -> bool:
    """Validate value is in list of valid values."""
    if pd.isna(value):
        return False
    return value in valid_list

def validate_string_length(value: str, min_len: int = 1, max_len: int = 255) -> bool:
    """Validate string length."""
    if pd.isna(value):
        return False
    return min_len <= len(str(value)) <= max_len

def validate_no_special_chars(value: str, allowed_chars: str = " _-") -> bool:
    """
    Validate no special characters in string.
    
    Args:
        value: String to validate
        allowed_chars: Special characters that are allowed
    
    Returns:
        True if valid
    """
    if pd.isna(value):
        return False
    
    value_str = str(value)
    pattern = f'^[a-zA-Z0-9{re.escape(allowed_chars)}]+$'
    return bool(re.match(pattern, value_str))

def validate_percentage(value: float) -> bool:
    """Validate percentage value (0-100)."""
    if pd.isna(value):
        return False
    return 0 <= value <= 100
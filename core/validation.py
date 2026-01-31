"""
Data validation module for quality checks.
"""

import re
import pandas as pd
from typing import Dict, List, Callable

class ValidationEngine:
    """Comprehensive validation engine."""
    
    def __init__(self):
        self.validation_rules = {}
    
    def add_validation_rule(self, rule_name: str, column: str, 
                           rule_func: Callable, error_msg: str):
        """Add a validation rule."""
        self.validation_rules[rule_name] = {
            'column': column,
            'function': rule_func,
            'error_message': error_msg
        }
    
    def validate_email(self, email: str) -> bool:
        """Validate email format."""
        if pd.isna(email):
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))
    
    def validate_phone(self, phone: str) -> bool:
        """Validate phone number format."""
        if pd.isna(phone):
            return False
        pattern = r'^\+254[0-9]{9}$'
        return bool(re.match(pattern, str(phone)))
    
    def validate_age(self, age: float) -> bool:
        """Validate age range."""
        if pd.isna(age):
            return False
        return 0 < age < 120
    
    def validate_amount(self, amount: float) -> bool:
        """Validate amount is positive."""
        if pd.isna(amount):
            return False
        return amount > 0
    
    def run_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run all validation rules on dataframe."""
        df_validated = df.copy()
        
        for rule_name, rule in self.validation_rules.items():
            col = rule['column']
            if col in df_validated.columns:
                validation_col = f'{col}_valid'
                df_validated[validation_col] = df_validated[col].apply(rule['function'])
                
                invalid_count = (~df_validated[validation_col]).sum()
                if invalid_count > 0:
                    print(f"{rule_name}: {invalid_count} invalid records")
        
        return df_validated
    
    def get_validation_summary(self, df_validated: pd.DataFrame) -> Dict:
        """Generate validation summary report."""
        summary = {'total_records': len(df_validated), 'fields': {}}
        
        validation_cols = [col for col in df_validated.columns if col.endswith('_valid')]
        
        for col in validation_cols:
            field_name = col.replace('_valid', '')
            valid_count = df_validated[col].sum()
            invalid_count = len(df_validated) - valid_count
            
            summary['fields'][field_name] = {
                'valid': int(valid_count),
                'invalid': int(invalid_count),
                'validity_rate': round((valid_count / len(df_validated)) * 100, 2)
            }
        
        # Calculate overall validity
        if validation_cols:
            all_valid = df_validated[validation_cols].all(axis=1).sum()
            summary['overall_validity'] = round((all_valid / len(df_validated)) * 100, 2)
        
        return summary
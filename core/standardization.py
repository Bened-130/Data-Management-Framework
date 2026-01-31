"""
Data standardization module for consistent formatting.
"""

import pandas as pd
import re

class StandardizationManager:
    """Manage data standardization across systems."""
    
    def __init__(self):
        self.standards = {}
    
    def set_naming_convention(self, convention: str = 'snake_case'):
        """Set column naming convention."""
        self.standards['naming_convention'] = convention
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names based on naming convention."""
        df_clean = df.copy()
        
        convention = self.standards.get('naming_convention', 'snake_case')
        
        if convention == 'snake_case':
            df_clean.columns = [
                col.lower().replace(' ', '_').replace('-', '_')
                for col in df_clean.columns
            ]
        
        print(f"Standardized column names to {convention}")
        return df_clean
    
    def create_data_dictionary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create data dictionary for documentation."""
        data_dict = []
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            null = df[col].isna().sum()
            unique = df[col].nunique()
            
            # Sample values
            samples = df[col].dropna().unique()[:3]
            sample_str = ', '.join([str(s)[:30] for s in samples])
            
            data_dict.append({
                'Column Name': col,
                'Data Type': dtype,
                'Non-Null Count': non_null,
                'Null Count': null,
                'Unique Values': unique,
                'Sample Values': sample_str
            })
        
        dict_df = pd.DataFrame(data_dict)
        print(f"Created data dictionary with {len(dict_df)} columns")
        
        return dict_df
"""
Data cleansing module for cleaning operations.
"""

import pandas as pd
import numpy as np
import re

class DataCleanser:
    """Comprehensive data cleansing operations."""
    
    def __init__(self):
        self.cleaning_log = []
    
    def remove_duplicates(self, df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
        """Remove duplicate records."""
        initial_count = len(df)
        df_clean = df.drop_duplicates(subset=subset)
        removed = initial_count - len(df_clean)
        
        if removed > 0:
            print(f"Removed {removed} duplicate records")
            self.cleaning_log.append(f"Duplicates removed: {removed}")
        
        return df_clean
    
    def standardize_text(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Standardize text fields."""
        df_clean = df.copy()
        
        for col in columns:
            if col in df_clean.columns:
                # Strip whitespace
                df_clean[col] = df_clean[col].astype(str).str.strip()
                
                # Apply proper casing
                if 'name' in col.lower():
                    df_clean[col] = df_clean[col].str.title()
                elif 'status' in col.lower():
                    df_clean[col] = df_clean[col].str.capitalize()
                elif 'email' in col.lower():
                    df_clean[col] = df_clean[col].str.lower()
        
        print(f"Standardized text in {len(columns)} columns")
        return df_clean
    
    def fix_email_format(self, df: pd.DataFrame, email_col: str = 'email') -> pd.DataFrame:
        """Fix common email format issues."""
        df_clean = df.copy()
        
        if email_col in df_clean.columns:
            # Remove spaces and convert to lowercase
            df_clean[email_col] = df_clean[email_col].str.replace(' ', '').str.lower()
            
            # Add @ if missing
            mask = ~df_clean[email_col].str.contains('@') & df_clean[email_col].notna()
            df_clean.loc[mask, email_col] = df_clean.loc[mask, email_col] + '@fixed.com'
        
        print(f"Fixed email format in {email_col}")
        return df_clean
    
    def fix_phone_format(self, df: pd.DataFrame, phone_col: str = 'phone') -> pd.DataFrame:
        """Fix phone number format."""
        df_clean = df.copy()
        
        if phone_col in df_clean.columns:
            # Remove non-numeric characters except +
            df_clean[phone_col] = df_clean[phone_col].astype(str).str.replace(r'[^0-9+]', '', regex=True)
            
            # Add country code if missing
            mask = ~df_clean[phone_col].str.startswith('+254')
            df_clean.loc[mask, phone_col] = '+254' + df_clean.loc[mask, phone_col].str[-9:]
        
        print(f"Fixed phone format in {phone_col}")
        return df_clean
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values intelligently."""
        df_clean = df.copy()
        
        for col in df_clean.columns:
            missing_count = df_clean[col].isnull().sum()
            if missing_count > 0:
                # Numeric columns: fill with median
                if pd.api.types.is_numeric_dtype(df_clean[col]):
                    df_clean[col].fillna(df_clean[col].median(), inplace=True)
                # Categorical columns: fill with mode
                else:
                    mode_value = df_clean[col].mode()[0] if len(df_clean[col].mode()) > 0 else 'Unknown'
                    df_clean[col].fillna(mode_value, inplace=True)
        
        print("Handled missing values")
        return df_clean
    
    def remove_outliers(self, df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
        """Remove outliers using IQR method."""
        df_clean = df.copy()
        
        if columns is None:
            columns = df_clean.select_dtypes(include=[np.number]).columns
        
        for col in columns:
            if col in df_clean.columns:
                Q1 = df_clean[col].quantile(0.25)
                Q3 = df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                initial_count = len(df_clean)
                df_clean = df_clean[
                    (df_clean[col] >= lower_bound) & 
                    (df_clean[col] <= upper_bound)
                ]
                removed = initial_count - len(df_clean)
                
                if removed > 0:
                    print(f"Removed {removed} outliers from {col}")
        
        return df_clean
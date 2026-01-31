"""
Data ingestion module for loading and generating sample data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class DataIngestionManager:
    """Manage data ingestion from multiple sources."""
    
    def __init__(self):
        self.ingestion_logs = []
    
    def generate_sample_records(self, n_records: int = 10000) -> pd.DataFrame:
        """Generate sample customer records with intentional data quality issues."""
        np.random.seed(42)
        
        customer_ids = [f"CUST{str(i).zfill(6)}" for i in range(1, n_records + 1)]
        first_names = ['John', 'Jane', 'Bob', 'Alice', 'Mike', 'Sarah', 'David', 'Emma']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        
        data = []
        for i, cust_id in enumerate(customer_ids):
            # Introduce data quality issues in 10% of records
            first_name = np.random.choice(first_names)
            if np.random.random() < 0.1:
                first_name = first_name.lower()  # Case issue
            
            last_name = np.random.choice(last_names)
            
            # Email with issues
            email = f"{first_name.lower()}.{last_name.lower()}@email.com"
            if np.random.random() < 0.05:
                email = email.replace('@', '')
            
            # Phone with issues
            phone = f"+254{np.random.randint(700000000, 799999999)}"
            if np.random.random() < 0.03:
                phone = phone.replace('+', '')
            
            # Age with outliers
            age = np.random.randint(18, 80)
            if np.random.random() < 0.02:
                age = np.random.choice([-1, 0, 150])
            
            # Amount with negative values
            amount = round(np.random.uniform(100, 10000), 2)
            if np.random.random() < 0.02:
                amount = -amount
            
            data.append({
                'customer_id': cust_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'age': age,
                'amount': amount,
                'registration_date': datetime.now() - timedelta(days=np.random.randint(0, 365)),
                'status': np.random.choice(['Active', 'Inactive', 'Pending']),
                'city': np.random.choice(['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru'])
            })
        
        df = pd.DataFrame(data)
        
        # Add duplicates (2%)
        n_duplicates = int(len(df) * 0.02)
        duplicate_indices = np.random.choice(len(df), n_duplicates, replace=False)
        df_duplicates = df.iloc[duplicate_indices].copy()
        df = pd.concat([df, df_duplicates], ignore_index=True)
        
        # Add missing values (3%)
        for col in df.columns:
            if col != 'customer_id':
                missing_indices = np.random.choice(
                    len(df), 
                    int(len(df) * 0.03), 
                    replace=False
                )
                df.loc[missing_indices, col] = np.nan
        
        print(f"Generated {len(df)} sample records")
        return df
    
    def load_from_csv(self, filepath: str) -> pd.DataFrame:
        """Load data from CSV file."""
        try:
            df = pd.read_csv(filepath)
            print(f"Loaded {len(df)} records from {filepath}")
            return df
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return pd.DataFrame()
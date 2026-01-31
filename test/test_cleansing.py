"""
Tests for the data cleansing module.
"""

import pytest
import pandas as pd
import numpy as np
from core.cleansing import DataCleanser


class TestDataCleanser:
    
    @pytest.fixture
    def data_cleanser(self):
        return DataCleanser()
    
    @pytest.fixture
    def sample_data_with_issues(self):
        """Create sample data with various issues."""
        return pd.DataFrame({
            'customer_id': ['C001', 'C001', 'C002', 'C003', 'C004'],
            'first_name': ['  john  ', 'JANE', 'bob', None, 'alice'],
            'last_name': ['SMITH', 'doe', 'WILLIAMS', 'brown', None],
            'email': ['TEST@EMAIL.COM', 'jane.doe@email.com', 'bad-email', None, 'alice.smith@email.com'],
            'phone': ['+254712345678', '0712345678', '+254700000000', None, '254711223344'],
            'age': [25, 30, -5, 150, 35],
            'amount': [100.50, -50, 200, 300.75, 0],
            'status': ['active', 'INACTIVE', 'pending', 'active', None]
        })
    
    def test_remove_duplicates(self, data_cleanser, sample_data_with_issues):
        """Test duplicate removal."""
        # Check initial duplicates
        initial_duplicates = sample_data_with_issues.duplicated(subset=['customer_id']).sum()
        assert initial_duplicates > 0
        
        # Remove duplicates
        cleaned_data = data_cleanser.remove_duplicates(
            sample_data_with_issues, subset=['customer_id']
        )
        
        # Check duplicates removed
        final_duplicates = cleaned_data.duplicated(subset=['customer_id']).sum()
        assert final_duplicates == 0
        assert len(cleaned_data) < len(sample_data_with_issues)
    
    def test_standardize_text(self, data_cleanser, sample_data_with_issues):
        """Test text standardization."""
        cleaned_data = data_cleanser.standardize_text(
            sample_data_with_issues,
            ['first_name', 'last_name', 'status']
        )
        
        # Check whitespace removed
        assert cleaned_data['first_name'].iloc[0] == 'John'  # Was '  john  '
        
        # Check proper casing
        assert cleaned_data['first_name'].iloc[1] == 'Jane'  # Was 'JANE'
        assert cleaned_data['first_name'].iloc[2] == 'Bob'  # Was 'bob'
        
        # Check status capitalization
        assert cleaned_data['status'].iloc[0] == 'Active'  # Was 'active'
        assert cleaned_data['status'].iloc[1] == 'Inactive'  # Was 'INACTIVE'
        
        # Check None values remain None
        assert pd.isna(cleaned_data['first_name'].iloc[3])
        assert pd.isna(cleaned_data['status'].iloc[4])
    
    def test_fix_email_format(self, data_cleanser, sample_data_with_issues):
        """Test email format fixing."""
        cleaned_data = data_cleanser.fix_email_format(sample_data_with_issues)
        
        # Check emails are lowercase
        assert cleaned_data['email'].iloc[0] == 'test@email.com'  # Was 'TEST@EMAIL.COM'
        
        # Check valid emails unchanged
        assert cleaned_data['email'].iloc[1] == 'jane.doe@email.com'
        
        # Check invalid email fixed
        assert cleaned_data['email'].iloc[2] == 'bad-email@fixed.com'  # Was 'bad-email'
        
        # Check None remains None
        assert pd.isna(cleaned_data['email'].iloc[3])
    
    def test_fix_phone_format(self, data_cleanser, sample_data_with_issues):
        """Test phone format fixing."""
        cleaned_data = data_cleanser.fix_phone_format(sample_data_with_issues)
        
        # Check valid phone unchanged
        assert cleaned_data['phone'].iloc[0] == '+254712345678'
        
        # Check phone without country code fixed
        assert cleaned_data['phone'].iloc[1] == '+254712345678'  # Was '0712345678'
        
        # Check phone with 254 but no + fixed
        assert cleaned_data['phone'].iloc[4] == '+254711223344'  # Was '254711223344'
        
        # Check None remains None
        assert pd.isna(cleaned_data['phone'].iloc[3])
    
    def test_handle_missing_values(self, data_cleanser):
        """Test missing value handling."""
        data_with_missing = pd.DataFrame({
            'numeric_col': [1, 2, None, 4, None],
            'text_col': ['A', None, 'C', None, 'E'],
            'all_missing': [None, None, None, None, None]
        })
        
        cleaned_data = data_cleanser.handle_missing_values(data_with_missing)
        
        # Check numeric missing filled with median
        assert cleaned_data['numeric_col'].isnull().sum() == 0
        assert cleaned_data['numeric_col'].iloc[2] == 2.5  # Median of [1, 2, 4] = 2.5
        
        # Check text missing filled with mode
        assert cleaned_data['text_col'].isnull().sum() == 0
        # Mode of ['A', 'C', 'E'] is 'A' (first occurrence)
        assert cleaned_data['text_col'].iloc[1] == 'A'
        assert cleaned_data['text_col'].iloc[3] == 'A'
        
        # Check all missing column filled with 'Unknown'
        assert cleaned_data['all_missing'].isnull().sum() == 0
        assert cleaned_data['all_missing'].iloc[0] == 'Unknown'
    
    def test_remove_outliers(self, data_cleanser):
        """Test outlier removal."""
        data_with_outliers = pd.DataFrame({
            'normal_data': [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            'with_outliers': [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 100]  # 100 is outlier
        })
        
        cleaned_data = data_cleanser.remove_outliers(
            data_with_outliers, columns=['with_outliers']
        )
        
        # Check outlier removed
        assert len(cleaned_data) == 10  # One record removed
        assert cleaned_data['with_outliers'].max() == 19  # 100 removed
        
        # Check normal column unchanged
        assert len(cleaned_data['normal_data']) == 10
    
    def test_multiple_cleaning_operations(self, data_cleanser, sample_data_with_issues):
        """Test multiple cleaning operations in sequence."""
        # Apply multiple cleaning steps
        cleaned = data_cleanser.remove_duplicates(sample_data_with_issues, ['customer_id'])
        cleaned = data_cleanser.standardize_text(cleaned, ['first_name', 'last_name', 'status'])
        cleaned = data_cleanser.fix_email_format(cleaned)
        cleaned = data_cleanser.fix_phone_format(cleaned)
        cleaned = data_cleanser.handle_missing_values(cleaned)
        
        # Check final result
        assert cleaned['customer_id'].nunique() == len(cleaned)
        assert cleaned['first_name'].str.contains('  ').sum() == 0  # No extra spaces
        assert cleaned['email'].str.contains('@').all()  # All emails have @
        assert cleaned['phone'].str.startswith('+254').all()  # All phones have country code
        assert cleaned.isnull().sum().sum() == 0  # No missing values
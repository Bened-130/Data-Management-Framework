"""
Tests for the data standardization module.
"""

import pytest
import pandas as pd
from core.standardization import StandardizationManager


class TestStandardizationManager:
    
    @pytest.fixture
    def standardization_manager(self):
        return StandardizationManager()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame({
            'Customer ID': ['C001', 'C002', 'C003'],
            'First Name': ['John', 'Jane', 'Bob'],
            'Last_Name': ['Smith', 'Doe', 'Williams'],
            'registration-date': ['2023-01-01', '2023-02-01', '2023-03-01'],
            'AMOUNT': [100.50, 200.75, 300.25],
            'Status Code': [1, 2, 3]
        })
    
    def test_set_naming_convention(self, standardization_manager):
        """Test setting naming convention."""
        standardization_manager.set_naming_convention('snake_case')
        assert standardization_manager.standards['naming_convention'] == 'snake_case'
        
        standardization_manager.set_naming_convention('camelCase')
        assert standardization_manager.standards['naming_convention'] == 'camelCase'
    
    def test_standardize_column_names_snake_case(self, standardization_manager, sample_data):
        """Test column name standardization to snake_case."""
        standardization_manager.set_naming_convention('snake_case')
        standardized = standardization_manager.standardize_column_names(sample_data)
        
        # Check column names converted to snake_case
        expected_columns = [
            'customer_id',
            'first_name', 
            'last_name',
            'registration_date',
            'amount',
            'status_code'
        ]
        
        assert list(standardized.columns) == expected_columns
        
        # Check data integrity preserved
        assert standardized.shape == sample_data.shape
        assert standardized.iloc[0, 0] == 'C001'
        assert standardized.iloc[0, 1] == 'John'
    
    def test_create_data_dictionary(self, standardization_manager, sample_data):
        """Test data dictionary creation."""
        data_dict = standardization_manager.create_data_dictionary(sample_data)
        
        # Check structure
        assert isinstance(data_dict, pd.DataFrame)
        assert len(data_dict) == len(sample_data.columns)
        
        expected_columns = [
            'Column Name', 'Data Type', 'Non-Null Count',
            'Null Count', 'Unique Values', 'Sample Values'
        ]
        
        for col in expected_columns:
            assert col in data_dict.columns
        
        # Check content
        assert data_dict['Column Name'].tolist() == list(sample_data.columns)
        assert data_dict['Non-Null Count'].sum() == sample_data.size  # All values present
        assert data_dict['Null Count'].sum() == 0  # No null values in sample
        
        # Check sample values
        first_sample = data_dict[data_dict['Column Name'] == 'First Name']['Sample Values'].iloc[0]
        assert 'John' in first_sample
        assert 'Jane' in first_sample
    
    def test_data_dictionary_with_missing_values(self, standardization_manager):
        """Test data dictionary with missing values."""
        data_with_missing = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['A', None, 'C', None],
            'col3': [None, None, None, None]
        })
        
        data_dict = standardization_manager.create_data_dictionary(data_with_missing)
        
        # Check null counts
        col1_info = data_dict[data_dict['Column Name'] == 'col1']
        assert col1_info['Null Count'].iloc[0] == 1
        
        col2_info = data_dict[data_dict['Column Name'] == 'col2']
        assert col2_info['Null Count'].iloc[0] == 2
        
        col3_info = data_dict[data_dict['Column Name'] == 'col3']
        assert col3_info['Null Count'].iloc[0] == 4
        assert col3_info['Unique Values'].iloc[0] == 0
    
    def test_empty_dataframe(self, standardization_manager):
        """Test with empty dataframe."""
        empty_df = pd.DataFrame()
        
        # Standardize column names (should not crash)
        standardized = standardization_manager.standardize_column_names(empty_df)
        assert len(standardized.columns) == 0
        
        # Create data dictionary (should not crash)
        data_dict = standardization_manager.create_data_dictionary(empty_df)
        assert len(data_dict) == 0
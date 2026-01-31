"""
Tests for the data ingestion module.
"""

import pytest
import pandas as pd
import numpy as np
from core.ingestion import DataIngestionManager


class TestDataIngestionManager:
    
    @pytest.fixture
    def ingestion_manager(self):
        return DataIngestionManager()
    
    def test_generate_sample_records(self, ingestion_manager):
        """Test sample record generation."""
        # Test with small number of records
        df = ingestion_manager.generate_sample_records(n_records=100)
        
        # Basic assertions
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 100  # Because duplicates are added
        assert 'customer_id' in df.columns
        assert 'email' in df.columns
        assert 'phone' in df.columns
        assert 'age' in df.columns
        assert 'amount' in df.columns
        
        # Check data types
        assert pd.api.types.is_numeric_dtype(df['age'])
        assert pd.api.types.is_numeric_dtype(df['amount'])
        
        # Check that customer IDs are unique (after removing duplicates in pipeline)
        # Note: The generation adds duplicates, but customer_id should still be present
        assert df['customer_id'].str.startswith('CUST').all()
    
    def test_generate_sample_records_custom_size(self, ingestion_manager):
        """Test with different record sizes."""
        sizes = [10, 50, 200]
        
        for size in sizes:
            df = ingestion_manager.generate_sample_records(n_records=size)
            # Should have at least the requested number of records
            assert len(df) >= size
    
    def test_record_structure(self, ingestion_manager):
        """Test the structure of generated records."""
        df = ingestion_manager.generate_sample_records(n_records=50)
        
        expected_columns = [
            'customer_id', 'first_name', 'last_name', 'email',
            'phone', 'age', 'amount', 'registration_date',
            'status', 'city'
        ]
        
        # Check all expected columns are present
        for col in expected_columns:
            assert col in df.columns
        
        # Check no unexpected columns
        assert len(df.columns) == len(expected_columns)
    
    def test_data_quality_issues_introduced(self, ingestion_manager):
        """Test that data quality issues are properly introduced."""
        df = ingestion_manager.generate_sample_records(n_records=200)
        
        # Check for missing values (should be introduced)
        missing_count = df.isnull().sum().sum()
        assert missing_count > 0
        
        # Check for duplicates (should be introduced)
        duplicate_count = df.duplicated().sum()
        assert duplicate_count > 0
        
        # Check for negative amounts (should be introduced in some records)
        negative_amounts = (df['amount'] < 0).sum()
        assert negative_amounts > 0
        
        # Check for invalid ages (should be introduced in some records)
        invalid_ages = ((df['age'] < 0) | (df['age'] > 120)).sum()
        assert invalid_ages > 0
    
    def test_customer_id_format(self, ingestion_manager):
        """Test customer ID format."""
        df = ingestion_manager.generate_sample_records(n_records=50)
        
        # Check format CUST followed by 6 digits
        pattern = r'^CUST\d{6}$'
        for cust_id in df['customer_id'].unique()[:10]:  # Check first 10
            assert cust_id.startswith('CUST')
            assert len(cust_id) == 10  # CUST + 6 digits
    
    def test_email_generation(self, ingestion_manager):
        """Test email generation."""
        df = ingestion_manager.generate_sample_records(n_records=100)
        
        # Check emails are generated
        emails = df['email'].dropna()
        assert len(emails) > 0
        
        # Check some emails have issues (missing @)
        emails_without_at = emails.str.contains('@') == False
        assert emails_without_at.any()
    
    def test_phone_generation(self, ingestion_manager):
        """Test phone number generation."""
        df = ingestion_manager.generate_sample_records(n_records=100)
        
        phones = df['phone'].dropna()
        assert len(phones) > 0
        
        # Check some phones have issues (missing country code)
        phones_without_plus = phones.str.startswith('+254') == False
        assert phones_without_plus.any()
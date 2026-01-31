"""
Tests for the data validation module.
"""

import pytest
import pandas as pd
import numpy as np
from core.validation import ValidationEngine


class TestValidationEngine:
    
    @pytest.fixture
    def validation_engine(self):
        return ValidationEngine()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pd.DataFrame({
            'email': [
                'test@example.com',
                'invalid-email',
                None,
                'another.valid@test.co.uk',
                'missing.at.com'
            ],
            'phone': [
                '+254712345678',
                '0712345678',
                None,
                '+254700000000',
                '12345'
            ],
            'age': [25, -5, 30, 150, 18],
            'amount': [100.50, -50, 200, 0, 300.75],
            'customer_id': ['CUST000001', 'CUST000002', 'CUST000003', 'CUST000004', 'CUST000005']
        })
    
    def test_validate_email(self, validation_engine):
        """Test email validation function."""
        # Valid emails
        assert validation_engine.validate_email('test@example.com') == True
        assert validation_engine.validate_email('user.name@domain.co.uk') == True
        assert validation_engine.validate_email('test+tag@example.com') == True
        
        # Invalid emails
        assert validation_engine.validate_email('invalid-email') == False
        assert validation_engine.validate_email('@domain.com') == False
        assert validation_engine.validate_email('user@') == False
        assert validation_engine.validate_email('user@.com') == False
        
        # Null/NaN values
        assert validation_engine.validate_email(None) == False
        assert validation_engine.validate_email(np.nan) == False
        assert validation_engine.validate_email('') == False
    
    def test_validate_phone(self, validation_engine):
        """Test phone validation function."""
        # Valid phones (Kenya format)
        assert validation_engine.validate_phone('+254712345678') == True
        
        # Invalid phones
        assert validation_engine.validate_phone('0712345678') == False  # Missing country code
        assert validation_engine.validate_phone('254712345678') == False  # Missing +
        assert validation_engine.validate_phone('+25471') == False  # Too short
        assert validation_engine.validate_phone('+2547123456789') == False  # Too long
        
        # Null/NaN values
        assert validation_engine.validate_phone(None) == False
        assert validation_engine.validate_phone(np.nan) == False
    
    def test_validate_age(self, validation_engine):
        """Test age validation function."""
        # Valid ages
        assert validation_engine.validate_age(1) == True
        assert validation_engine.validate_age(25) == True
        assert validation_engine.validate_age(119) == True
        
        # Invalid ages
        assert validation_engine.validate_age(0) == False
        assert validation_engine.validate_age(-5) == False
        assert validation_engine.validate_age(150) == False
        
        # Null/NaN values
        assert validation_engine.validate_age(None) == False
        assert validation_engine.validate_age(np.nan) == False
    
    def test_validate_amount(self, validation_engine):
        """Test amount validation function."""
        # Valid amounts
        assert validation_engine.validate_amount(0.01) == True
        assert validation_engine.validate_amount(100.50) == True
        assert validation_engine.validate_amount(1000) == True
        
        # Invalid amounts
        assert validation_engine.validate_amount(0) == False
        assert validation_engine.validate_amount(-50) == False
        assert validation_engine.validate_amount(-0.01) == False
        
        # Null/NaN values
        assert validation_engine.validate_amount(None) == False
        assert validation_engine.validate_amount(np.nan) == False
    
    def test_add_validation_rule(self, validation_engine):
        """Test adding validation rules."""
        # Define a custom validation function
        def is_even(x):
            return x % 2 == 0 if not pd.isna(x) else False
        
        # Add rule
        validation_engine.add_validation_rule(
            'even_check',
            'test_column',
            is_even,
            'Value must be even'
        )
        
        # Verify rule was added
        assert 'even_check' in validation_engine.validation_rules
        rule = validation_engine.validation_rules['even_check']
        assert rule['column'] == 'test_column'
        assert rule['error_message'] == 'Value must be even'
        assert rule['function'] == is_even
    
    def test_run_validation(self, validation_engine, sample_data):
        """Test running validation on sample data."""
        # Add validation rules
        validation_engine.add_validation_rule(
            'email_check', 'email',
            validation_engine.validate_email, 'Invalid email'
        )
        validation_engine.add_validation_rule(
            'age_check', 'age',
            validation_engine.validate_age, 'Invalid age'
        )
        
        # Run validation
        validated_df = validation_engine.run_validation(sample_data)
        
        # Check validation columns were added
        assert 'email_valid' in validated_df.columns
        assert 'age_valid' in validated_df.columns
        
        # Check validation results
        # First email should be valid
        assert validated_df['email_valid'].iloc[0] == True
        # Second email should be invalid
        assert validated_df['email_valid'].iloc[1] == False
        # Third email (None) should be invalid
        assert validated_df['email_valid'].iloc[2] == False
        # Fourth email should be valid
        assert validated_df['email_valid'].iloc[3] == True
        # Fifth email should be invalid
        assert validated_df['email_valid'].iloc[4] == False
    
    def test_get_validation_summary(self, validation_engine, sample_data):
        """Test validation summary generation."""
        # Add rules and run validation
        validation_engine.add_validation_rule(
            'email_check', 'email',
            validation_engine.validate_email, 'Invalid email'
        )
        validation_engine.add_validation_rule(
            'phone_check', 'phone',
            validation_engine.validate_phone, 'Invalid phone'
        )
        
        validated_df = validation_engine.run_validation(sample_data)
        summary = validation_engine.get_validation_summary(validated_df)
        
        # Check summary structure
        assert 'total_records' in summary
        assert 'fields' in summary
        assert 'overall_validity' in summary
        
        assert summary['total_records'] == 5
        
        # Check field-specific results
        assert 'email' in summary['fields']
        assert 'phone' in summary['fields']
        
        email_results = summary['fields']['email']
        assert 'valid' in email_results
        assert 'invalid' in email_results
        assert 'validity_rate' in email_results
        
        # Check counts (2 valid emails out of 5)
        assert email_results['valid'] == 2
        assert email_results['invalid'] == 3
        assert email_results['validity_rate'] == 40.0  # 2/5 * 100
    
    def test_validation_with_empty_dataframe(self, validation_engine):
        """Test validation with empty dataframe."""
        empty_df = pd.DataFrame()
        
        validation_engine.add_validation_rule(
            'test_check', 'test_col',
            lambda x: True, 'Test error'
        )
        
        # Should not crash with empty dataframe
        validated_df = validation_engine.run_validation(empty_df)
        assert len(validated_df) == 0
        
        summary = validation_engine.get_validation_summary(validated_df)
        assert summary['total_records'] == 0
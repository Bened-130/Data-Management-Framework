"""
Tests for the main pipeline module.
"""

import pytest
import pandas as pd
from core.pipeline import DataManagementPipeline


class TestDataManagementPipeline:
    
    @pytest.fixture
    def pipeline(self):
        return DataManagementPipeline()
    
    def test_pipeline_initialization(self, pipeline):
        """Test pipeline initialization."""
        assert hasattr(pipeline, 'ingestion')
        assert hasattr(pipeline, 'validator')
        assert hasattr(pipeline, 'cleanser')
        assert hasattr(pipeline, 'standardizer')
        assert hasattr(pipeline, 'results')
        
        # Check components are initialized
        assert pipeline.ingestion is not None
        assert pipeline.validator is not None
        assert pipeline.cleanser is not None
        assert pipeline.standardizer is not None
        assert pipeline.results == {}
    
    def test_setup_validation_rules(self, pipeline):
        """Test validation rule setup."""
        pipeline.setup_validation_rules()
        
        # Check rules were added
        rules = pipeline.validator.validation_rules
        assert len(rules) >= 4  # At least 4 rules should be added
        
        # Check specific rules
        assert 'email_validation' in rules
        assert 'phone_validation' in rules
        assert 'age_validation' in rules
        assert 'amount_validation' in rules
        
        # Check rule configurations
        email_rule = rules['email_validation']
        assert email_rule['column'] == 'email'
        assert 'Invalid email format' in email_rule['error_message']
    
    def test_calculate_stats(self, pipeline):
        """Test statistics calculation."""
        test_data = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['A', 'B', 'A', None],
            'col3': [10, 20, 30, 40]
        })
        
        stats = pipeline._calculate_stats(test_data)
        
        # Check all expected stats are present
        expected_keys = ['total_records', 'total_columns', 'missing_values', 
                        'duplicate_records', 'data_size_mb']
        
        for key in expected_keys:
            assert key in stats
        
        # Check values
        assert stats['total_records'] == 4
        assert stats['total_columns'] == 3
        assert stats['missing_values'] == 2  # One in col1, one in col2
        assert stats['duplicate_records'] == 0  # No duplicates
    
    def test_calculate_quality_metrics(self, pipeline):
        """Test quality metrics calculation."""
        initial_stats = {
            'total_records': 100,
            'total_columns': 5,
            'missing_values': 10,
            'duplicate_records': 5
        }
        
        final_stats = {
            'total_records': 95,  # 5 duplicates removed
            'total_columns': 5,
            'missing_values': 2,  # Missing values handled
            'duplicate_records': 0
        }
        
        initial_validation = {'overall_validity': 80.0}
        final_validation = {'overall_validity': 95.0}
        
        metrics = pipeline._calculate_quality_metrics(
            initial_stats, final_stats,
            initial_validation, final_validation
        )
        
        # Check all expected metrics
        expected_metrics = [
            'initial_error_rate', 'final_error_rate', 'error_reduction',
            'accuracy', 'initial_validity', 'final_validity'
        ]
        
        for metric in expected_metrics:
            assert metric in metrics
        
        # Initial error rate calculation:
        # initial_errors = missing_values + (duplicate_records * total_columns)
        # initial_errors = 10 + (5 * 5) = 35
        # initial_cells = total_records * total_columns = 100 * 5 = 500
        # initial_error_rate = (35 / 500) * 100 = 7.0%
        assert metrics['initial_error_rate'] == 7.0
        
        # Final error rate calculation:
        # final_errors = 2 + (0 * 5) = 2
        # final_cells = 95 * 5 = 475
        # final_error_rate = (2 / 475) * 100 ≈ 0.42%
        assert round(metrics['final_error_rate'], 2) == 0.42
        
        # Error reduction = ((7.0 - 0.42) / 7.0) * 100 ≈ 94.0%
        assert round(metrics['error_reduction'], 1) == 94.0
        
        # Accuracy = 100 - 0.42 ≈ 99.58%
        assert round(metrics['accuracy'], 2) == 99.58
        
        # Validity scores
        assert metrics['initial_validity'] == 80.0
        assert metrics['final_validity'] == 95.0
    
    def test_pipeline_run_small_scale(self, pipeline):
        """Test running pipeline with small dataset."""
        # Run pipeline with small number of records
        results = pipeline.run_complete_pipeline(n_records=100)
        
        # Check results structure
        assert 'raw_data' in results
        assert 'cleaned_data' in results
        assert 'data_dictionary' in results
        assert 'quality_metrics' in results
        assert 'pipeline_time' in results
        
        # Check data was processed
        assert isinstance(results['raw_data'], pd.DataFrame)
        assert isinstance(results['cleaned_data'], pd.DataFrame)
        assert isinstance(results['data_dictionary'], pd.DataFrame)
        
        # Check quality metrics were calculated
        metrics = results['quality_metrics']
        assert 'accuracy' in metrics
        assert 'error_reduction' in metrics
        
        # Check accuracy improved
        assert metrics['final_validity'] > metrics['initial_validity']
        
        # Check cleaned data has no duplicates
        assert results['cleaned_data'].duplicated(subset=['customer_id']).sum() == 0
    
    def test_pipeline_results_structure(self, pipeline):
        """Test comprehensive results structure."""
        results = pipeline.run_complete_pipeline(n_records=500)
        
        # Check initial and final stats
        assert 'initial_stats' in results
        assert 'final_stats' in results
        
        # Check validation reports
        assert 'initial_validation' in results
        assert 'final_validation' in results
        
        # Check quality metrics
        quality_metrics = results['quality_metrics']
        required_metrics = [
            'initial_error_rate', 'final_error_rate', 
            'error_reduction', 'accuracy'
        ]
        
        for metric in required_metrics:
            assert metric in quality_metrics
            assert isinstance(quality_metrics[metric], (int, float))
        
        # Check pipeline time
        assert isinstance(results['pipeline_time'], float)
        assert results['pipeline_time'] > 0
        
        # Check data dictionary has expected columns
        data_dict = results['data_dictionary']
        expected_dict_columns = [
            'Column Name', 'Data Type', 'Non-Null Count',
            'Null Count', 'Unique Values', 'Sample Values'
        ]
        
        for col in expected_dict_columns:
            assert col in data_dict.columns
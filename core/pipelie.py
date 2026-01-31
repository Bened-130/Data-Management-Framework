"""
Main data management pipeline orchestrator.
"""

import pandas as pd
from datetime import datetime
from .ingestion import DataIngestionManager
from .validation import ValidationEngine
from .cleansing import DataCleanser
from .standardization import StandardizationManager

class DataManagementPipeline:
    """Complete data management and quality pipeline."""
    
    def __init__(self):
        self.ingestion = DataIngestionManager()
        self.validator = ValidationEngine()
        self.cleanser = DataCleanser()
        self.standardizer = StandardizationManager()
        self.results = {}
    
    def setup_validation_rules(self):
        """Setup validation rules for the pipeline."""
        self.validator.add_validation_rule(
            'email_validation', 'email',
            self.validator.validate_email, 'Invalid email format'
        )
        self.validator.add_validation_rule(
            'phone_validation', 'phone',
            self.validator.validate_phone, 'Invalid phone format'
        )
        self.validator.add_validation_rule(
            'age_validation', 'age',
            self.validator.validate_age, 'Invalid age range'
        )
        self.validator.add_validation_rule(
            'amount_validation', 'amount',
            self.validator.validate_amount, 'Invalid amount'
        )
    
    def run_complete_pipeline(self, n_records: int = 10000) -> dict:
        """Run complete data management pipeline."""
        print("=" * 60)
        print("STARTING DATA MANAGEMENT PIPELINE")
        print("=" * 60)
        
        pipeline_start = datetime.now()
        
        # Step 1: Data Ingestion
        print("\n--- STEP 1: Data Ingestion ---")
        raw_data = self.ingestion.generate_sample_records(n_records=n_records)
        initial_stats = self._calculate_stats(raw_data)
        
        # Step 2: Initial Validation
        print("\n--- STEP 2: Initial Validation ---")
        self.setup_validation_rules()
        validated_data = self.validator.run_validation(raw_data)
        initial_validation = self.validator.get_validation_summary(validated_data)
        
        # Step 3: Data Cleansing
        print("\n--- STEP 3: Data Cleansing ---")
        
        # Remove duplicates
        cleaned_data = self.cleanser.remove_duplicates(
            raw_data, subset=['customer_id']
        )
        
        # Standardize text
        cleaned_data = self.cleanser.standardize_text(
            cleaned_data, ['first_name', 'last_name', 'status']
        )
        
        # Fix formats
        cleaned_data = self.cleanser.fix_email_format(cleaned_data)
        cleaned_data = self.cleanser.fix_phone_format(cleaned_data)
        
        # Handle missing values
        cleaned_data = self.cleanser.handle_missing_values(cleaned_data)
        
        # Remove outliers
        cleaned_data = self.cleanser.remove_outliers(
            cleaned_data, ['age', 'amount']
        )
        
        # Step 4: Post-Cleaning Validation
        print("\n--- STEP 4: Post-Cleaning Validation ---")
        validated_clean = self.validator.run_validation(cleaned_data)
        final_validation = self.validator.get_validation_summary(validated_clean)
        
        # Step 5: Standardization
        print("\n--- STEP 5: Data Standardization ---")
        self.standardizer.set_naming_convention('snake_case')
        standardized_data = self.standardizer.standardize_column_names(cleaned_data)
        
        # Step 6: Data Dictionary
        print("\n--- STEP 6: Data Dictionary Creation ---")
        data_dictionary = self.standardizer.create_data_dictionary(standardized_data)
        
        # Calculate final stats
        final_stats = self._calculate_stats(standardized_data)
        
        # Calculate pipeline metrics
        pipeline_time = (datetime.now() - pipeline_start).total_seconds()
        quality_metrics = self._calculate_quality_metrics(
            initial_stats, final_stats,
            initial_validation, final_validation
        )
        
        # Compile results
        self.results = {
            'raw_data': raw_data,
            'cleaned_data': standardized_data,
            'data_dictionary': data_dictionary,
            'initial_stats': initial_stats,
            'final_stats': final_stats,
            'quality_metrics': quality_metrics,
            'initial_validation': initial_validation,
            'final_validation': final_validation,
            'pipeline_time': pipeline_time
        }
        
        # Display results
        self._display_results()
        
        return self.results
    
    def _calculate_stats(self, df: pd.DataFrame) -> dict:
        """Calculate statistics for dataframe."""
        return {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().sum(),
            'duplicate_records': df.duplicated().sum(),
            'data_size_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
        }
    
    def _calculate_quality_metrics(self, initial_stats: dict, final_stats: dict,
                                 initial_validation: dict, final_validation: dict) -> dict:
        """Calculate data quality metrics."""
        initial_cells = initial_stats['total_records'] * initial_stats['total_columns']
        final_cells = final_stats['total_records'] * final_stats['total_columns']
        
        initial_errors = (
            initial_stats['missing_values'] + 
            initial_stats['duplicate_records'] * initial_stats['total_columns']
        )
        final_errors = (
            final_stats['missing_values'] + 
            final_stats['duplicate_records'] * final_stats['total_columns']
        )
        
        initial_error_rate = (initial_errors / initial_cells) * 100 if initial_cells > 0 else 0
        final_error_rate = (final_errors / final_cells) * 100 if final_cells > 0 else 0
        
        error_reduction = ((initial_error_rate - final_error_rate) / initial_error_rate * 100) if initial_error_rate > 0 else 0
        accuracy = 100 - final_error_rate
        
        return {
            'initial_error_rate': round(initial_error_rate, 2),
            'final_error_rate': round(final_error_rate, 2),
            'error_reduction': round(error_reduction, 2),
            'accuracy': round(accuracy, 2),
            'initial_validity': initial_validation.get('overall_validity', 0),
            'final_validity': final_validation.get('overall_validity', 0)
        }
    
    def _display_results(self):
        """Display pipeline results."""
        print("\n" + "=" * 60)
        print("DATA MANAGEMENT PIPELINE RESULTS")
        print("=" * 60)
        
        metrics = self.results['quality_metrics']
        
        print(f"\nData Volume:")
        print(f"  Initial Records: {self.results['initial_stats']['total_records']:,}")
        print(f"  Final Records: {self.results['final_stats']['total_records']:,}")
        
        print(f"\nData Quality Metrics:")
        print(f"  Initial Error Rate: {metrics['initial_error_rate']:.2f}%")
        print(f"  Final Error Rate: {metrics['final_error_rate']:.2f}%")
        print(f"  Error Reduction: {metrics['error_reduction']:.2f}%")
        print(f"  Final Accuracy: {metrics['accuracy']:.2f}%")
        
        print(f"\nValidation Results:")
        print(f"  Initial Validity: {metrics['initial_validity']:.2f}%")
        print(f"  Final Validity: {metrics['final_validity']:.2f}%")
        
        print(f"\nPipeline Performance:")
        print(f"  Total Time: {self.results['pipeline_time']:.2f} seconds")
        
        print(f"\nData Dictionary Preview:")
        print(self.results['data_dictionary'].head(3).to_string(index=False))
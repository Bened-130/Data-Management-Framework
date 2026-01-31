#Configuration settings for the Data Management Framework.

class DataQualityConfig:
    """Data quality configuration."""
    
    # Validation thresholds
    EMAIL_VALIDITY_THRESHOLD = 0.95  # 95% validity required
    PHONE_VALIDITY_THRESHOLD = 0.90  # 90% validity required
    AGE_VALIDITY_THRESHOLD = 0.98    # 98% validity required
    
    # Cleaning settings
    OUTLIER_METHOD = 'iqr'  # 'iqr' or 'zscore'
    OUTLIER_THRESHOLD = 1.5  # For IQR method
    
    # Missing value handling
    MISSING_NUMERIC_STRATEGY = 'median'  # 'mean', 'median', 'mode'
    MISSING_CATEGORICAL_STRATEGY = 'mode'  # 'mode' or 'constant'
    
    # Standardization
    COLUMN_NAMING_CONVENTION = 'snake_case'  # 'snake_case', 'camelCase', 'PascalCase'
    DATE_FORMAT = '%Y-%m-%d'


class PipelineConfig:
    """Pipeline configuration."""
    
    # Processing
    BATCH_SIZE = 10000
    ENABLE_PARALLEL_PROCESSING = False
    
    # Output
    EXPORT_RESULTS = True
    OUTPUT_DIRECTORY = './output'
    
    # Validation
    FAIL_ON_CRITICAL_ERRORS = False
    MINIMUM_DATA_QUALITY_SCORE = 80.0  # Minimum acceptable quality score


class LoggingConfig:
    """Logging configuration."""
    
    LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOG_FILE = 'data_management.log'


# Global configuration instance
config = {
    'data_quality': DataQualityConfig(),
    'pipeline': PipelineConfig(),
    'logging': LoggingConfig()
}
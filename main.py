"""
Main entry point for the Data Management Framework.
"""

import sys
from core.pipeline import DataManagementPipeline

def main():
    """Execute the data management pipeline."""
    try:
        print("=" * 60)
        print("DATA MANAGEMENT FRAMEWORK")
        print("=" * 60)
        
        # Initialize and run pipeline
        pipeline = DataManagementPipeline()
        results = pipeline.run_complete_pipeline()
        
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"\nError running pipeline: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
"""
CPI Data Extractor
Extracts and validates CPI data from DOSM
"""
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from .dosm_client import DOSMClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CPIExtractor:
    """Extract and validate CPI data"""
    
    def __init__(self, client: DOSMClient):
        self.client = client
        
    def extract_full(self, save_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Extract complete CPI dataset with validation
        
        Args:
            save_path: Optional path to save data
            
        Returns:
            Validated DataFrame
        """
        logger.info("Starting full CPI extraction...")
        
        # Extract data
        df = self.client.get_cpi_data(granularity='2d')
        
        # Validate
        self._validate_data(df)
        
        # Save if path provided
        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(save_path, index=False)
            logger.info(f"💾 Saved to {save_path}")
        
        return df
    
    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate extracted data quality"""
        
        # Check required columns
        required_cols = ['state', 'date', 'division', 'index']
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Check for nulls
        null_counts = df[required_cols].isnull().sum()
        if null_counts.any():
            logger.warning(f"⚠️  Found nulls:\n{null_counts[null_counts > 0]}")
        
        # Check date range
        min_date = df['date'].min()
        max_date = df['date'].max()
        logger.info(f"📅 Date range: {min_date} to {max_date}")
        
        # Check states
        states = df['state'].nunique()
        logger.info(f"🗺️  States: {states}")
        
        # Check divisions
        divisions = df['division'].nunique()
        logger.info(f"📂 Divisions: {divisions}")
        
        logger.info(f"✅ Validation passed: {len(df):,} records")


# Add this import at the top

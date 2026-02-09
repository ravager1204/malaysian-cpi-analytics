"""
DOSM API Client
Professional client for accessing Malaysia Department of Statistics data
"""
import requests
import pandas as pd
import time
import logging
from typing import Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DOSMClient:
    """Client for DOSM (Department of Statistics Malaysia) API"""
    
    def __init__(self, storage_base_url: str = "https://storage.dosm.gov.my"):
        self.storage_base_url = storage_base_url
        logger.info("DOSM Client initialized")
    
    def get_cpi_data(self, granularity: str = '2d') -> pd.DataFrame:
        """
        Fetch CPI data from DOSM
        
        Args:
            granularity: '2d' for 2-digit, '3d' for 3-digit, '4d' for 4-digit
            
        Returns:
            DataFrame with CPI data
        """
        url = f"{self.storage_base_url}/cpi/cpi_{granularity}_state.parquet"
        
        try:
            logger.info(f"Fetching CPI data from {url}")
            df = pd.read_parquet(url)
            df['date'] = pd.to_datetime(df['date'])
            logger.info(f"✅ Fetched {len(df):,} records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch CPI data: {e}")
            raise
    
    def get_categories(self) -> pd.DataFrame:
        """Fetch MCOICOP category lookup table"""
        url = f"{self.storage_base_url}/dictionaries/mcoicop.parquet"
        
        try:
            logger.info(f"Fetching categories from {url}")
            df = pd.read_parquet(url)
            logger.info(f"✅ Fetched {len(df):,} categories")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch categories: {e}")
            raise
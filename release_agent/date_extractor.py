# release_agent/date_extractor.py

import re
import logging
from typing import Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DateExtractor:
    """Handles extraction of dates from natural language queries."""
    
    def __init__(self):
        self.month_map = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
            'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
            'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'october': 10, 'oct': 10,
            'november': 11, 'nov': 11, 'december': 12, 'dec': 12
        }
        
        self.quarter_map = {
            'q1': [1, 2, 3], 'first quarter': [1, 2, 3],
            'q2': [4, 5, 6], 'second quarter': [4, 5, 6],
            'q3': [7, 8, 9], 'third quarter': [7, 8, 9],
            'q4': [10, 11, 12], 'fourth quarter': [10, 11, 12]
        }
        
        self.hurricane_dates = {
            'hurricane ian': ['202209', '202208'],  # September 2022 + before for comparison
            'hurricane ida': ['202108', '202107'],  # August 2021
            'hurricane laura': ['202008', '202007'], # August 2020
            'hurricane harvey': ['201708', '201707'], # August 2017
        }
    
    def extract_dates(self, query: str) -> Optional[List[str]]:
        """
        Extract dates from query using various patterns.
        
        Args:
            query: Natural language query
            
        Returns:
            List of dates in YYYYMM format or None if no dates found
        """
        try:
            # Try advanced extraction first (with dateutil if available)
            return self._extract_dates_advanced(query)
        except ImportError:
            # Fallback to basic extraction
            logger.warning("dateutil not available, using basic date extraction")
            return self._extract_dates_basic(query)
    
    def _extract_dates_advanced(self, query: str) -> Optional[List[str]]:
        """Extract dates using advanced patterns with dateutil support."""
        try:
            from dateutil.relativedelta import relativedelta
        except ImportError:
            raise ImportError("dateutil not available")
        
        query_lower = query.lower()
        current_date = datetime.now()
        extracted_dates = []
        
        # Pattern 1: Specific months and years (January 2022, Jan 2023, etc.)
        extracted_dates.extend(self._extract_month_year_patterns(query_lower))
        
        # Pattern 2: Quarter patterns (Q1 2023, first quarter 2022, etc.)
        extracted_dates.extend(self._extract_quarter_patterns(query_lower))
        
        # Pattern 3: Year patterns (2022, year 2023, etc.)
        if not extracted_dates:  # Only if no specific months/quarters found
            extracted_dates.extend(self._extract_year_patterns(query_lower, current_date))
        
        # Pattern 4: Relative dates
        extracted_dates.extend(self._extract_relative_dates_advanced(query_lower, current_date, relativedelta))
        
        # Pattern 5: Hurricane/Storm specific dates
        extracted_dates.extend(self._extract_hurricane_dates(query_lower))
        
        # Pattern 6: Month ranges (March to June, etc.)
        extracted_dates.extend(self._extract_month_ranges(query_lower, current_date))
        
        # Remove duplicates and sort
        if extracted_dates:
            return self._clean_and_validate_dates(extracted_dates)
        
        return None
    
    def _extract_dates_basic(self, query: str) -> Optional[List[str]]:
        """Basic date extraction without dateutil dependency."""
        query_lower = query.lower()
        current_date = datetime.now()
        extracted_dates = []
        
        # Basic month/year extraction
        extracted_dates.extend(self._extract_month_year_patterns(query_lower))
        
        # Basic relative dates without dateutil
        if 'last year' in query_lower:
            year = current_date.year - 1
            for month in range(1, 13):
                extracted_dates.append(f"{year}{month:02d}")
        
        # Hurricane dates
        extracted_dates.extend(self._extract_hurricane_dates(query_lower))
        
        if extracted_dates:
            return self._clean_and_validate_dates(extracted_dates)
        
        return None
    
    def _extract_month_year_patterns(self, query_lower: str) -> List[str]:
        """Extract month/year patterns like 'January 2022'."""
        extracted = []
        month_year_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})'
        month_matches = re.findall(month_year_pattern, query_lower)
        
        for month_name, year in month_matches:
            if month_name in self.month_map:
                month_num = self.month_map[month_name]
                extracted.append(f"{year}{month_num:02d}")
        
        return extracted
    
    def _extract_quarter_patterns(self, query_lower: str) -> List[str]:
        """Extract quarter patterns like 'Q1 2023'."""
        extracted = []
        quarter_pattern = r'(q[1-4]|first quarter|second quarter|third quarter|fourth quarter)\s+(\d{4})'
        quarter_matches = re.findall(quarter_pattern, query_lower)
        
        for quarter, year in quarter_matches:
            if quarter in self.quarter_map:
                for month in self.quarter_map[quarter]:
                    extracted.append(f"{year}{month:02d}")
        
        return extracted
    
    def _extract_year_patterns(self, query_lower: str, current_date: datetime) -> List[str]:
        """Extract year patterns like '2022'."""
        extracted = []
        year_pattern = r'(?:year\s+)?(\d{4})(?!\d)'
        year_matches = re.findall(year_pattern, query_lower)
        
        for year in year_matches:
            if 2020 <= int(year) <= current_date.year + 1:  # Reasonable year range
                for month in range(1, 13):
                    extracted.append(f"{year}{month:02d}")
        
        return extracted
    
    def _extract_relative_dates_advanced(self, query_lower: str, current_date: datetime, relativedelta) -> List[str]:
        """Extract relative dates like 'last 3 months' using dateutil."""
        extracted = []
        
        if 'last' not in query_lower:
            return extracted
        
        # Last X months
        last_months_pattern = r'last\s+(\d+)\s+months?'
        last_matches = re.findall(last_months_pattern, query_lower)
        if last_matches:
            months_back = int(last_matches[0])
            for i in range(months_back):
                date = current_date - relativedelta(months=i)
                extracted.append(f"{date.year}{date.month:02d}")
            return extracted
        
        # Last quarter
        if 'last quarter' in query_lower:
            current_quarter = (current_date.month - 1) // 3 + 1
            if current_quarter == 1:
                prev_quarter_months = [10, 11, 12]
                year = current_date.year - 1
            else:
                prev_quarter_months = [(current_quarter - 2) * 3 + i for i in [1, 2, 3]]
                year = current_date.year
            
            for month in prev_quarter_months:
                extracted.append(f"{year}{month:02d}")
            return extracted
        
        # Last year
        if 'last year' in query_lower:
            year = current_date.year - 1
            for month in range(1, 13):
                extracted.append(f"{year}{month:02d}")
        
        return extracted
    
    def _extract_hurricane_dates(self, query_lower: str) -> List[str]:
        """Extract dates for specific hurricanes/storms."""
        for hurricane, dates in self.hurricane_dates.items():
            if hurricane in query_lower:
                return dates
        return []
    
    def _extract_month_ranges(self, query_lower: str, current_date: datetime) -> List[str]:
        """Extract month ranges like 'March to June 2023'."""
        extracted = []
        range_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+to\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(?:\s+(\d{4}))?'
        range_matches = re.findall(range_pattern, query_lower)
        
        for start_month, end_month, year in range_matches:
            year = year or str(current_date.year)
            start_num = self.month_map.get(start_month, 1)
            end_num = self.month_map.get(end_month, 12)
            
            if start_num <= end_num:
                for month in range(start_num, end_num + 1):
                    extracted.append(f"{year}{month:02d}")
        
        return extracted
    
    def _clean_and_validate_dates(self, extracted_dates: List[str]) -> List[str]:
        """Clean up and validate extracted dates."""
        validated = []
        for date_str in extracted_dates:
            if isinstance(date_str, str) and len(date_str) == 6 and date_str.isdigit():
                year = int(date_str[:4])
                month = int(date_str[4:])
                # Validate year and month ranges
                if 2020 <= year <= 2030 and 1 <= month <= 12:
                    validated.append(date_str)
        
        return sorted(list(set(validated)))  # Remove duplicates and sort
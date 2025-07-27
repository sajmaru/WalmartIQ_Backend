# release_agent/file_manager.py

import os
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class KGFileManager:
    """Manages KG file selection and validation."""
    
    def determine_target_files(
        self, 
        query_analysis: Dict[str, Any], 
        kg_path: str,
        date_range: Optional[List[str]] = None
    ) -> List[str]:
        """
        Determine which KG files to load based on query analysis.
        
        Args:
            query_analysis: Analysis results from QueryAnalyzer
            kg_path: Base path to KG files directory
            date_range: Optional explicit date range (YYYYMM format)
            
        Returns:
            List of file paths to load
        """
        
        # Priority 1: Use provided date_range parameter (from API call)
        if date_range:
            return self._get_files_for_dates(date_range, kg_path)
        
        # Priority 2: Use extracted date range from query analysis
        extracted_dates = query_analysis.get('extracted_date_range')
        if extracted_dates:
            target_files = self._get_files_for_dates(extracted_dates, kg_path)
            
            if target_files:
                logger.info(f"Using extracted date range: {extracted_dates}")
                return target_files
            else:
                logger.warning(f"No KG files found for extracted dates: {extracted_dates}")
        
        # Priority 3: Auto-determine based on query type (fallback)
        return self._auto_select_files(query_analysis, kg_path)
    
    def _get_files_for_dates(self, dates: List[str], kg_path: str) -> List[str]:
        """Get file paths for specific dates."""
        target_files = []
        for date in dates:
            file_path = f"{kg_path}/{date}.json"
            if os.path.exists(file_path):
                target_files.append(file_path)
            else:
                logger.warning(f"KG file not found: {file_path}")
        return target_files
    
    def _auto_select_files(self, query_analysis: Dict[str, Any], kg_path: str) -> List[str]:
        """Auto-select files based on query analysis when no specific dates are given."""
        available_files = self._get_available_files(kg_path)
        
        if not available_files:
            logger.warning(f"No KG files found in {kg_path}")
            return []
        
        # Select files based on analysis
        time_scope = query_analysis.get('time_scope', 'single_month')
        
        if time_scope == 'single_month':
            selected_files = available_files[-1:]  # Last 1 month
        elif time_scope == 'multi_month':
            selected_files = available_files[-6:]  # Last 6 months
        elif time_scope == 'year_over_year':
            selected_files = available_files[-24:]  # Last 2 years for comparison
        else:
            selected_files = available_files[-12:]  # Default: last year
        
        logger.info(f"Auto-selected {len(selected_files)} files based on time_scope: {time_scope}")
        return selected_files
    
    def _get_available_files(self, kg_path: str) -> List[str]:
        """Get all available KG files in chronological order."""
        available_files = []
        
        if not os.path.exists(kg_path):
            return available_files
        
        for file in os.listdir(kg_path):
            if file.endswith('.json') and len(file) == 11:  # YYYYMM.json format
                file_path = os.path.join(kg_path, file)
                # Validate the filename format
                date_part = file[:-5]  # Remove .json
                if self._is_valid_date_format(date_part):
                    available_files.append(file_path)
        
        available_files.sort()  # Chronological order
        return available_files
    
    def _is_valid_date_format(self, date_str: str) -> bool:
        """Validate that a string is in YYYYMM format."""
        if len(date_str) != 6 or not date_str.isdigit():
            return False
        
        year = int(date_str[:4])
        month = int(date_str[4:])
        
        return 2020 <= year <= 2030 and 1 <= month <= 12
    
    def validate_files_exist(self, file_paths: List[str]) -> List[str]:
        """Validate that files exist and return only existing ones."""
        existing_files = []
        for file_path in file_paths:
            if os.path.exists(file_path):
                existing_files.append(file_path)
            else:
                logger.warning(f"File does not exist: {file_path}")
        
        return existing_files
    
    def get_file_info(self, file_paths: List[str]) -> Dict[str, Any]:
        """Get information about the selected files."""
        info = {
            'file_count': len(file_paths),
            'date_range': [],
            'total_size_mb': 0
        }
        
        for file_path in file_paths:
            if os.path.exists(file_path):
                # Extract date from filename
                filename = os.path.basename(file_path)
                if filename.endswith('.json') and len(filename) == 11:
                    date_part = filename[:-5]
                    info['date_range'].append(date_part)
                
                # Get file size
                size_bytes = os.path.getsize(file_path)
                info['total_size_mb'] += size_bytes / (1024 * 1024)
        
        info['date_range'].sort()
        info['total_size_mb'] = round(info['total_size_mb'], 2)
        
        return info
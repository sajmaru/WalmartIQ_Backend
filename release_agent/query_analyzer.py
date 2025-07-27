# release_agent/query_analyzer.py

import json
import re
import logging
from typing import Dict, Any
from datetime import datetime

from .date_extractor import DateExtractor
from .schema_manager import KGSchemaManager

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """Analyzes natural language queries to understand intent and extract information."""
    
    def __init__(self, llm_model=None, schema_manager: KGSchemaManager = None):
        self.llm = llm_model
        self.date_extractor = DateExtractor()
        self.schema_manager = schema_manager or KGSchemaManager()
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze the query to understand intent and extract key information.
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary containing analysis results
        """
        current_date = datetime.now()
        
        # Try LLM-based analysis first
        if self.llm:
            try:
                return self._analyze_with_llm(query, current_date)
            except Exception as e:
                logger.warning(f"LLM analysis failed, using basic analysis: {e}")
        
        # Fallback to basic analysis
        return self._analyze_basic(query)
    
    def _analyze_with_llm(self, query: str, current_date: datetime) -> Dict[str, Any]:
        """Analyze query using LLM for better understanding."""
        analysis_prompt = f"""
        Analyze this query about retail/sales data and classify it:
        
        Query: "{query}"
        Current Date: {current_date.strftime('%Y-%m-%d')}
        
        KG Schema Context:
        {json.dumps(self.schema_manager.schema, indent=2)}
        
        Extract specific date ranges from the query and determine:
        1. Query type (temporal_analysis, spatial_analysis, comparison, aggregation, correlation, impact_analysis)
        2. Time scope (single_month, multi_month, year_over_year, seasonal)
        3. Geographic scope (all_locations, specific_state, specific_stores)
        4. Business scope (all_sbus, specific_sbu, specific_department)
        5. Key entities mentioned (hurricanes, weather events, sales metrics, etc.)
        6. Analysis type (trend, correlation, impact, comparison, aggregation)
        7. Required node types from schema
        8. Relevant query pattern from schema
        9. **EXTRACTED DATE RANGE**: Convert any date mentions to YYYYMM format
        
        Date Range Extraction Examples:
        - "January 2022" → ["202201"]
        - "Q1 2023" → ["202301", "202302", "202303"] 
        - "March to June 2023" → ["202303", "202304", "202305", "202306"]
        - "Hurricane Ian in September 2022" → ["202209", "202208"] (include before for comparison)
        - "last 3 months" → [last 3 months from current date]
        - "2022 vs 2023" → ["202201" through "202212", "202301" through "202312"]
        - "recent trends" → [last 6 months from current date]
        - "this year" → [current year months]
        - "before hurricane" / "after storm" → infer appropriate months
        
        Return as JSON:
        {{
            "type": "temporal_analysis",
            "time_scope": "multi_month",
            "geographic_scope": "specific_state",
            "business_scope": "specific_sbu", 
            "entities": ["hurricane", "electronics", "florida"],
            "analysis_type": "impact",
            "requires_weather": true,
            "requires_geospatial": true,
            "target_node_types": ["sbu", "sbu_store", "weather"],
            "query_pattern": "weather_impact",
            "extracted_date_range": ["202209", "202208"],
            "date_extraction_reasoning": "Hurricane Ian occurred in September 2022, included August for before/after comparison"
        }}
        """
        
        analysis_json = self.llm.generate(analysis_prompt)
        
        # Try to extract JSON from the response
        json_match = re.search(r'\{.*\}', analysis_json, re.DOTALL)
        if json_match:
            analysis = json.loads(json_match.group())
            # Validate and clean up extracted date range
            if 'extracted_date_range' in analysis:
                analysis['extracted_date_range'] = self._validate_date_range(analysis['extracted_date_range'])
            return analysis
        else:
            # If LLM response can't be parsed, fall back to basic
            raise ValueError("Could not parse LLM response as JSON")
    
    def _analyze_basic(self, query: str) -> Dict[str, Any]:
        """Fallback basic query analysis using keyword matching."""
        query_lower = query.lower()
        
        analysis = {
            "type": "aggregation",  # default
            "time_scope": "single_month",
            "geographic_scope": "all_locations", 
            "business_scope": "all_sbus",
            "entities": [],
            "analysis_type": "aggregation",
            "requires_weather": False,
            "requires_geospatial": False,
            "target_node_types": ["sbu", "store"],
            "query_pattern": "sbu_analysis",
            "extracted_date_range": None
        }
        
        # Extract date range using the date extractor
        extracted_dates = self.date_extractor.extract_dates(query)
        if extracted_dates:
            analysis['extracted_date_range'] = extracted_dates
        
        # Determine query type and node types based on keywords
        analysis.update(self._classify_query_type(query_lower))
        analysis.update(self._determine_scope(query_lower))
        
        return analysis
    
    def _classify_query_type(self, query_lower: str) -> Dict[str, Any]:
        """Classify query type based on keywords."""
        updates = {}
        
        if any(word in query_lower for word in ['hurricane', 'storm', 'weather', 'temperature']):
            updates.update({
                'type': 'impact_analysis',
                'requires_weather': True,
                'requires_geospatial': True,
                'target_node_types': ['weather', 'day_store', 'store'],
                'query_pattern': 'weather_impact'
            })
        elif any(word in query_lower for word in ['compare', 'vs', 'versus', 'difference']):
            updates.update({
                'type': 'comparison',
                'target_node_types': ['sbu', 'dept', 'store']
            })
        elif any(word in query_lower for word in ['trend', 'over time', 'timeline', 'before', 'after']):
            updates.update({
                'type': 'temporal_analysis',
                'time_scope': 'multi_month',
                'target_node_types': ['month', 'day', 'sbu', 'store'],
                'query_pattern': 'temporal_analysis'
            })
        elif any(word in query_lower for word in ['store', 'location', 'shop']):
            updates.update({
                'target_node_types': ['store', 'day_store', 'sbu_store'],
                'query_pattern': 'store_performance'
            })
        elif any(word in query_lower for word in ['department', 'category', 'dept']):
            updates.update({
                'target_node_types': ['dept', 'store'],
                'query_pattern': 'department_analysis'
            })
        
        return updates
    
    def _determine_scope(self, query_lower: str) -> Dict[str, Any]:
        """Determine geographic and business scope."""
        updates = {}
        
        # Geographic scope
        if any(word in query_lower for word in ['state', 'florida', 'california', 'texas']):
            updates['geographic_scope'] = 'specific_state'
            updates['requires_geospatial'] = True
        
        # Business scope
        if any(word in query_lower for word in ['food', 'home']):
            updates['business_scope'] = 'specific_sbu'
        
        return updates
    
    def _validate_date_range(self, date_range: list) -> list:
        """Validate and clean up extracted date range."""
        if not date_range:
            return []
        
        validated = []
        for date_str in date_range:
            # Ensure YYYYMM format
            if isinstance(date_str, str) and len(date_str) == 6 and date_str.isdigit():
                year = int(date_str[:4])
                month = int(date_str[4:])
                # Validate year and month ranges
                if 2020 <= year <= 2030 and 1 <= month <= 12:
                    validated.append(date_str)
        
        return sorted(list(set(validated)))  # Remove duplicates and sort
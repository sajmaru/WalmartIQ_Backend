# release_agent/result_formatter.py

import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class ResultFormatter:
    """Formats execution results and generates insights for frontend consumption."""
    
    def format_results(self, execution_result: Dict[str, Any], query_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format execution results for frontend consumption.
        
        Args:
            execution_result: Raw execution results from code executor
            query_analysis: Original query analysis
            
        Returns:
            Formatted results dictionary
        """
        if not execution_result.get('success'):
            return {'error': execution_result.get('error', 'Execution failed')}
        
        raw_results = execution_result.get('result', {})
        
        # Structure the data based on query type
        formatted = {
            'query_type': query_analysis['type'],
            'data': raw_results.get('data', []),
            'metadata': raw_results.get('metadata', {}),
            'summary': raw_results.get('summary', {}),
            'timestamp': datetime.now().isoformat(),
            'schema_info': {
                'node_types_used': query_analysis.get('target_node_types', []),
                'query_pattern': query_analysis.get('query_pattern', 'general')
            }
        }
        
        return formatted
    
    def generate_insights(self, formatted_data: Dict[str, Any], original_query: str) -> List[str]:
        """
        Generate insights about the query results.
        
        Args:
            formatted_data: Formatted results from format_results
            original_query: Original user query
            
        Returns:
            List of insight strings
        """
        if 'error' in formatted_data:
            return [f"Query execution failed: {formatted_data['error']}"]
        
        insights = []
        
        data = formatted_data.get('data', [])
        if isinstance(data, list) and len(data) > 0:
            insights.append(f"Found {len(data)} data points matching your query")
            
            # Add insights based on node types
            node_types = formatted_data.get('schema_info', {}).get('node_types_used', [])
            if node_types:
                insights.append(f"Analysis focused on: {', '.join(node_types)} level data")
            
            # Add data distribution insights
            insights.extend(self._generate_data_insights(data))
        
        summary = formatted_data.get('summary', {})
        if summary:
            for key, value in summary.items():
                insights.append(f"{key.replace('_', ' ').title()}: {value}")
        
        # Add query pattern insight
        query_pattern = formatted_data.get('schema_info', {}).get('query_pattern')
        if query_pattern and query_pattern != 'general':
            insights.append(f"Query pattern used: {query_pattern}")
        
        if not insights:
            insights.append("Query completed successfully")
        
        return insights
    
    def _generate_data_insights(self, data: List[Dict[str, Any]]) -> List[str]:
        """Generate insights based on the actual data returned."""
        insights = []
        
        if not data:
            return insights
        
        # Analyze node type distribution
        node_type_counts = {}
        for item in data:
            node_type = item.get('node_type', 'unknown')
            node_type_counts[node_type] = node_type_counts.get(node_type, 0) + 1
        
        if len(node_type_counts) > 1:
            most_common = max(node_type_counts.items(), key=lambda x: x[1])
            insights.append(f"Most common data type: {most_common[0]} ({most_common[1]} records)")
        
        # Analyze SBU distribution if applicable
        sbu_counts = {}
        for item in data:
            node_id = item.get('node_id', '')
            if 'FOOD' in node_id:
                sbu_counts['FOOD'] = sbu_counts.get('FOOD', 0) + 1
            elif 'HOME' in node_id:
                sbu_counts['HOME'] = sbu_counts.get('HOME', 0) + 1
        
        if sbu_counts:
            if len(sbu_counts) == 2:
                insights.append(f"Data includes both FOOD ({sbu_counts.get('FOOD', 0)}) and HOME ({sbu_counts.get('HOME', 0)}) SBUs")
            else:
                dominant_sbu = max(sbu_counts.items(), key=lambda x: x[1])
                insights.append(f"Data primarily from {dominant_sbu[0]} SBU ({dominant_sbu[1]} records)")
        
        # Analyze temporal distribution
        dates = set()
        for item in data:
            node_id = item.get('node_id', '')
            if len(node_id) >= 8 and node_id[:8].isdigit():
                dates.add(node_id[:8])
        
        if dates:
            if len(dates) == 1:
                insights.append(f"Data from a single date: {list(dates)[0]}")
            else:
                insights.append(f"Data spans {len(dates)} different dates")
        
        # Analyze store distribution
        stores = set()
        for item in data:
            st_cd = item.get('st_cd')
            if st_cd:
                stores.add(st_cd)
        
        if stores:
            insights.append(f"Data covers {len(stores)} unique stores")
        
        return insights
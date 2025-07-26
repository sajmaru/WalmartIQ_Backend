# release_agent/kg_query_agent.py

import json
import os
import re
import ast
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from google.adk.agents import Agent
from .llm_model import llm_model
from .secure_executor import SecureCodeExecutor  # We'll create this next

logger = logging.getLogger(__name__)

class KGQueryAgent(Agent):
    """
    Specialized agent for processing natural language queries against Knowledge Graphs.
    
    This agent:
    1. Analyzes user queries to understand intent
    2. Generates Python code to query KG files
    3. Executes code safely in a sandbox
    4. Returns formatted data for frontend consumption
    """
    
    def __init__(self):
        super().__init__(
            name="kg_query_agent",
            description="Agent specialized in dynamic knowledge graph querying using LLM-generated code"
        )
        object.__setattr__(self, 'llm', llm_model)
        self.executor = SecureCodeExecutor()
        self.kg_schema = self._load_kg_schema()
    
    def _load_kg_schema(self) -> Dict[str, Any]:
        """Load and cache the KG schema for better code generation."""
        return {
            "hierarchy": ["Month", "Day", "SBU", "Department", "Store", "Weather"],
            "node_types": {
                "month": {"color": "#FFD700", "properties": ["month_id", "year"]},
                "day": {"color": "#FF8C00", "properties": ["day_id", "date"]},
                "sbu": {"color": "#FF6F61", "properties": ["sbu_id", "sbu_name", "daily_sbu_GMV_AMT"]},
                "dept": {"color": "#F7CAC9", "properties": ["dept_id", "dept_name", "daily_dept_GMV_AMT"]},
                "store": {"color": "#88B04B", "properties": ["store_id", "total_sales_unit", "total_gmv_amt", "LAT_DGR", "LONG_DGR"]},
                "weather": {"color": "#00A8E8", "properties": ["AVG_AIR_TEMPR_DGR", "AVG_POS_DLY_SNOWFALL_QTY", "LAT_DGR", "LONG_DGR"]}
            },
            "relationships": [
                "Month -> Day",
                "Day -> SBU", 
                "SBU -> Department",
                "Department -> Store",
                "Store -> Weather"
            ]
        }
    
    async def process_query(
        self, 
        query: str, 
        kg_path: str = "Data/KGs",
        date_range: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process a natural language query against KG data.
        
        Args:
            query: Natural language query
            kg_path: Path to KG files directory
            date_range: Optional list of KG files to query (e.g., ["202201", "202202"])
            context: Additional context for the query
            
        Returns:
            Dictionary containing data, generated code, insights, etc.
        """
        try:
            # Step 1: Analyze query intent and classify query type
            query_analysis = self._analyze_query(query)
            
            # Step 2: Determine which KG files to use
            target_files = self._determine_target_files(query_analysis, kg_path, date_range)
            
            # Step 3: Generate Python code to query the KG
            generated_code = self._generate_kg_query_code(query, query_analysis, target_files)
            
            # Step 4: Execute the code safely
            execution_result = await self._execute_code_safely(generated_code, kg_path)
            
            # Step 5: Format results for frontend
            formatted_data = self._format_results(execution_result, query_analysis)
            
            # Step 6: Generate insights
            insights = self._generate_insights(formatted_data, query)
            
            return {
                'data': formatted_data,
                'generated_code': generated_code,
                'query_type': query_analysis['type'],
                'insights': insights,
                'target_files': target_files,
                'execution_success': execution_result.get('success', False)
            }
            
        except Exception as e:
            logger.error(f"Error processing query '{query}': {str(e)}")
            raise
    
    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze the query to understand intent and extract key information."""
        
        analysis_prompt = f"""
        Analyze this query about retail/sales data and classify it:
        
        Query: "{query}"
        
        Determine:
        1. Query type (temporal_analysis, spatial_analysis, comparison, aggregation, correlation, impact_analysis)
        2. Time scope (single_month, multi_month, year_over_year, seasonal)
        3. Geographic scope (all_locations, specific_state, specific_stores)
        4. Business scope (all_sbus, specific_sbu, specific_department)
        5. Key entities mentioned (hurricanes, weather events, sales metrics, etc.)
        6. Analysis type (trend, correlation, impact, comparison, aggregation)
        
        Return as JSON:
        {{
            "type": "temporal_analysis",
            "time_scope": "multi_month",
            "geographic_scope": "specific_state",
            "business_scope": "specific_sbu", 
            "entities": ["hurricane", "electronics", "florida"],
            "analysis_type": "impact",
            "requires_weather": true,
            "requires_geospatial": true
        }}
        """
        
        try:
            analysis_json = self.llm.generate(analysis_prompt)
            # Try to extract JSON from the response
            json_match = re.search(r'\{.*\}', analysis_json, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                # Fallback to basic analysis
                return self._basic_query_analysis(query)
        except Exception as e:
            logger.warning(f"LLM analysis failed, using basic analysis: {e}")
            return self._basic_query_analysis(query)
    
    def _basic_query_analysis(self, query: str) -> Dict[str, Any]:
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
            "requires_geospatial": False
        }
        
        # Determine query type
        if any(word in query_lower for word in ['hurricane', 'storm', 'weather', 'temperature']):
            analysis['type'] = 'impact_analysis'
            analysis['requires_weather'] = True
            analysis['requires_geospatial'] = True
        elif any(word in query_lower for word in ['compare', 'vs', 'versus', 'difference']):
            analysis['type'] = 'comparison'
        elif any(word in query_lower for word in ['trend', 'over time', 'timeline', 'before', 'after']):
            analysis['type'] = 'temporal_analysis'
            analysis['time_scope'] = 'multi_month'
        elif any(word in query_lower for word in ['correlation', 'relationship', 'impact']):
            analysis['type'] = 'correlation'
        
        # Determine scope
        if any(word in query_lower for word in ['state', 'florida', 'california', 'texas']):
            analysis['geographic_scope'] = 'specific_state'
            analysis['requires_geospatial'] = True
        
        if any(word in query_lower for word in ['electronics', 'clothing', 'home', 'sports', 'beauty']):
            analysis['business_scope'] = 'specific_sbu'
        
        return analysis
    
    def _determine_target_files(
        self, 
        query_analysis: Dict[str, Any], 
        kg_path: str,
        date_range: Optional[List[str]] = None
    ) -> List[str]:
        """Determine which KG files to load based on query analysis."""
        
        if date_range:
            return [f"{kg_path}/{date}.json" for date in date_range if os.path.exists(f"{kg_path}/{date}.json")]
        
        # Auto-determine based on query type
        available_files = []
        if os.path.exists(kg_path):
            for file in os.listdir(kg_path):
                if file.endswith('.json') and len(file) == 11:  # YYYYMM.json format
                    available_files.append(os.path.join(kg_path, file))
        
        available_files.sort()
        
        # Select files based on analysis
        if query_analysis['time_scope'] == 'single_month':
            return available_files[-1:] if available_files else []
        elif query_analysis['time_scope'] == 'multi_month':
            return available_files[-6:] if available_files else []  # Last 6 months
        else:
            return available_files[-12:] if available_files else []  # Last year
    
    def _generate_kg_query_code(
        self, 
        query: str, 
        analysis: Dict[str, Any], 
        target_files: List[str]
    ) -> str:
        """Generate Python code to query the knowledge graph based on analysis."""
        
        code_prompt = f"""
        Generate Python code to query knowledge graphs for this analysis:
        
        Query: "{query}"
        Analysis: {json.dumps(analysis, indent=2)}
        Target files: {target_files}
        
        KG Schema:
        {json.dumps(self.kg_schema, indent=2)}
        
        Requirements:
        1. Load the NetworkX graphs from JSON files
        2. Query the graph structure according to the hierarchy
        3. Extract relevant data based on the query
        4. Return results in a standardized format
        
        Available imports: json, networkx as nx, pandas as pd, numpy as np, datetime
        
        Code template:
        ```python
        import json
        import networkx as nx
        import pandas as pd
        import numpy as np
        from datetime import datetime
        
        # Results dictionary to return
        results = {{
            'data': [],
            'metadata': {{}},
            'summary': {{}}
        }}
        
        try:
            # Load KG files
            graphs = []
            for file_path in {target_files}:
                with open(file_path, 'r') as f:
                    kg_data = json.load(f)
                    kg = nx.node_link_graph(kg_data)
                    graphs.append((file_path, kg))
            
            # Your analysis code here based on the query
            # ... 
            
            # Format results
            results['data'] = analyzed_data
            results['metadata'] = {{'query_type': '{analysis['type']}', 'file_count': len(graphs)}}
            results['summary'] = {{'total_records': len(analyzed_data)}}
            
        except Exception as e:
            results['error'] = str(e)
        
        # Return results
        results
        ```
        
        Generate complete, executable Python code that returns the 'results' dictionary.
        Focus on extracting meaningful data that matches the query intent.
        """
        
        generated_code = self.llm.generate(code_prompt)
        
        # Clean up the code (remove markdown formatting if present)
        code_match = re.search(r'```python\n(.*?)\n```', generated_code, re.DOTALL)
        if code_match:
            return code_match.group(1)
        else:
            # If no markdown formatting, return as-is but validate
            return self._validate_and_fix_code(generated_code)
    
    def _validate_and_fix_code(self, code: str) -> str:
        """Validate generated code and attempt basic fixes."""
        try:
            # Parse the code to check for syntax errors
            ast.parse(code)
            return code
        except SyntaxError as e:
            logger.warning(f"Syntax error in generated code: {e}")
            # Try to fix common issues
            fixed_code = self._fix_common_syntax_errors(code)
            try:
                ast.parse(fixed_code)
                return fixed_code
            except:
                logger.error("Could not fix syntax errors in generated code")
                return code  # Return original even if broken
    
    def _fix_common_syntax_errors(self, code: str) -> str:
        """Apply common fixes to generated code."""
        # Remove any text before the first import or code line
        lines = code.split('\n')
        start_idx = 0
        for i, line in enumerate(lines):
            if line.strip().startswith(('import ', 'from ', 'results =', '#')):
                start_idx = i
                break
        
        return '\n'.join(lines[start_idx:])
    
    async def _execute_code_safely(self, code: str, kg_path: str) -> Dict[str, Any]:
        """Execute the generated code in a secure environment."""
        try:
            result = await self.executor.execute(code, working_directory=kg_path)
            return result
        except Exception as e:
            logger.error(f"Code execution failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _format_results(self, execution_result: Dict[str, Any], query_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Format execution results for frontend consumption."""
        if not execution_result.get('success'):
            return {'error': execution_result.get('error', 'Execution failed')}
        
        raw_results = execution_result.get('result', {})
        
        # Structure the data based on query type
        formatted = {
            'query_type': query_analysis['type'],
            'data': raw_results.get('data', []),
            'metadata': raw_results.get('metadata', {}),
            'summary': raw_results.get('summary', {}),
            'timestamp': datetime.now().isoformat()
        }
        
        return formatted
    
    def _generate_insights(self, formatted_data: Dict[str, Any], original_query: str) -> List[str]:
        """Generate insights about the query results."""
        if 'error' in formatted_data:
            return [f"Query execution failed: {formatted_data['error']}"]
        
        insights = []
        
        data = formatted_data.get('data', [])
        if isinstance(data, list) and len(data) > 0:
            insights.append(f"Found {len(data)} data points matching your query")
        
        summary = formatted_data.get('summary', {})
        if summary:
            for key, value in summary.items():
                insights.append(f"{key.replace('_', ' ').title()}: {value}")
        
        if not insights:
            insights.append("Query completed successfully")
        
        return insights
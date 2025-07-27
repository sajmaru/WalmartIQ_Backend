# release_agent/code_generator.py

import json
import re
import ast
import logging
from typing import Dict, Any, List

from .schema_manager import KGSchemaManager

logger = logging.getLogger(__name__)


class KGCodeGenerator:
    """Generates Python code to query knowledge graphs based on analysis."""
    
    def __init__(self, llm_model=None, schema_manager: KGSchemaManager = None):
        self.llm = llm_model
        self.schema_manager = schema_manager or KGSchemaManager()
    
    def generate_query_code(
        self, 
        query: str, 
        analysis: Dict[str, Any], 
        target_files: List[str]
    ) -> str:
        """
        Generate Python code to query the knowledge graph based on analysis.
        
        Args:
            query: Original natural language query
            analysis: Query analysis results
            target_files: List of KG files to query
            
        Returns:
            Generated Python code as string
        """
        if self.llm:
            try:
                return self._generate_with_llm(query, analysis, target_files)
            except Exception as e:
                logger.warning(f"LLM code generation failed, using template: {e}")
        
        # Fallback to template-based generation
        return self._generate_with_template(analysis, target_files)
    
    def _generate_with_llm(
        self, 
        query: str, 
        analysis: Dict[str, Any], 
        target_files: List[str]
    ) -> str:
        """Generate code using LLM for more sophisticated queries."""
        code_prompt = f"""
        Generate Python code to query knowledge graphs for this analysis:
        
        Query: "{query}"
        Analysis: {json.dumps(analysis, indent=2)}
        Target files: {target_files}
        
        Complete KG Schema:
        {json.dumps(self.schema_manager.schema, indent=2)}
        
        Requirements:
        1. Load the NetworkX graphs from JSON files
        2. Query the graph structure according to the hierarchy and node types
        3. Focus on node types: {analysis.get('target_node_types', [])}
        4. Use query pattern: {analysis.get('query_pattern', 'general')}
        5. Extract relevant data based on the query
        6. Return results in a standardized format
        
        Available imports: json, networkx as nx, pandas as pd, numpy as np, datetime
        
        Node ID patterns to use:
        {json.dumps(self.schema_manager.schema['node_id_patterns'], indent=2)}
        
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
            
            # Initialize data collection
            analyzed_data = []
            
            # Process each graph
            for file_path, kg in graphs:
                # Extract nodes by type based on analysis
                target_node_types = {analysis.get('target_node_types', ['sbu', 'store'])}
                
                for node_id, node_attrs in kg.nodes(data=True):
                    node_type = node_attrs.get('node_type')
                    if node_type in target_node_types:
                        # Collect relevant data based on query
                        data_point = {{
                            'node_id': node_id,
                            'node_type': node_type,
                            'file_source': file_path,
                            **node_attrs
                        }}
                        analyzed_data.append(data_point)
            
            # Format results based on query pattern
            results['data'] = analyzed_data
            results['metadata'] = {{
                'query_type': '{analysis['type']}', 
                'file_count': len(graphs),
                'target_node_types': {analysis.get('target_node_types', [])},
                'query_pattern': '{analysis.get('query_pattern', 'general')}'
            }}
            results['summary'] = {{'total_records': len(analyzed_data)}}
            
        except Exception as e:
            results['error'] = str(e)
        
        # Print results as JSON for capture by executor
        print(json.dumps(results))
        ```
        
        Generate complete, executable Python code that prints the 'results' dictionary as JSON.
        The final line MUST be: print(json.dumps(results))
        
        IMPORTANT: Do not end with just 'results' - always end with print(json.dumps(results)) so the executor can capture the output.
        
        Focus on extracting meaningful data that matches the query intent and uses the correct node types.
        """
        
        generated_code = self.llm.generate(code_prompt)
        
        # Clean up the code (remove markdown formatting if present)
        code_match = re.search(r'```python\n(.*?)\n```', generated_code, re.DOTALL)
        if code_match:
            return code_match.group(1)
        else:
            # If no markdown formatting, return as-is but validate
            return self.validate_and_fix_code(generated_code)
    
    def _generate_with_template(self, analysis: Dict[str, Any], target_files: List[str]) -> str:
        """Generate code using a template-based approach."""
        query_pattern = analysis.get('query_pattern', 'general')
        target_node_types = analysis.get('target_node_types', ['sbu', 'store'])
        
        # Basic template that works for most queries
        template = f"""
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
    
    # Initialize data collection
    analyzed_data = []
    
    # Process each graph
    for file_path, kg in graphs:
        # Extract nodes by type based on analysis
        target_node_types = {target_node_types}
        
        for node_id, node_attrs in kg.nodes(data=True):
            node_type = node_attrs.get('node_type')
            if node_type in target_node_types:
                # Collect relevant data based on query
                data_point = {{
                    'node_id': node_id,
                    'node_type': node_type,
                    'file_source': file_path,
                    **node_attrs
                }}
                analyzed_data.append(data_point)
    
    # Apply pattern-specific processing
    {self._get_pattern_specific_code(query_pattern)}
    
    # Format results
    results['data'] = analyzed_data
    results['metadata'] = {{
        'query_type': '{analysis.get('type', 'general')}', 
        'file_count': len(graphs),
        'target_node_types': {target_node_types},
        'query_pattern': '{query_pattern}'
    }}
    results['summary'] = {{'total_records': len(analyzed_data)}}
    
except Exception as e:
    results['error'] = str(e)

# Print results as JSON for capture by executor
print(json.dumps(results))
"""
        return template.strip()
    
    def _get_pattern_specific_code(self, pattern: str) -> str:
        """Get pattern-specific processing code."""
        pattern_code = {
            'weather_impact': """
    # Filter for weather-related nodes and correlate with sales
    weather_data = [d for d in analyzed_data if d.get('node_type') == 'weather']
    sales_data = [d for d in analyzed_data if d.get('node_type') in ['store', 'day_store', 'sbu_store']]
    
    # Add weather correlation analysis
    for sales_point in sales_data:
        sales_point['has_weather_correlation'] = len(weather_data) > 0
            """,
            
            'temporal_analysis': """
    # Sort data by date for temporal analysis
    analyzed_data.sort(key=lambda x: x.get('node_id', ''))
    
    # Add temporal metadata
    for data_point in analyzed_data:
        node_id = data_point.get('node_id', '')
        if len(node_id) >= 8:  # YYYYMMDD format
            data_point['date_extracted'] = node_id[:8]
            """,
            
            'store_performance': """
    # Group by store and calculate aggregations
    store_aggregations = {}
    for data_point in analyzed_data:
        store_id = data_point.get('st_cd', 'unknown')
        if store_id not in store_aggregations:
            store_aggregations[store_id] = []
        store_aggregations[store_id].append(data_point)
    
    # Add store performance metrics
    for data_point in analyzed_data:
        store_id = data_point.get('st_cd', 'unknown')
        data_point['store_record_count'] = len(store_aggregations.get(store_id, []))
            """,
            
            'sbu_analysis': """
    # Group by SBU for comparison
    sbu_data = {}
    for data_point in analyzed_data:
        node_id = data_point.get('node_id', '')
        sbu = 'UNKNOWN'
        if 'FOOD' in node_id:
            sbu = 'FOOD'
        elif 'HOME' in node_id:
            sbu = 'HOME'
        
        if sbu not in sbu_data:
            sbu_data[sbu] = []
        sbu_data[sbu].append(data_point)
        data_point['sbu_category'] = sbu
            """,
            
            'department_analysis': """
    # Extract department information
    for data_point in analyzed_data:
        node_id = data_point.get('node_id', '')
        # Extract department from node_id pattern
        parts = node_id.split('-')
        if len(parts) >= 3:
            data_point['department'] = parts[2]
        else:
            data_point['department'] = 'Total'
            """
        }
        
        return pattern_code.get(pattern, "# No specific pattern processing needed")
    
    def validate_and_fix_code(self, code: str) -> str:
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
        
        fixed_lines = lines[start_idx:]
        
        # Fix the ending - replace standalone 'results' with print statement
        if fixed_lines and fixed_lines[-1].strip() == 'results':
            fixed_lines[-1] = 'print(json.dumps(results))'
        elif fixed_lines and not any('print(json.dumps(results))' in line for line in fixed_lines):
            # Add print statement if missing
            fixed_lines.append('print(json.dumps(results))')
        
        return '\n'.join(fixed_lines)
# release_agent/kg_query_agent.py

import ast
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.adk.agents import Agent

from .code_formatter import CodeFormatter
from .code_generator import KGCodeGenerator
from .file_manager import KGFileManager
from .llm_model import llm_model
from .query_analyzer import QueryAnalyzer
from .result_formatter import ResultFormatter
from .schema_manager import KGSchemaManager
from .secure_executor import SecureCodeExecutor

logger = logging.getLogger(__name__)


class KGQueryAgent(Agent):
    """
    Specialized agent for processing natural language queries against Knowledge Graphs.

    This agent coordinates multiple components to:
    1. Analyze user queries to understand intent
    2. Generate Python code to query KG files
    3. Execute code safely in a sandbox
    4. Format results for frontend consumption
    """

    def __init__(self):
        super().__init__(
            name="kg_query_agent",
            description="Agent specialized in dynamic knowledge graph querying using LLM-generated code",
        )
        # Use object.__setattr__ to bypass Pydantic validation
        object.__setattr__(self, "llm", llm_model)
        object.__setattr__(self, "executor", SecureCodeExecutor())
        object.__setattr__(self, "schema_manager", KGSchemaManager())
        object.__setattr__(
            self, "query_analyzer", QueryAnalyzer(llm_model, self.schema_manager)
        )
        object.__setattr__(
            self, "code_generator", KGCodeGenerator(llm_model, self.schema_manager)
        )
        object.__setattr__(self, "result_formatter", ResultFormatter())
        object.__setattr__(self, "file_manager", KGFileManager())
        object.__setattr__(self, "code_formatter", CodeFormatter())

    @property
    def kg_schema(self) -> Dict[str, Any]:
        """Access to the KG schema via schema manager."""
        return self.schema_manager.schema

    async def process_query(
        self,
        query: str,
        kg_path: str = "Data/KGs",
        date_range: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
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
            query_analysis = self.query_analyzer.analyze_query(query)

            # Step 2: Determine which KG files to use
            target_files = self.file_manager.determine_target_files(
                query_analysis, kg_path, date_range
            )

            # Step 3: Generate Python code to query the KG
            generated_code = self.code_generator.generate_query_code(
                query, query_analysis, target_files
            )

            # Step 3.5: Format and validate the generated code
            formatted_code, is_valid, format_errors = (
                self.code_formatter.format_and_validate(generated_code)
            )

            if not is_valid:
                logger.warning(f"Generated code has syntax errors: {format_errors}")
                logger.info("Using fallback code template")
                formatted_code = self.code_formatter.generate_fallback_code(
                    query, target_files
                )

            logger.info(
                f"Using {'validated' if is_valid else 'fallback'} code for execution"
            )

            # Step 4: Execute the validated code safely
            execution_result = await self._execute_code_safely(formatted_code, kg_path)

            # Step 5: Format results for frontend
            formatted_data = self.result_formatter.format_results(
                execution_result, query_analysis
            )

            # Step 6: Generate insights
            insights = self.result_formatter.generate_insights(formatted_data, query)

            return {
                "data": formatted_data,
                "generated_code": formatted_code,  # Return the validated/formatted code
                "original_code": generated_code,  # Keep original for debugging
                "code_validation": {
                    "is_valid": is_valid,
                    "errors": format_errors,
                    "used_fallback": not is_valid,
                },
                "query_type": query_analysis["type"],
                "insights": insights,
                "target_files": target_files,
                "execution_success": execution_result.get("success", False),
            }

        except Exception as e:
            logger.error(f"Error processing query '{query}': {e!s}")
            raise

    def _extract_dates_regex(self, query: str) -> Optional[List[str]]:
        """Extract dates from query using regex patterns."""
        try:
            from dateutil.relativedelta import relativedelta
        except ImportError:
            # Fallback if dateutil not available
            logger.warning("dateutil not available, using basic date extraction")
            return self._extract_dates_basic(query)

        query_lower = query.lower()
        current_date = datetime.now()
        extracted_dates = []

        # Pattern 1: Specific months and years (January 2022, Jan 2023, etc.)
        month_year_pattern = r"(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})"
        month_matches = re.findall(month_year_pattern, query_lower)

        month_map = {
            "january": 1,
            "jan": 1,
            "february": 2,
            "feb": 2,
            "march": 3,
            "mar": 3,
            "april": 4,
            "apr": 4,
            "may": 5,
            "june": 6,
            "jun": 6,
            "july": 7,
            "jul": 7,
            "august": 8,
            "aug": 8,
            "september": 9,
            "sep": 9,
            "october": 10,
            "oct": 10,
            "november": 11,
            "nov": 11,
            "december": 12,
            "dec": 12,
        }

        for month_name, year in month_matches:
            if month_name in month_map:
                month_num = month_map[month_name]
                extracted_dates.append(f"{year}{month_num:02d}")

        # Pattern 2: Quarter patterns (Q1 2023, first quarter 2022, etc.)
        quarter_pattern = r"(q[1-4]|first quarter|second quarter|third quarter|fourth quarter)\s+(\d{4})"
        quarter_matches = re.findall(quarter_pattern, query_lower)

        quarter_map = {
            "q1": [1, 2, 3],
            "first quarter": [1, 2, 3],
            "q2": [4, 5, 6],
            "second quarter": [4, 5, 6],
            "q3": [7, 8, 9],
            "third quarter": [7, 8, 9],
            "q4": [10, 11, 12],
            "fourth quarter": [10, 11, 12],
        }

        for quarter, year in quarter_matches:
            if quarter in quarter_map:
                for month in quarter_map[quarter]:
                    extracted_dates.append(f"{year}{month:02d}")

        # Pattern 3: Year patterns (2022, year 2023, etc.)
        year_pattern = r"(?:year\s+)?(\d{4})(?!\d)"
        year_matches = re.findall(year_pattern, query_lower)

        # Only add full year if no specific months/quarters found
        if not extracted_dates and year_matches:
            for year in year_matches:
                if 2020 <= int(year) <= current_date.year + 1:  # Reasonable year range
                    for month in range(1, 13):
                        extracted_dates.append(f"{year}{month:02d}")

        # Pattern 4: Relative dates
        if "last" in query_lower:
            # Last X months
            last_months_pattern = r"last\s+(\d+)\s+months?"
            last_matches = re.findall(last_months_pattern, query_lower)
            if last_matches:
                months_back = int(last_matches[0])
                for i in range(months_back):
                    date = current_date - relativedelta(months=i)
                    extracted_dates.append(f"{date.year}{date.month:02d}")

            # Last quarter, last year, etc.
            elif "last quarter" in query_lower:
                # Get previous quarter
                current_quarter = (current_date.month - 1) // 3 + 1
                if current_quarter == 1:
                    prev_quarter_months = [10, 11, 12]
                    year = current_date.year - 1
                else:
                    prev_quarter_months = [
                        (current_quarter - 2) * 3 + i for i in [1, 2, 3]
                    ]
                    year = current_date.year

                for month in prev_quarter_months:
                    extracted_dates.append(f"{year}{month:02d}")

            elif "last year" in query_lower:
                year = current_date.year - 1
                for month in range(1, 13):
                    extracted_dates.append(f"{year}{month:02d}")

        # Pattern 5: Hurricane/Storm specific dates
        hurricane_dates = {
            "hurricane ian": [
                "202209",
                "202208",
            ],  # September 2022 + before for comparison
            "hurricane ida": ["202108", "202107"],  # August 2021
            "hurricane laura": ["202008", "202007"],  # August 2020
            "hurricane harvey": ["201708", "201707"],  # August 2017
        }

        for hurricane, dates in hurricane_dates.items():
            if hurricane in query_lower:
                extracted_dates.extend(dates)
                break

        # Pattern 6: Month ranges (March to June, etc.)
        range_pattern = r"(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+to\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(?:\s+(\d{4}))?"
        range_matches = re.findall(range_pattern, query_lower)

        for start_month, end_month, year in range_matches:
            year = year or str(current_date.year)
            start_num = month_map.get(start_month, 1)
            end_num = month_map.get(end_month, 12)

            if start_num <= end_num:
                for month in range(start_num, end_num + 1):
                    extracted_dates.append(f"{year}{month:02d}")

        # Remove duplicates and sort
        if extracted_dates:
            extracted_dates = sorted(list(set(extracted_dates)))
            return extracted_dates

        return None

    def _extract_dates_basic(self, query: str) -> Optional[List[str]]:
        """Basic date extraction without dateutil dependency."""

        query_lower = query.lower()
        current_date = datetime.now()
        extracted_dates = []

        # Basic month/year extraction
        month_year_pattern = r"(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})"
        month_matches = re.findall(month_year_pattern, query_lower)

        month_map = {
            "january": 1,
            "jan": 1,
            "february": 2,
            "feb": 2,
            "march": 3,
            "mar": 3,
            "april": 4,
            "apr": 4,
            "may": 5,
            "june": 6,
            "jun": 6,
            "july": 7,
            "jul": 7,
            "august": 8,
            "aug": 8,
            "september": 9,
            "sep": 9,
            "october": 10,
            "oct": 10,
            "november": 11,
            "nov": 11,
            "december": 12,
            "dec": 12,
        }

        for month_name, year in month_matches:
            if month_name in month_map:
                month_num = month_map[month_name]
                extracted_dates.append(f"{year}{month_num:02d}")

        # Basic relative dates without dateutil
        if "last" in query_lower:
            if "last year" in query_lower:
                year = current_date.year - 1
                for month in range(1, 13):
                    extracted_dates.append(f"{year}{month:02d}")

        # Hurricane dates
        hurricane_dates = {
            "hurricane ian": ["202209", "202208"],
            "hurricane ida": ["202108", "202107"],
        }

        for hurricane, dates in hurricane_dates.items():
            if hurricane in query_lower:
                extracted_dates.extend(dates)
                break

        if extracted_dates:
            return sorted(list(set(extracted_dates)))

        return None

    def _validate_date_range(self, date_range: List[str]) -> List[str]:
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

    def _determine_target_files(
        self,
        query_analysis: Dict[str, Any],
        kg_path: str,
        date_range: Optional[List[str]] = None,
    ) -> List[str]:
        """Determine which KG files to load based on query analysis."""

        # Priority 1: Use provided date_range parameter (from API call)
        if date_range:
            return [
                f"{kg_path}/{date}.json"
                for date in date_range
                if os.path.exists(f"{kg_path}/{date}.json")
            ]

        # Priority 2: Use extracted date range from query analysis
        extracted_dates = query_analysis.get("extracted_date_range")
        if extracted_dates:
            target_files = []
            for date in extracted_dates:
                file_path = f"{kg_path}/{date}.json"
                if os.path.exists(file_path):
                    target_files.append(file_path)

            if target_files:
                logger.info(f"Using extracted date range: {extracted_dates}")
                return target_files
            logger.warning(f"No KG files found for extracted dates: {extracted_dates}")

        # Priority 3: Auto-determine based on query type (fallback)
        available_files = []
        if os.path.exists(kg_path):
            for file in os.listdir(kg_path):
                if file.endswith(".json") and len(file) == 11:  # YYYYMM.json format
                    available_files.append(os.path.join(kg_path, file))

        available_files.sort()

        if not available_files:
            logger.warning(f"No KG files found in {kg_path}")
            return []

        # Select files based on analysis
        time_scope = query_analysis.get("time_scope", "single_month")

        if time_scope == "single_month":
            selected_files = available_files[-1:]  # Last 1 month
        elif time_scope == "multi_month":
            selected_files = available_files[-6:]  # Last 6 months
        elif time_scope == "year_over_year":
            selected_files = available_files[-24:]  # Last 2 years for comparison
        else:
            selected_files = available_files[-12:]  # Default: last year

        logger.info(
            f"Auto-selected {len(selected_files)} files based on time_scope: {time_scope}"
        )
        return selected_files

    def _generate_kg_query_code(
        self, query: str, analysis: Dict[str, Any], target_files: List[str]
    ) -> str:
        """Generate Python code to query the knowledge graph based on analysis."""

        code_prompt = f"""
        Generate Python code to query knowledge graphs for this analysis:
        
        Query: "{query}"
        Analysis: {json.dumps(analysis, indent=2)}
        Target files: {target_files}
        
        Complete KG Schema:
        {json.dumps(self.kg_schema, indent=2)}
        
        Requirements:
        1. Load the NetworkX graphs from JSON files
        2. Query the graph structure according to the hierarchy and node types
        3. Focus on node types: {analysis.get('target_node_types', [])}
        4. Use query pattern: {analysis.get('query_pattern', 'general')}
        5. Extract relevant data based on the query
        6. Return results in a standardized format
        
        Available imports: json, networkx as nx, pandas as pd, numpy as np, datetime
        
        Node ID patterns to use:
        {json.dumps(self.kg_schema['node_id_patterns'], indent=2)}
        
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
        code_match = re.search(r"```python\n(.*?)\n```", generated_code, re.DOTALL)
        if code_match:
            return code_match.group(1)
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
        lines = code.split("\n")
        start_idx = 0
        for i, line in enumerate(lines):
            if line.strip().startswith(("import ", "from ", "results =", "#")):
                start_idx = i
                break

        fixed_lines = lines[start_idx:]

        # Fix the ending - replace standalone 'results' with print statement
        if fixed_lines and fixed_lines[-1].strip() == "results":
            fixed_lines[-1] = "print(json.dumps(results))"
        elif fixed_lines and not any(
            "print(json.dumps(results))" in line for line in fixed_lines
        ):
            # Add print statement if missing
            fixed_lines.append("print(json.dumps(results))")

        return "\n".join(fixed_lines)

    async def _execute_code_safely(self, code: str, kg_path: str) -> Dict[str, Any]:
        """Execute the generated code in a secure environment."""
        try:
            logger.info(f"Executing code in path: {kg_path}")
            logger.info(f"Code ends with: {code.split('\n')[-3:]}")  # Last 3 lines

            result = await self.executor.execute(code, working_directory=kg_path)

            print(f"Execution result: {result}")
            logger.info(f"Execution result keys: {result.keys()}")
            logger.info(f"Execution success: {result.get('success')}")
            logger.info(f"Stdout length: {len(result.get('stdout', ''))}")
            logger.info(f"Stderr: {result.get('stderr', '')}")

            return result
        except Exception as e:
            logger.error(f"Code execution failed: {e}")
            return {"success": False, "error": str(e)}

    def _format_results(
        self, execution_result: Dict[str, Any], query_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Format execution results for frontend consumption."""
        if not execution_result.get("success"):
            return {"error": execution_result.get("error", "Execution failed")}

        raw_results = execution_result.get("result", {})

        # Structure the data based on query type
        formatted = {
            "query_type": query_analysis["type"],
            "data": raw_results.get("data", []),
            "metadata": raw_results.get("metadata", {}),
            "summary": raw_results.get("summary", {}),
            "timestamp": datetime.now().isoformat(),
            "schema_info": {
                "node_types_used": query_analysis.get("target_node_types", []),
                "query_pattern": query_analysis.get("query_pattern", "general"),
            },
        }

        return formatted

    def _generate_insights(
        self, formatted_data: Dict[str, Any], original_query: str
    ) -> List[str]:
        """Generate insights about the query results."""
        if "error" in formatted_data:
            return [f"Query execution failed: {formatted_data['error']}"]

        insights = []

        data = formatted_data.get("data", [])
        if isinstance(data, list) and len(data) > 0:
            insights.append(f"Found {len(data)} data points matching your query")

            # Add insights based on node types
            node_types = formatted_data.get("schema_info", {}).get(
                "node_types_used", []
            )
            if node_types:
                insights.append(
                    f"Analysis focused on: {', '.join(node_types)} level data"
                )

        summary = formatted_data.get("summary", {})
        if summary:
            for key, value in summary.items():
                insights.append(f"{key.replace('_', ' ').title()}: {value}")

        # Add query pattern insight
        query_pattern = formatted_data.get("schema_info", {}).get("query_pattern")
        if query_pattern and query_pattern != "general":
            insights.append(f"Query pattern used: {query_pattern}")

        if not insights:
            insights.append("Query completed successfully")

        return insights

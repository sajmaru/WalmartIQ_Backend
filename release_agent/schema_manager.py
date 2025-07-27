# release_agent/schema_manager.py

from typing import Dict, Any


class KGSchemaManager:
    """Manages the knowledge graph schema and provides schema-related utilities."""
    
    def __init__(self):
        self._schema = self._build_schema()
    
    @property
    def schema(self) -> Dict[str, Any]:
        """Get the complete KG schema."""
        return self._schema
    
    def _build_schema(self) -> Dict[str, Any]:
        """Build and return the complete KG schema definition."""
        return {
            "hierarchy": [
                "Month", 
                "Day", 
                "SBU", 
                "Department", 
                "SBU-Store",
                "Day-Store", 
                "Store", 
                "Weather"
            ],
            "node_types": {
                "month": {
                    "color": "#FFD700", 
                    "properties": ["month_id", "year"],
                    "example_id": "202201",
                    "description": "Monthly aggregation level"
                },
                "day": {
                    "color": "#FF8C00", 
                    "properties": ["day_id", "date"],
                    "example_id": "20220101",
                    "description": "Daily data points"
                },
                "sbu": {
                    "color": "#FF6F61", 
                    "properties": [
                        "daily_sbu_GMV_AMT", 
                        "daily_sbu_GMV_AMT_pred", 
                        "dept_count"
                    ],
                    "example_id": "20220101-FOOD-Total-Total",
                    "description": "Strategic Business Unit aggregation (FOOD, HOME)"
                },
                "dept": {
                    "color": "#F7CAC9", 
                    "properties": [
                        "dept_id", 
                        "dept_name", 
                        "daily_dept_GMV_AMT",
                        "daily_dept_GMV_AMT_pred"
                    ],
                    "example_id": "20220101-FOOD-Bakery-Total",
                    "description": "Department level within SBU"
                },
                "sbu_store": {
                    "color": "#92C5DE", 
                    "properties": [
                        "total_sales_unit", 
                        "total_gmv_amt", 
                        "st_cd",
                        "LAT_DGR", 
                        "LONG_DGR"
                    ],
                    "example_id": "20220101-FOOD-1001",
                    "description": "SBU performance at specific store level"
                },
                "day_store": {
                    "color": "#D4A574", 
                    "properties": [
                        "total_sales_unit", 
                        "total_gmv_amt", 
                        "st_cd",
                        "LAT_DGR", 
                        "LONG_DGR"
                    ],
                    "example_id": "20220101-Total-Total-1001",
                    "description": "Daily total performance at store level (across all SBUs)"
                },
                "store": {
                    "color": "#88B04B", 
                    "properties": [
                        "total_sales_unit", 
                        "total_gmv_amt", 
                        "dept_number",
                        "st_cd",
                        "LAT_DGR", 
                        "LONG_DGR"
                    ],
                    "example_id": "20220101-FOOD-Bakery-1001",
                    "description": "Specific department performance at store level"
                },
                "weather": {
                    "color": "#00A8E8", 
                    "properties": [
                        "AVG_AIR_TEMPR_DGR", 
                        "AVG_POS_DLY_SNOWFALL_QTY",
                        "AVG_POS_DLY_SNOW_DP_QTY",
                        "AVG_POS_PRECIP_QTY",
                        "LAT_DGR", 
                        "LONG_DGR"
                    ],
                    "example_id": "20220101-1001-Weather",
                    "description": "Weather conditions at store location"
                }
            },
            "relationships": [
                {
                    "source": "Month",
                    "target": "Day",
                    "label": "has day",
                    "description": "Month contains multiple days"
                },
                {
                    "source": "Day", 
                    "target": "SBU",
                    "label": "has sbu",
                    "description": "Day has SBU-level aggregations"
                },
                {
                    "source": "Day",
                    "target": "Day-Store", 
                    "label": "has day store",
                    "description": "Day has total store performance (all SBUs combined)"
                },
                {
                    "source": "SBU",
                    "target": "Department",
                    "label": "has department", 
                    "description": "SBU contains departments"
                },
                {
                    "source": "SBU",
                    "target": "SBU-Store",
                    "label": "has store",
                    "description": "SBU performance at individual stores"
                },
                {
                    "source": "Department",
                    "target": "Store",
                    "label": "has store",
                    "description": "Department performance at individual stores"
                },
                {
                    "source": "Day-Store",
                    "target": "Weather",
                    "label": "has weather",
                    "description": "Store location has weather conditions"
                }
            ],
            "query_patterns": {
                "sbu_analysis": {
                    "description": "Analysis at SBU level (FOOD vs HOME)",
                    "typical_nodes": ["sbu", "sbu_store"],
                    "example_queries": [
                        "Compare FOOD vs HOME sales",
                        "Which SBU performed better?",
                        "SBU trends over time"
                    ]
                },
                "store_performance": {
                    "description": "Store-level performance analysis",
                    "typical_nodes": ["store", "day_store", "sbu_store"],
                    "example_queries": [
                        "Top performing stores",
                        "Store sales by location",
                        "Individual store trends"
                    ]
                },
                "department_analysis": {
                    "description": "Department performance within SBUs",
                    "typical_nodes": ["dept", "store"],
                    "example_queries": [
                        "Best performing departments",
                        "Department sales by store",
                        "Category performance"
                    ]
                },
                "weather_impact": {
                    "description": "Weather correlation with sales",
                    "typical_nodes": ["weather", "day_store", "store"],
                    "example_queries": [
                        "Weather impact on sales",
                        "Temperature correlation",
                        "Storm effects on retail"
                    ]
                },
                "geographic_analysis": {
                    "description": "Location-based analysis",
                    "typical_nodes": ["store", "day_store", "sbu_store", "weather"],
                    "example_queries": [
                        "Sales by state/region",
                        "Geographic performance",
                        "Location trends"
                    ]
                },
                "temporal_analysis": {
                    "description": "Time-based trends and patterns",
                    "typical_nodes": ["month", "day", "sbu", "dept", "store"],
                    "example_queries": [
                        "Sales trends over time",
                        "Monthly comparisons", 
                        "Seasonal patterns"
                    ]
                }
            },
            "node_id_patterns": {
                "month": "YYYYMM (e.g., 202201)",
                "day": "YYYYMMDD (e.g., 20220101)",
                "sbu": "YYYYMMDD-{SBU}-Total-Total (e.g., 20220101-FOOD-Total-Total)",
                "dept": "YYYYMMDD-{SBU}-{DEPT_NAME}-Total (e.g., 20220101-FOOD-Bakery-Total)",
                "sbu_store": "YYYYMMDD-{SBU}-{STORE_ID} (e.g., 20220101-FOOD-1001)",
                "day_store": "YYYYMMDD-Total-Total-{STORE_ID} (e.g., 20220101-Total-Total-1001)",
                "store": "YYYYMMDD-{SBU}-{DEPT_NAME}-{STORE_ID} (e.g., 20220101-FOOD-Bakery-1001)",
                "weather": "YYYYMMDD-{STORE_ID}-Weather (e.g., 20220101-1001-Weather)"
            },
            "common_filters": {
                "sbu_values": ["FOOD", "HOME"],
                "geographic_properties": ["st_cd", "LAT_DGR", "LONG_DGR"],
                "sales_metrics": ["total_sales_unit", "total_gmv_amt", "daily_sbu_GMV_AMT", "daily_dept_GMV_AMT"],
                "weather_metrics": ["AVG_AIR_TEMPR_DGR", "AVG_POS_DLY_SNOWFALL_QTY", "AVG_POS_PRECIP_QTY"],
                "forecast_metrics": ["daily_sbu_GMV_AMT_pred", "daily_dept_GMV_AMT_pred"]
            }
        }
    
    def get_node_types_for_pattern(self, pattern: str) -> list:
        """Get typical node types for a query pattern."""
        pattern_info = self._schema["query_patterns"].get(pattern, {})
        return pattern_info.get("typical_nodes", [])
    
    def get_properties_for_node_type(self, node_type: str) -> list:
        """Get properties available for a specific node type."""
        node_info = self._schema["node_types"].get(node_type, {})
        return node_info.get("properties", [])
    
    def get_example_id_for_node_type(self, node_type: str) -> str:
        """Get example ID format for a node type."""
        node_info = self._schema["node_types"].get(node_type, {})
        return node_info.get("example_id", "")
    
    def validate_node_type(self, node_type: str) -> bool:
        """Check if a node type is valid."""
        return node_type in self._schema["node_types"]
    
    def get_common_filters(self, filter_category: str) -> list:
        """Get common filter values for a category."""
        return self._schema["common_filters"].get(filter_category, [])
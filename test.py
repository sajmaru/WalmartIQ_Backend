import json
import networkx as nx
import pandas as pd
import numpy as np
from datetime import datetime

# Results dictionary to return
results = {
    "data": [],
    "metadata": {},
    "summary": {}
}

try:
    # ---- Step 1: Load KG Files ----
    graphs = []
    for file_path in ["Data/KGs/202201.json"]:
        with open(file_path, "r") as f:
            kg_data = json.load(f)
            kg = nx.node_link_graph(kg_data)
            graphs.append((file_path, kg))

    # ---- Step 2: Initialization ----
    # We aggregate sales (total_gmv_amt) for FOOD SBU in Florida for 202201
    target_sbu = "FOOD"
    target_month = "202201"
    target_state = "FL"
    target_node_types = ["sbu_store"]

    # Optional: Store additional context if needed
    matched_nodes = []

    # ---- Step 3: Main Graph Traversal and Filtering ----
    for file_path, kg in graphs:
        for node_id, node_attrs in kg.nodes(data=True):
            node_type = node_attrs.get("node_type", "").lower()

            if node_type == "sbu_store":
                # sbu_store IDs: YYYYMMDD-{SBU}-{STORE_ID}
                # We'll need to check SBU and the date is in the target month
                node_id_parts = node_id.split("-")
                if len(node_id_parts) == 3:
                    day_str, sbu_str, store_id = node_id_parts

                    if sbu_str.upper() == target_sbu:
                        # Check if this node is in the target month
                        if day_str.startswith(target_month):
                            # Check state (st_cd) property
                            st_cd = node_attrs.get("st_cd", None)
                            if (
                                st_cd is not None
                                and str(st_cd).strip().lower() == target_state.lower()
                            ):
                                # Extract relevant sales info
                                data_point = {
                                    "node_id": node_id,
                                    "date": day_str,
                                    "sbu": sbu_str,
                                    "store_id": store_id,
                                    "state": st_cd,
                                    "total_sales_unit": node_attrs.get("total_sales_unit"),
                                    "total_gmv_amt": node_attrs.get("total_gmv_amt"),
                                    "LAT_DGR": node_attrs.get("LAT_DGR"),
                                    "LONG_DGR": node_attrs.get("LONG_DGR"),
                                    "node_type": node_type,
                                    "file_source": file_path,
                                }
                                matched_nodes.append(data_point)

    # ---- Step 4: Aggregate Results ----
    # Convert to DataFrame for easy aggregation
    df = pd.DataFrame(matched_nodes)

    total_gmv = (
        float(df["total_gmv_amt"].sum()) 
        if not df.empty and "total_gmv_amt" in df.columns 
        else 0.0
    )
    
    total_sales_units = (
        int(df["total_sales_unit"].sum()) 
        if not df.empty and "total_sales_unit" in df.columns 
        else 0
    )

    # Assign results to return
    results["data"] = matched_nodes
    results["metadata"] = {
        "query_type": "temporal_analysis",
        "file_count": len(graphs),
        "target_node_types": ["sbu_store"],
        "query_pattern": "geographic_analysis",
        "filtered_sbu": target_sbu,
        "filtered_month": target_month,
        "filtered_state": target_state,
    }

    results["summary"] = {
        "total_records": len(matched_nodes),
        "total_gmv_amt": total_gmv,
        "total_sales_unit": total_sales_units,
    }

except Exception as e:
    results["error"] = str(e)

# Print results as JSON for capture by executor
print(json.dumps(results))

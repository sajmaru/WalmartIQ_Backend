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
    # Step 1: Load KG files
    graphs = []
    for file_path in ["Data/KGs/202201.json"]:
        with open(file_path, "r") as f:
            kg_data = json.load(f)
            kg = nx.node_link_graph(kg_data)
            graphs.append((file_path, kg))

    # Step 2: Set query-specific parameters
    # For query: "WHAT is the same for FOOD on 2022 Jan 01"
    # Target is SBU nodes for FOOD, for 2022-01-01, i.e., 20220101-FOOD-Total-Total
    # Node type: 'sbu'
    target_date = "20220101"
    sbu_value = "FOOD"
    target_sbu_node_id = f"{target_date}-{sbu_value}-Total-Total"
    target_node_types = ["sbu"]

    analyzed_data = []

    # Step 3: Process each loaded graph
    for file_path, kg in graphs:
        # We look for sbu node with node_id matching target_sbu_node_id
        node_attrs = kg.nodes.get(target_sbu_node_id)
        if node_attrs and node_attrs.get("node_type") == "sbu":
            # Collect only the relevant sales/GVMA data
            data_point = {
                "node_id": target_sbu_node_id,
                "node_type": "sbu",
                "file_source": file_path,
            }

            # Extract relevant SBU properties
            # According to schema: daily_sbu_GMV_AMT, daily_sbu_GMV_AMT_pred, dept_count
            data_point["daily_sbu_GMV_AMT"] = node_attrs.get("daily_sbu_GMV_AMT")
            data_point["daily_sbu_GMV_AMT_pred"] = node_attrs.get("daily_sbu_GMV_AMT_pred")
            data_point["dept_count"] = node_attrs.get("dept_count")

            # Optional: add _all_ other attributes if they exist
            for k, v in node_attrs.items():
                if k not in data_point:
                    data_point[k] = v

            analyzed_data.append(data_point)

    # Step 4: Prepare results
    results["data"] = analyzed_data
    results["metadata"] = {
        "query_type": "temporal_analysis",
        "file_count": len(graphs),
        "target_node_types": target_node_types,
        "query_pattern": "sbu_analysis",
        "queried_date": target_date,
        "queried_sbu": sbu_value,
    }
    results["summary"] = {
        "total_records": len(analyzed_data),
        "matched_node_id": target_sbu_node_id,
    }

    # Warning if no results were found
    if len(analyzed_data) == 0:
        results["warning"] = (
            f"No SBU node found for SBU={sbu_value} on date={target_date} "
            f"in {file_path}. Check data coverage or spelling."
        )

except Exception as e:
    results["error"] = str(e)

# Print results as JSON for capture by executor
print(json.dumps(results))

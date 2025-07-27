# Simplified Monthly Store-Weather Knowledge Graph Creation

import networkx as nx
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
import json
import numpy as np
import multiprocessing as mp
import os

# Color sets for nodes
set_colors = {
    'month': '#FFD700',
    'day': '#FF8C00',
    'sbu': '#FF6F61',
    'dept': '#F7CAC9',
    'sbu_store': '#92C5DE',  # Light Blue
    'store': '#88B04B',
    'day_store': '#D4A574',  # Light Brown
    'weather': '#00A8E8',
}

def find_store_id_column(df, preferred_names=['SRC_STORE_ID', 'STORE_ID', 'store_id']):
    """Find the store ID column in a DataFrame"""
    for col_name in preferred_names:
        if col_name in df.columns:
            return col_name
    
    # If none of the preferred names found, look for columns containing 'store'
    store_cols = [col for col in df.columns if 'store' in col.lower()]
    if store_cols:
        return store_cols[0]
    
    return None

def process_single_day(args):
    """Process a single day's data - designed for multiprocessing"""
    day_str, day_sales_data, day_weather_data, day_dept_forecast_data, month = args
    
    # Convert data back to DataFrames
    day_sales = pd.DataFrame(day_sales_data) if day_sales_data else pd.DataFrame()
    day_weather = pd.DataFrame(day_weather_data) if day_weather_data else pd.DataFrame()
    day_dept_forecast = pd.DataFrame(day_dept_forecast_data) if day_dept_forecast_data else pd.DataFrame()
    
    # Convert dept_id to string in forecast data
    if not day_dept_forecast.empty:
        day_dept_forecast['dept_id'] = day_dept_forecast['dept_id'].astype(str)
    
    # Store nodes and edges for this day
    day_nodes = {}
    day_edges = []
    
    # Create day node
    day_node = day_str
    day_nodes[day_node] = {
        'label': day_node, 
        'color': set_colors['day'], 
        'node_type': 'day'
    }
    
    # Add edge from month to day
    day_edges.append((str(month), day_node, {'label': 'has day'}))
    
    # Track created nodes to avoid duplicates
    created_sbu_nodes = set()
    created_dept_nodes = set()
    created_sbu_store_nodes = set()
    created_day_store_nodes = set()
    created_weather_nodes = set()
    
    # Process SBU-level aggregation first for FOOD and HOME
    for sbu in ['FOOD', 'HOME']:
        sbu_node = f"{day_str}-{sbu}-Total-Total"
        
        if sbu_node not in created_sbu_nodes:
            # Find SBU-level aggregate data from dept forecast
            sbu_total_data = day_dept_forecast[
                (day_dept_forecast['sbu'] == sbu) & 
                (day_dept_forecast['dept_id'] == 'Total')
            ]
            
            if not sbu_total_data.empty:
                sbu_row = sbu_total_data.iloc[0]
                sbu_gmv_amt = sbu_row.get('GMV_AMT', np.nan)
                sbu_gmv_pred = sbu_row.get('GMV_AMT_pred', np.nan)
                dept_count = len(day_dept_forecast[
                    (day_dept_forecast['sbu'] == sbu) & 
                    (day_dept_forecast['dept_id'] != 'Total')
                ])
            else:
                sbu_gmv_amt = np.nan
                sbu_gmv_pred = np.nan
                dept_count = 0
            
            sbu_info = {
                'daily_sbu_GMV_AMT': sbu_gmv_amt,
                'daily_sbu_GMV_AMT_pred': sbu_gmv_pred,
                'dept_count': dept_count
            }
            
            day_nodes[sbu_node] = {
                'label': sbu_node, 
                'color': set_colors['sbu'], 
                'node_type': 'sbu', 
                **sbu_info
            }
            day_edges.append((day_node, sbu_node, {'label': 'has sbu'}))
            created_sbu_nodes.add(sbu_node)
    
    # Process Day-Store nodes (sbu='Total', ACCTG_DEPT_NBR='Total')
    day_store_sales = day_sales[
        (day_sales['sbu'] == 'Total') & 
        (day_sales['ACCTG_DEPT_NBR'] == 'Total')
    ]
    
    for _, day_store_row in day_store_sales.iterrows():
        # Extract store ID
        full_store_id = str(day_store_row['SRC_STORE_ID'])
        if '-' in full_store_id:
            store_id = full_store_id.split('-')[-1]
        else:
            store_id = full_store_id
        
        day_store_node = f"{day_str}-Total-Total-{store_id}"
        
        if day_store_node not in created_day_store_nodes:
            day_store_info = {
                'total_sales_unit': day_store_row.get('total_sales_unit', np.nan),
                'total_gmv_amt': day_store_row.get('total_gmv_amt', np.nan),
                'st_cd': day_store_row.get('ST_CD', np.nan),
                'LAT_DGR': day_store_row.get('LAT_DGR', np.nan),
                'LONG_DGR': day_store_row.get('LONG_DGR', np.nan)
            }
            
            day_nodes[day_store_node] = {
                'label': day_store_node,
                'color': set_colors['day_store'],
                'node_type': 'day_store',
                **day_store_info
            }
            day_edges.append((day_node, day_store_node, {'label': 'has day store'}))
            created_day_store_nodes.add(day_store_node)
            
            # Create Weather node connected to Day-Store
            weather_node = f"{day_str}-{store_id}-Weather"
            
            if weather_node not in created_weather_nodes:

                # Find corresponding weather data - automatically detect store column
                weather_data = pd.DataFrame()
                if not day_weather.empty:
                    weather_store_col = find_store_id_column(day_weather)
                    if weather_store_col:
                        weather_data = day_weather[day_weather[weather_store_col] == day_store_row['SRC_STORE_ID']]
                    else:
                        print(f"Warning: No store ID column found in weather data. Available columns: {list(day_weather.columns)}")
                        weather_data = pd.DataFrame()
                
                if not weather_data.empty:
                    weather_row = weather_data.iloc[0]
                    weather_info = {
                        'AVG_AIR_TEMPR_DGR': weather_row.get('AVG_AIR_TEMPR_DGR', np.nan),
                        'AVG_POS_DLY_SNOWFALL_QTY': weather_row.get('AVG_POS_DLY_SNOWFALL_QTY', np.nan),
                        'AVG_POS_DLY_SNOW_DP_QTY': weather_row.get('AVG_POS_DLY_SNOW_DP_QTY', np.nan),
                        'AVG_POS_PRECIP_QTY': weather_row.get('AVG_POS_PRECIP_QTY', np.nan),
                        'LAT_DGR': weather_row.get('LAT_DGR', np.nan),
                        'LONG_DGR': weather_row.get('LONG_DGR', np.nan)
                    }
                else:
                    weather_info = {
                        'AVG_AIR_TEMPR_DGR': np.nan,
                        'AVG_POS_DLY_SNOWFALL_QTY': np.nan,
                        'AVG_POS_DLY_SNOW_DP_QTY': np.nan,
                        'AVG_POS_PRECIP_QTY': np.nan,
                        'LAT_DGR': np.nan,
                        'LONG_DGR': np.nan
                    }
                
                day_nodes[weather_node] = {
                    'label': weather_node,
                    'color': set_colors['weather'],
                    'node_type': 'weather',
                    **weather_info
                }
                day_edges.append((day_store_node, weather_node, {'label': 'has weather'}))
                created_weather_nodes.add(weather_node)
    
    # Process SBU-Store nodes (sbu in ['FOOD', 'HOME'], ACCTG_DEPT_NBR='Total')
    for sbu in ['FOOD', 'HOME']:
        sbu_store_sales = day_sales[
            (day_sales['sbu'] == sbu) & 
            (day_sales['ACCTG_DEPT_NBR'] == 'Total')
        ]
        
        for _, sbu_store_row in sbu_store_sales.iterrows():
            # Extract store ID
            full_store_id = str(sbu_store_row['SRC_STORE_ID'])
            if '-' in full_store_id:
                store_id = full_store_id.split('-')[-1]
            else:
                store_id = full_store_id
            
            sbu_node = f"{day_str}-{sbu}-Total-Total"
            sbu_store_node = f"{day_str}-{sbu}-{store_id}"
            
            if sbu_store_node not in created_sbu_store_nodes:
                sbu_store_info = {
                    'total_sales_unit': sbu_store_row.get('total_sales_unit', np.nan),
                    'total_gmv_amt': sbu_store_row.get('total_gmv_amt', np.nan),
                    'st_cd': sbu_store_row.get('ST_CD', np.nan),
                    'LAT_DGR': sbu_store_row.get('LAT_DGR', np.nan),
                    'LONG_DGR': sbu_store_row.get('LONG_DGR', np.nan)
                }
                
                day_nodes[sbu_store_node] = {
                    'label': sbu_store_node,
                    'color': set_colors['sbu_store'],
                    'node_type': 'sbu_store',
                    **sbu_store_info
                }
                day_edges.append((sbu_node, sbu_store_node, {'label': 'has store'}))
                created_sbu_store_nodes.add(sbu_store_node)
    
    # Process Department and Store nodes (sbu in ['FOOD', 'HOME'], ACCTG_DEPT_NBR != 'Total')
    dept_store_sales = day_sales[
        (day_sales['sbu'].isin(['FOOD', 'HOME'])) & 
        (day_sales['ACCTG_DEPT_NBR'] != 'Total')
    ]
    
    for _, sale_row in dept_store_sales.iterrows():
        sbu = sale_row['sbu']
        dept_name = sale_row['dept_name']
        
        # Extract store ID
        full_store_id = str(sale_row['SRC_STORE_ID'])
        if '-' in full_store_id:
            store_id = full_store_id.split('-')[-1]
        else:
            store_id = full_store_id
        
        dept_id = str(sale_row.get('ACCTG_DEPT_NBR', ''))
        
        # Create node IDs
        sbu_node = f"{day_str}-{sbu}-Total-Total"
        dept_node = f"{day_str}-{sbu}-{dept_name}-Total"
        store_node = f"{day_str}-{sbu}-{dept_name}-{store_id}"
        
        # Create Department node if not already created
        if dept_node not in created_dept_nodes:
            # Find department forecast data
            dept_forecast_data = day_dept_forecast[
                (day_dept_forecast['sbu'] == sbu) & 
                (day_dept_forecast['dept_id'] == dept_id) &
                (day_dept_forecast['dept_id'] != 'Total')
            ]
            
            if not dept_forecast_data.empty:
                dept_forecast_row = dept_forecast_data.iloc[0]
                dept_gmv_amt = dept_forecast_row.get('GMV_AMT', np.nan)
                dept_gmv_pred = dept_forecast_row.get('GMV_AMT_pred', np.nan)
            else:
                dept_gmv_amt = np.nan
                dept_gmv_pred = np.nan
            
            dept_info = {
                'daily_dept_GMV_AMT': dept_gmv_amt,
                'daily_dept_GMV_AMT_pred': dept_gmv_pred,
                'dept_id': dept_id,
                'dept_name': dept_name
            }
            
            day_nodes[dept_node] = {
                'label': dept_node, 
                'color': set_colors['dept'],
                'node_type': 'dept', 
                **dept_info
            }
            day_edges.append((sbu_node, dept_node, {'label': 'has department'}))
            created_dept_nodes.add(dept_node)
        
        # Create Store node with sales data
        store_info = {
            'total_sales_unit': sale_row.get('total_sales_unit', np.nan),
            'total_gmv_amt': sale_row.get('total_gmv_amt', np.nan),
            'dept_number': sale_row.get('ACCTG_DEPT_NBR', np.nan),
            'st_cd': sale_row.get('ST_CD', np.nan),
            'LAT_DGR': sale_row.get('LAT_DGR', np.nan),
            'LONG_DGR': sale_row.get('LONG_DGR', np.nan)
        }
        
        day_nodes[store_node] = {
            'label': store_node, 
            'color': set_colors['store'],
            'node_type': 'store', 
            **store_info
        }
        day_edges.append((dept_node, store_node, {'label': 'has store'}))
    
    return {
        'day': day_str,
        'nodes': day_nodes,
        'edges': day_edges
    }

def create_monthly_store_weather_kg(store_sales_df, weather_df, dept_forecast_df, year, month, n_processes=None):
    """Create monthly store-weather knowledge graph"""
    
    month_str = f"{year:04d}{month:02d}"
    print(f"Creating monthly store-weather KG for {month_str}")
    
    # Debug: Print column names
    print(f"Sales columns: {list(store_sales_df.columns)}")
    print(f"Weather columns: {list(weather_df.columns)}")
    print(f"Forecast columns: {list(dept_forecast_df.columns)}")
    
    # Set number of processes
    if n_processes is None:
        n_processes = max(1, mp.cpu_count() - 1)
    print(f"Using {n_processes} processes")
    
    # Ensure date columns are datetime
    for df, col in [(store_sales_df, 'EVENT_DT'), (weather_df, 'OBSRVTN_DT'), (dept_forecast_df, 'ds')]:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col])
    
    # Filter data for the specific month
    month_sales = store_sales_df[
        (store_sales_df['EVENT_DT'].dt.year == year) & 
        (store_sales_df['EVENT_DT'].dt.month == month)
    ].copy()
    
    month_weather = weather_df[
        (weather_df['OBSRVTN_DT'].dt.year == year) & 
        (weather_df['OBSRVTN_DT'].dt.month == month)
    ].copy()
    
    month_dept_forecast = dept_forecast_df[
        (dept_forecast_df['ds'].dt.year == year) &
        (dept_forecast_df['ds'].dt.month == month) &
        (dept_forecast_df['sbu'].isin(['FOOD', 'HOME']))
    ].copy()
    
    if month_sales.empty:
        print(f"No sales data found for month {month_str}")
        return nx.DiGraph()
    
    # Get unique days
    unique_days = month_sales['EVENT_DT'].dt.strftime('%Y%m%d').unique()
    print(f"Processing {len(unique_days)} unique days")
    
    # Prepare data for multiprocessing
    day_args = []
    for day_str in tqdm(unique_days, desc="Preparing day data"):
        day_sales = month_sales[month_sales['EVENT_DT'].dt.strftime('%Y%m%d') == day_str]
        day_weather = month_weather[month_weather['OBSRVTN_DT'].dt.strftime('%Y%m%d') == day_str]
        day_dept_forecast = month_dept_forecast[month_dept_forecast['ds'].dt.strftime('%Y%m%d') == day_str]
        
        day_args.append((
            day_str, 
            day_sales.to_dict('records'),
            day_weather.to_dict('records'),
            day_dept_forecast.to_dict('records'),
            month_str
        ))
    
    # Process days in parallel
    if n_processes == 1:
        results = [process_single_day(args) for args in tqdm(day_args, desc="Processing days")]
    else:
        try:
            with mp.Pool(processes=n_processes) as pool:
                results = list(tqdm(
                    pool.imap(process_single_day, day_args),
                    total=len(day_args),
                    desc=f"Processing days ({n_processes} processes)"
                ))
        except Exception as e:
            print(f"Multiprocessing failed: {e}, falling back to single process")
            results = [process_single_day(args) for args in tqdm(day_args, desc="Processing days")]
    
    # Initialize the knowledge graph
    kg = nx.DiGraph()
    
    # Create month node
    month_node = month_str
    kg.add_node(month_node, label=month_node, color=set_colors['month'], node_type='month')
    
    # Combine results into the knowledge graph
    for result in tqdm(results, desc="Assembling graph"):
        # Add all nodes from this day
        for node_id, node_attrs in result['nodes'].items():
            kg.add_node(node_id, **node_attrs)
        
        # Add all edges from this day
        for source, target, edge_attrs in result['edges']:
            kg.add_edge(source, target, **edge_attrs)
    
    print(f"Monthly KG created for {month_str}: {kg.number_of_nodes()} nodes, {kg.number_of_edges()} edges")
    return kg

def save_kg_as_json(kg, filepath):
    """Save KG using NetworkX's node-link JSON format"""
    kg_data = nx.node_link_data(kg, edges="links")
    
    with open(filepath, 'w') as f:
        json.dump(kg_data, f, indent=2, default=str)
    
    print(f"KG saved to {filepath}")

def create_and_save_monthly_kgs(store_sales_files, weather_files, dept_forecast_file, years_to_process, n_processes=None):
    """Create and save knowledge graphs for multiple months"""
    
    # Create KGs folder
    os.makedirs('KGs', exist_ok=True)
    
    # Load department forecast data
    print(f"Loading department forecast data from: {dept_forecast_file}")
    dept_forecast_df = pd.read_csv(dept_forecast_file)
    
    for year in tqdm(years_to_process, desc="Processing years"):
        print(f"\nProcessing year: {year}")
        
        # Load sales and weather data
        sales_file = store_sales_files.get(year)
        weather_file = weather_files.get(year)
        
        if not sales_file or not weather_file:
            print(f"Missing files for year {year}")
            continue
            
        store_sales_df = pd.read_csv(sales_file)
        weather_df = pd.read_csv(weather_file)
        
        # Process each month
        for month in range(1, 13):
            month_str = f"{year:04d}{month:02d}"
            print(f"\nProcessing month: {month_str}")
            
            # Create KG for the month
            monthly_kg = create_monthly_store_weather_kg(
                store_sales_df, 
                weather_df, 
                dept_forecast_df,
                year,
                month, 
                n_processes
            )
            
            # Save if graph has content
            if monthly_kg.number_of_nodes() > 1:
                filepath = f"KGs/{month_str}.json"
                save_kg_as_json(monthly_kg, filepath)
                print(f"KG saved for {month_str}")
            else:
                print(f"No data found for month {month_str}")

# Usage example
if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    
    # Define file mappings
    store_sales_files = {
        2021: 'store_sales_mock/store_sales_mock_2021.csv',
        2022: 'store_sales_mock/store_sales_mock_2022.csv',
        2023: 'store_sales_mock/store_sales_mock_2023.csv',
        2024: 'store_sales_mock/store_sales_mock_2024.csv',
        2025: 'store_sales_mock/store_sales_mock_2025.csv'
    }
    
    weather_files = {
        2022: 'weather/weather_2022.csv',
        2023: 'weather/weather_2023.csv', 
        2024: 'weather/weather_2024.csv',
        2025: 'weather/weather_2025.csv'
    }
    
    dept_forecast_file = 'daily/daily_forecast_dept_level.csv'
    years_to_process = [2022, 2023, 2024, 2025]
    
    # Create and save all monthly KGs
    create_and_save_monthly_kgs(
        store_sales_files, 
        weather_files, 
        dept_forecast_file,
        years_to_process
    )
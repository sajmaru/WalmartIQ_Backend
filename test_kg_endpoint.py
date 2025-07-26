# test_kg_endpoint.py

import asyncio
import json
import os
import requests
import time
from pathlib import Path

# Test the KG Query Agent locally
async def test_kg_agent_locally():
    """Test the KG Query Agent directly without API."""
    print("🧪 Testing KG Query Agent locally...")
    
    try:
        from release_agent.kg_query_agent import KGQueryAgent
        
        # Initialize agent
        agent = KGQueryAgent()
        
        # Test queries
        test_queries = [
            "Show me total sales for electronics in January 2022",
            "What was the impact of weather on clothing sales?",
            "Compare SBU performance across different months",
            "Find stores with highest GMV in the system"
        ]
        
        for query in test_queries:
            print(f"\n📝 Testing query: '{query}'")
            
            try:
                result = await agent.process_query(
                    query=query,
                    kg_path="Data/KGs"
                )
                
                print(f"✅ Query processed successfully")
                print(f"   - Query type: {result.get('query_type')}")
                print(f"   - Target files: {len(result.get('target_files', []))}")
                print(f"   - Execution success: {result.get('execution_success')}")
                print(f"   - Data points: {len(result.get('data', {}).get('data', []))}")
                print(f"   - Insights: {len(result.get('insights', []))}")
                
                if result.get('data', {}).get('error'):
                    print(f"   ⚠️  Execution error: {result['data']['error']}")
                
            except Exception as e:
                print(f"❌ Query failed: {str(e)}")
        
        print("\n🧪 Testing secure executor...")
        test_result = await agent.executor.test_execution()
        print(f"Executor test result: {test_result.get('success', False)}")
        
    except Exception as e:
        print(f"❌ Local test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def test_api_endpoint():
    """Test the API endpoint via HTTP requests."""
    print("\n🌐 Testing API endpoint...")
    
    base_url = "http://localhost:8000"
    
    # Test health check
    try:
        response = requests.get(f"{base_url}/health")
        print(f"Health check: {response.status_code}")
        if response.status_code == 200:
            health_data = response.json()
            print(f"   Status: {health_data.get('status')}")
    except requests.exceptions.ConnectionError:
        print("❌ API server not running. Start with: python api.py")
        return
    
    # Test list KG files
    try:
        response = requests.get(f"{base_url}/kg-files")
        if response.status_code == 200:
            kg_files = response.json()
            print(f"📂 Found {kg_files.get('total_files', 0)} KG files")
            for file_info in kg_files.get('files', [])[:3]:
                print(f"   - {file_info['filename']} ({file_info['size_mb']} MB)")
        else:
            print(f"❌ Failed to list KG files: {response.status_code}")
    except Exception as e:
        print(f"❌ Error listing KG files: {e}")
    
    # Test KG queries
    test_queries = [
        {
            "query": "Show me total sales for electronics",
            "description": "Simple aggregation query"
        },
        {
            "query": "What was the weather impact on sales in Florida?",
            "description": "Complex analysis with weather correlation"
        },
        {
            "query": "Compare SBU performance over the last 3 months",
            "description": "Temporal comparison query"
        }
    ]
    
    for test_case in test_queries:
        print(f"\n📝 Testing: {test_case['description']}")
        print(f"   Query: '{test_case['query']}'")
        
        try:
            start_time = time.time()
            response = requests.post(
                f"{base_url}/kg-query",
                json={"query": test_case["query"]},
                timeout=60
            )
            
            execution_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Success in {execution_time:.2f}s")
                print(f"   - Query type: {result.get('query_type')}")
                print(f"   - Data available: {'Yes' if result.get('data') else 'No'}")
                print(f"   - Insights: {len(result.get('insights', []))}")
                
                if result.get('error'):
                    print(f"   ⚠️  Error: {result['error']}")
                
            else:
                print(f"❌ API Error {response.status_code}: {response.text}")
                
        except requests.exceptions.Timeout:
            print(f"⏱️  Request timed out after 60 seconds")
        except Exception as e:
            print(f"❌ Request failed: {str(e)}")

def create_sample_kg_file():
    """Create a sample KG file for testing if none exists."""
    kg_dir = Path("Data/KGs")
    kg_dir.mkdir(parents=True, exist_ok=True)
    
    sample_file = kg_dir / "202201.json"
    
    if not sample_file.exists():
        print("📁 Creating sample KG file for testing...")
        
        # Create a minimal sample KG structure
        sample_kg = {
            "directed": True,
            "multigraph": False,
            "graph": {},
            "nodes": [
                {
                    "id": "202201",
                    "label": "202201",
                    "color": "#FFD700",
                    "node_type": "month"
                },
                {
                    "id": "20220101",
                    "label": "20220101", 
                    "color": "#FF8C00",
                    "node_type": "day"
                },
                {
                    "id": "20220101-EL-Total-Total",
                    "label": "20220101-EL-Total-Total",
                    "color": "#FF6F61",
                    "node_type": "sbu",
                    "daily_sbu_GMV_AMT": 1500000.0,
                    "dept_count": 5
                },
                {
                    "id": "20220101-EL-Smartphones-Total",
                    "label": "20220101-EL-Smartphones-Total",
                    "color": "#F7CAC9", 
                    "node_type": "dept",
                    "daily_dept_GMV_AMT": 800000.0,
                    "dept_id": "EL_SM",
                    "dept_name": "Smartphones"
                },
                {
                    "id": "20220101-EL-Smartphones-1001",
                    "label": "20220101-EL-Smartphones-1001",
                    "color": "#88B04B",
                    "node_type": "store",
                    "total_sales_unit": 150.0,
                    "total_gmv_amt": 45000.0,
                    "LAT_DGR": 25.7617,
                    "LONG_DGR": -80.1918
                },
                {
                    "id": "20220101-1001-Weather",
                    "label": "20220101-1001-Weather",
                    "color": "#00A8E8",
                    "node_type": "weather",
                    "AVG_AIR_TEMPR_DGR": 75.2,
                    "AVG_POS_DLY_SNOWFALL_QTY": 0.0,
                    "AVG_POS_PRECIP_QTY": 0.1,
                    "LAT_DGR": 25.7617,
                    "LONG_DGR": -80.1918
                }
            ],
            "links": [
                {"source": "202201", "target": "20220101", "label": "has day"},
                {"source": "20220101", "target": "20220101-EL-Total-Total", "label": "has sbu"},
                {"source": "20220101-EL-Total-Total", "target": "20220101-EL-Smartphones-Total", "label": "has department"},
                {"source": "20220101-EL-Smartphones-Total", "target": "20220101-EL-Smartphones-1001", "label": "has store"},
                {"source": "20220101-EL-Smartphones-1001", "target": "20220101-1001-Weather", "label": "has weather"}
            ]
        }
        
        with open(sample_file, 'w') as f:
            json.dump(sample_kg, f, indent=2)
        
        print(f"✅ Created sample KG file: {sample_file}")

def main():
    """Run all tests."""
    print("🚀 Starting KG Query System Tests")
    print("=" * 50)
    
    # Create sample data if needed
    create_sample_kg_file()
    
    # Test locally first
    asyncio.run(test_kg_agent_locally())
    
    # Test API endpoint
    test_api_endpoint()
    
    print("\n" + "=" * 50)
    print("🏁 Test complete!")
    print("\nTo start the API server:")
    print("  python api.py")
    print("\nTo test a specific query:")
    print("  curl -X POST http://localhost:8000/kg-query \\")
    print("       -H 'Content-Type: application/json' \\")
    print("       -d '{\"query\": \"Show me electronics sales data\"}'")

if __name__ == "__main__":
    main()
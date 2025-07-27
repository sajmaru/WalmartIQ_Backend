from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uvicorn
import json
import os
import logging
from datetime import datetime
import traceback

from release_agent.agent import root_agent
from release_agent.test_agent import test_agent
from release_agent.kg_query_agent import KGQueryAgent  # We'll create this

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="KG Demo Release Agent API",
    description="API for dynamic knowledge graph querying using LLM-generated code",
    version="2.0.0"
)

# Initialize the KG Query Agent
kg_agent = KGQueryAgent()

class ChatRequest(BaseModel):
    """Request model for chat endpoint."""
    message: str
    agent_type: Optional[str] = "test"

class DynamicKGRequest(BaseModel):
    """Request model for dynamic KG query endpoint."""
    query: str
    kg_path: Optional[str] = "Data/KGs"  # Base path to KG files
    context: Optional[Dict[str, Any]] = None  # Additional context

class DynamicKGResponse(BaseModel):
    """Response model for dynamic KG query."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    generated_code: Optional[str] = None
    execution_time: Optional[float] = None
    insights: Optional[List[str]] = None
    error: Optional[str] = None
    query_type: Optional[str] = None

class ChatResponse(BaseModel):
    """Response model for chat endpoint."""
    response: str
    agent_used: str
    status: str

class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    message: str
    agent_name: str
    llm_response: Optional[str] = None
    error: Optional[str] = None

@app.get("/")
async def root():
    """Root endpoint with basic info."""
    return {
        "message": "KG Demo Release Agent API",
        "version": "2.0.0",
        "endpoints": {
            "/chat": "POST - Chat with the agent",
            "/kg-query": "POST - Dynamic knowledge graph querying",
            "/health": "GET - Health check",
            "/kg-files": "GET - List available KG files",
            "/docs": "GET - API documentation"
        }
    }

@app.post("/kg-query", response_model=DynamicKGResponse)
async def dynamic_kg_query(request: DynamicKGRequest):
    """
    Dynamic Knowledge Graph Query Endpoint
    
    Takes a natural language query, automatically extracts date ranges,
    generates code to fetch data from KG files, executes it safely, 
    and returns formatted data for frontend consumption.
    
    Args:
        request: DynamicKGRequest containing query and optional parameters
        
    Returns:
        DynamicKGResponse with formatted data, generated code, and insights
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"Processing KG query: {request.query[:100]}...")
        
        # Process the query using KG agent (date range extracted automatically)
        result = await kg_agent.process_query(
            query=request.query,
            kg_path=request.kg_path,
            date_range=None,  # Always None - dates extracted from query
            context=request.context
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"KG query processed successfully in {execution_time:.2f}s")
        
        return DynamicKGResponse(
            success=True,
            data=result.get('data'),
            generated_code=result.get('generated_code'),
            execution_time=execution_time,
            insights=result.get('insights', []),
            query_type=result.get('query_type')
        )
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        error_msg = f"Error processing KG query: {str(e)}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        
        return DynamicKGResponse(
            success=False,
            error=error_msg,
            execution_time=execution_time
        )
    
@app.get("/kg-files")
async def list_kg_files(kg_path: str = "Data/KGs"):
    """List available KG files in the specified directory."""
    try:
        if not os.path.exists(kg_path):
            raise HTTPException(status_code=404, detail=f"KG path not found: {kg_path}")
        
        kg_files = []
        for file in os.listdir(kg_path):
            if file.endswith('.json'):
                file_path = os.path.join(kg_path, file)
                file_stats = os.stat(file_path)
                
                kg_files.append({
                    "filename": file,
                    "path": file_path,
                    "size_mb": round(file_stats.st_size / (1024 * 1024), 2),
                    "modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                })
        
        kg_files.sort(key=lambda x: x['filename'])
        
        return {
            "kg_path": kg_path,
            "total_files": len(kg_files),
            "files": kg_files
        }
        
    except Exception as e:
        logger.error(f"Error listing KG files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing KG files: {str(e)}")

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Chat endpoint to interact with the agent."""
    try:
        logger.info(f"Received chat request: {request.message[:100]}...")
        
        if request.agent_type == "test":
            response = test_agent.run(request.message)
            agent_used = "test_agent"
        elif request.agent_type == "root":
            response = root_agent.run(request.message)
            agent_used = "root_agent"
        else:
            raise HTTPException(status_code=400, detail="Invalid agent_type. Use 'test' or 'root'")
        
        logger.info(f"Agent response generated successfully")
        
        return ChatResponse(
            response=response,
            agent_used=agent_used,
            status="success"
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint to verify agent and LLM connectivity."""
    try:
        logger.info("Performing health check")
        
        # Use the test agent's health check method
        health_info = test_agent.health_check()
        
        return HealthResponse(
            status=health_info["status"],
            message=health_info["message"],
            agent_name=health_info["agent_name"],
            llm_response=health_info.get("llm_response"),
            error=health_info.get("error")
        )
        
    except Exception as e:
        logger.error(f"Error in health check: {str(e)}")
        return HealthResponse(
            status="unhealthy",
            message="Health check failed",
            agent_name="unknown",
            error=str(e)
        )

@app.get("/agent-info")
async def agent_info():
    """Get information about the configured agents."""
    try:
        return {
            "root_agent": {
                "name": root_agent.name,
                "description": root_agent.description,
                "sub_agents_count": len(root_agent.sub_agents)
            },
            "test_agent": {
                "name": test_agent.name,
                "description": test_agent.description
            },
            "kg_agent": {
                "name": kg_agent.name,
                "description": kg_agent.description
            },
            "available_agent_types": ["test", "root", "kg_query"]
        }
    except Exception as e:
        logger.error(f"Error getting agent info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting agent info: {str(e)}")

if __name__ == "__main__":
    print("🐶 Starting KG Demo Release Agent API...")
    print("📝 API Documentation available at: http://localhost:8000/docs")
    print("🔍 Health check available at: http://localhost:8000/health")
    print("💬 Chat endpoint available at: http://localhost:8000/chat")
    print("🔍 KG Query endpoint available at: http://localhost:8000/kg-query")
    print("📂 List KG files at: http://localhost:8000/kg-files")
    
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
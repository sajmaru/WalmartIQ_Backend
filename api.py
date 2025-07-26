from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uvicorn
from release_agent.agent import root_agent
from release_agent.test_agent import test_agent
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="KG Demo Release Agent API",
    description="API for testing the knowledge graph demo agent using Google ADK",
    version="1.0.0"
)


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""
    message: str
    agent_type: Optional[str] = "test"  # "test" or "root"


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
        "version": "1.0.0",
        "endpoints": {
            "/chat": "POST - Chat with the agent",
            "/health": "GET - Health check",
            "/docs": "GET - API documentation"
        }
    }


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Chat endpoint to interact with the agent.
    
    Args:
        request: ChatRequest containing message and optional agent_type
        
    Returns:
        ChatResponse with the agent's response
    """
    try:
        logger.info(f"Received chat request: {request.message[:100]}...")
        
        if request.agent_type == "test":
            # Use the test agent directly
            response = test_agent.run(request.message)
            agent_used = "test_agent"
        elif request.agent_type == "root":
            # Use the root agent (which includes test_agent as sub-agent)
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
    """Health check endpoint to verify agent and LLM connectivity.
    
    Returns:
        HealthResponse with health status information
    """
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
            "available_agent_types": ["test", "root"]
        }
    except Exception as e:
        logger.error(f"Error getting agent info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting agent info: {str(e)}")


if __name__ == "__main__":
    print("🐶 Starting KG Demo Release Agent API...")
    print("📝 API Documentation available at: http://localhost:8000/docs")
    print("🔍 Health check available at: http://localhost:8000/health")
    print("💬 Chat endpoint available at: http://localhost:8000/chat")
    
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

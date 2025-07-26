from google.adk.agents import Agent
from .llm_model import llm_model
from .prompts import prompt_manager


class TestAgent(Agent):
    """A simple test agent that processes user input and returns LLM responses.
    
    This agent is designed to validate that the LLM connection and agent structure
    is working correctly before building more complex functionality.
    """
    
    def __init__(self):
        """Initialize the TestAgent."""
        super().__init__(
            name="test_agent",
            description="A simple test agent for validating LLM connectivity and agent structure"
        )
        # Use object.__setattr__ to bypass Pydantic validation for the llm attribute
        object.__setattr__(self, 'llm', llm_model)
    
    def run(self, user_input: str) -> str:
        """Process user input and return a response from the LLM.
        
        Args:
            user_input (str): The user's input message.
            
        Returns:
            str: The LLM's response.
        """
        try:
            # Create a simple prompt for the test
            system_prompt = prompt_manager.get_system_prompt("default")
            
            # Format the user prompt
            full_prompt = f"""
            System: {system_prompt}
            
            User: {user_input}
            
            Please provide a helpful and informative response.
            """
            
            # Get response from LLM
            response = self.llm.generate(full_prompt)
            
            return response
            
        except Exception as e:
            error_msg = f"Error in TestAgent: {str(e)}"
            print(error_msg)
            return f"Sorry, I encountered an error: {error_msg}"
    
    def health_check(self) -> dict:
        """Perform a health check of the agent and LLM connectivity.
        
        Returns:
            dict: Health status information.
        """
        try:
            # Simple test prompt
            test_response = self.llm.generate("Say 'Hello, I am working correctly!'")
            
            return {
                "status": "healthy",
                "agent_name": self.name,
                "llm_response": test_response,
                "message": "Agent and LLM are functioning correctly"
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "agent_name": self.name,
                "error": str(e),
                "message": "Agent or LLM connectivity issue detected"
            }


# Create a test agent instance for use in other modules
test_agent = TestAgent()

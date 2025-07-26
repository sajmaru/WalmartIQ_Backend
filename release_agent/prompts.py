"""Prompts module for the release agent.

This module contains prompt templates and configurations for the agents.
Prompts are organized by functionality and can be easily modified or extended.
"""

# System prompts for different agent types
SYSTEM_PROMPTS = {
    "kg_demo": """
    You are the kg_demo agent, responsible for coordinating and running different sub-agents
    to accomplish complex tasks related to knowledge graph operations.
    
    Your role is to:
    1. Analyze incoming requests and determine which sub-agents are needed
    2. Coordinate the execution of sub-agents in the correct sequence
    3. Combine results from multiple sub-agents to provide comprehensive responses
    4. Handle errors and retries when sub-agents fail
    
    Always be clear about what you're doing and why, and provide helpful feedback
    to users about the progress of their requests.
    """,
    
    "default": """
    You are an AI assistant built using the Google ADK framework.
    You are helpful, harmless, and honest in all your interactions.
    """
}

# User prompt templates
USER_PROMPTS = {
    "task_execution": """
    Please execute the following task: {task_description}
    
    Additional context: {context}
    
    Requirements:
    {requirements}
    """,
    
    "sub_agent_coordination": """
    I need to coordinate multiple sub-agents for this task.
    
    Task: {task}
    Available sub-agents: {sub_agents}
    
    Please determine the optimal sequence and coordination strategy.
    """,
    
    "error_handling": """
    An error occurred while executing: {operation}
    
    Error details: {error_details}
    
    Please suggest a recovery strategy or alternative approach.
    """
}

# Response templates
RESPONSE_TEMPLATES = {
    "task_started": "Starting task: {task_name}. Coordinating {num_agents} sub-agents.",
    "task_completed": "Task '{task_name}' completed successfully. Results: {results}",
    "task_failed": "Task '{task_name}' failed. Error: {error}. Suggested action: {suggestion}",
    "agent_status": "Agent '{agent_name}' status: {status}. Progress: {progress}%"
}


class PromptManager:
    """Manager for handling prompt templates and formatting."""
    
    @staticmethod
    def get_system_prompt(agent_type="default"):
        """Get the system prompt for a specific agent type.
        
        Args:
            agent_type (str): The type of agent.
            
        Returns:
            str: The system prompt.
        """
        return SYSTEM_PROMPTS.get(agent_type, SYSTEM_PROMPTS["default"])
    
    @staticmethod
    def format_user_prompt(template_name, **kwargs):
        """Format a user prompt template with provided arguments.
        
        Args:
            template_name (str): The name of the template.
            **kwargs: Arguments to format the template.
            
        Returns:
            str: The formatted prompt.
        """
        template = USER_PROMPTS.get(template_name, "")
        return template.format(**kwargs)
    
    @staticmethod
    def format_response(template_name, **kwargs):
        """Format a response template with provided arguments.
        
        Args:
            template_name (str): The name of the template.
            **kwargs: Arguments to format the template.
            
        Returns:
            str: The formatted response.
        """
        template = RESPONSE_TEMPLATES.get(template_name, "")
        return template.format(**kwargs)


# Global prompt manager instance
prompt_manager = PromptManager()

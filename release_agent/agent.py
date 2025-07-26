from google.adk.agents import SequentialAgent
from .llm_model import llm_model
from .test_agent import test_agent


# Root agent for the kg_demo system
root_agent = SequentialAgent(
    name="kg_demo",
    description="Responsible for running different sub-agents. Name: kg_demo.",
    sub_agents=[test_agent]  # Added test_agent for initial testing
)
# Use object.__setattr__ to bypass Pydantic validation for the llm attribute
object.__setattr__(root_agent, 'llm', llm_model)


if __name__ == "__main__":
    # Example of how to run the agent
    print("Starting kg_demo agent...")
    print(f"Agent name: {root_agent.name}")
    print(f"Agent description: {root_agent.description}")
    print(f"Number of sub-agents: {len(root_agent.sub_agents)}")

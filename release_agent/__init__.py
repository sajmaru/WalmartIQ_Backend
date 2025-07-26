"""Release Agent package for kg_demo.

This package contains the main agent implementation and supporting modules
for the knowledge graph demonstration using Google ADK framework.
"""

from .agent import root_agent
from .llm_model import llm_model, LLMModel
from .tools import tool_registry, ToolRegistry
from .prompts import prompt_manager, PromptManager
from .test_agent import test_agent, TestAgent

__version__ = "1.0.0"
__author__ = "Walmart AI Team"

__all__ = [
    "root_agent",
    "llm_model",
    "LLMModel",
    "tool_registry",
    "ToolRegistry",
    "prompt_manager",
    "PromptManager",
    "test_agent",
    "TestAgent"
]

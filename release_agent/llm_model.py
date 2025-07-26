from google.adk.models.base_llm import BaseLlm
from .constants import API_BASE_URL, API_KEY
import asyncio
import litellm
from typing import Optional, Dict, Any


class LLMModel(BaseLlm):
    """LLM Model class that extends BaseLlm from google_adk.
    
    This class configures the LiteLLM model to work with Walmart's LLM gateway,
    using the appropriate API base URL, authentication headers, and SSL configuration.
    """
    
    def __init__(self, model="azure/gpt-4.1"):
        """Initialize the LLMModel.
        
        Args:
            model (str): The model identifier. Defaults to "azure/gpt-4.1".
        """
        super().__init__(
            model=model,
            api_base=API_BASE_URL,
            api_key="<ignored>",
            extra_headers={"X-Api-Key": API_KEY },
        )
        
        # Configure litellm for this instance (using underscore to avoid Pydantic conflicts)
        self._model_name = model
        self._api_base_url = API_BASE_URL
        self._custom_headers = {"X-Api-Key": API_KEY}


    async def generate_content_async(self, prompt: str, **kwargs) -> str:
        """Generate content asynchronously using the LLM.
        
        Args:
            prompt (str): The input prompt for the LLM.
            **kwargs: Additional keyword arguments for the LLM call.
            
        Returns:
            str: The generated content from the LLM.
        """
        try:
            # Use litellm to make the async call
            response = await litellm.acompletion(
                model=self._model_name,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                api_base=self._api_base_url,
                api_key="<ignored>",  # We use custom headers instead
                extra_headers=self._custom_headers,
                **kwargs
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            raise Exception(f"Error generating content with LLM: {str(e)}")
    
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate content synchronously using the LLM.
        
        Args:
            prompt (str): The input prompt for the LLM.
            **kwargs: Additional keyword arguments for the LLM call.
            
        Returns:
            str: The generated content from the LLM.
        """
        try:
            # Use litellm to make the sync call
            response = litellm.completion(
                model=self._model_name,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                api_base=self._api_base_url,
                api_key="<ignored>",  # We use custom headers instead
                extra_headers=self._custom_headers,
                **kwargs
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            raise Exception(f"Error generating content with LLM: {str(e)}")


# Create a default LLM model instance for use in other modules
llm_model = LLMModel()

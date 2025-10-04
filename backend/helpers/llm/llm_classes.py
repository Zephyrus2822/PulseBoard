import os
from typing import Dict, Any, Optional
from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path

# Import provider SDKs
import google.generativeai as genai
import cohere
import openai
from openai import OpenAI
import sys

sys.path.insert(1, "../../")
from helpers.logger import get_logger

logger = get_logger(__name__)


class LLM:
    """LLM wrapper class supporting multiple providers"""
    
    def __init__(
        self,
        model_type: str,
        model_name: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        response_format: str = "text",
        endpoint: Optional[str] = None
    ):
        """
        Initialize LLM with provider-specific configuration
        
        Args:
            model_type: Provider type (gemini, cohere, openai, togetherai)
            model_name: Model name
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            response_format: Response format (text, json_object)
            endpoint: API endpoint (optional)
        """
        self.model_type = model_type.lower()
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.response_format = response_format
        self.endpoint = endpoint
        
        # Initialize Jinja2 environment
        template_dir = Path(__file__).parent.parent.parent / "ai_agents" / "prompts"
        self.jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))
        
        logger.info(f"Initialized LLM: {model_type}/{model_name}")
    
    def get_prompt_template(
        self,
        system_template: Optional[str],
        user_template: str,
        message_history: list,
        context_variables: Dict[str, Any]
    ) -> str:
        """
        Render Jinja templates with context variables
        
        Args:
            system_template: System prompt template path (optional)
            user_template: User prompt template path or raw text
            message_history: Conversation history
            context_variables: Variables for template rendering
            
        Returns:
            Rendered prompt string
        """
        try:
            # Load system template (security guidelines)
            system_content = ""
            if system_template:
                sys_template = self.jinja_env.get_template(system_template)
                system_content = sys_template.render(**context_variables)
            
            # Load user template
            if user_template.endswith('.jinja'):
                user_tmpl = self.jinja_env.get_template(user_template)
                user_content = user_tmpl.render(**context_variables)
            else:
                # Raw text
                user_content = user_template
            
            # Combine system + user prompts
            full_prompt = f"{system_content}\n\n{user_content}".strip()
            
            logger.debug(f"Rendered template with {len(context_variables)} variables")
            return full_prompt
            
        except Exception as e:
            logger.error(f"Template rendering failed: {e}")
            raise
    
    def get_gemini_llm(self):
        """Get Google Gemini LLM instance"""
        genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))
        model = genai.GenerativeModel(self.model_name)
        logger.info(f"Initialized Gemini: {self.model_name}")
        return GeminiWrapper(model, self.temperature, self.max_tokens)
    
    def get_cohere_llm(self):
        """Get Cohere LLM instance"""
        client = cohere.Client(api_key=os.getenv('COHERE_API_KEY'))
        logger.info(f"Initialized Cohere: {self.model_name}")
        return CohereWrapper(client, self.model_name, self.temperature, self.max_tokens)
    
    def get_openai_llm(self):
        """Get OpenAI LLM instance"""
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        logger.info(f"Initialized OpenAI: {self.model_name}")
        return OpenAIWrapper(client, self.model_name, self.temperature, self.max_tokens)
    
    def get_togetherai_llm(self):
        """Get TogetherAI LLM instance"""
        client = OpenAI(
            api_key=os.getenv('TOGETHER_API_KEY'),
            base_url=self.endpoint
        )
        logger.info(f"Initialized TogetherAI: {self.model_name}")
        return TogetherAIWrapper(client, self.model_name, self.temperature, self.max_tokens)


class GeminiWrapper:
    """Wrapper for Google Gemini"""
    
    def __init__(self, model, temperature: float, max_tokens: int):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
    
    def invoke(self, prompt: str):
        """Invoke Gemini model"""
        generation_config = {
            "temperature": self.temperature,
            "max_output_tokens": self.max_tokens,
        }
        response = self.model.generate_content(
            prompt,
            generation_config=generation_config
        )
        return response


class CohereWrapper:
    """Wrapper for Cohere"""
    
    def __init__(self, client, model_name: str, temperature: float, max_tokens: int):
        self.client = client
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
    
    def invoke(self, prompt: str):
        """Invoke Cohere model"""
        response = self.client.chat(
            model=self.model_name,
            message=prompt,
            temperature=self.temperature,
            max_tokens=self.max_tokens
        )
        return response


class OpenAIWrapper:
    """Wrapper for OpenAI"""
    
    def __init__(self, client, model_name: str, temperature: float, max_tokens: int):
        self.client = client
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
    
    def invoke(self, prompt: str):
        """Invoke OpenAI model"""
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=self.temperature,
            max_tokens=self.max_tokens
        )
        return response


class TogetherAIWrapper:
    """Wrapper for TogetherAI"""
    
    def __init__(self, client, model_name: str, temperature: float, max_tokens: int):
        self.client = client
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
    
    def invoke(self, prompt: str):
        """Invoke TogetherAI model"""
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=self.temperature,
            max_tokens=self.max_tokens
        )
        return response
# LLM API calls
import time
import yaml
from pathlib import Path
from typing import Dict, Any, Union, Optional
import sys

sys.path.insert(1, "../../")
from helpers.llm.llm_classes import LLM
from helpers.logger import get_logger

logger = get_logger(__name__)

# Load config
config_path = Path(__file__).parent / "config.yml"
with open(config_path, 'r') as f:
    CONFIG = yaml.safe_load(f)


def call_llm(
    prompt_or_template: str,
    system_prompt: Optional[str] = None,
    message_history: Optional[list] = None,
    context_variables: Optional[Dict[str, Any]] = None,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    use_template: bool = True,
    use_fallback: bool = True,
) -> Union[str, Any]:
    """
    Sets up and calls an LLM with the specified configuration.
    Implements primary/fallback logic.

    Args:
        prompt_or_template: The prompt text or template path
        system_prompt: Optional system prompt or system prompt template path
        message_history: Optional conversation history
        context_variables: Optional variables to be used in template rendering
        model_name: Override model name (default: from config)
        temperature: Override temperature (default: from config)
        use_template: Whether prompt_or_template is a template path (True) or raw text (False)
        use_fallback: Enable fallback to secondary LLM on error

    Returns:
        The LLM response object
    """
    llm_setup_start = time.time()
    
    # Try primary LLM first
    try:
        logger.info("Attempting primary LLM call...")
        response = _call_llm_with_config(
            config=CONFIG['primaryLlm'],
            prompt_or_template=prompt_or_template,
            system_prompt=system_prompt,
            message_history=message_history,
            context_variables=context_variables,
            model_name=model_name,
            temperature=temperature,
            use_template=use_template,
        )
        
        total_time = time.time() - llm_setup_start
        logger.info(f"✅ Primary LLM call successful (took {total_time:.4f} seconds)")
        return response
        
    except Exception as primary_error:
        logger.warning(f"❌ Primary LLM failed: {str(primary_error)}")
        
        if not use_fallback:
            logger.error("Fallback disabled, raising error")
            raise
        
        # Try fallback LLM
        try:
            logger.info("Attempting fallback LLM call...")
            response = _call_llm_with_config(
                config=CONFIG['fallbackLlm'],
                prompt_or_template=prompt_or_template,
                system_prompt=system_prompt,
                message_history=message_history,
                context_variables=context_variables,
                model_name=model_name,
                temperature=temperature,
                use_template=use_template,
            )
            
            total_time = time.time() - llm_setup_start
            logger.info(f"✅ Fallback LLM call successful (took {total_time:.4f} seconds)")
            return response
            
        except Exception as fallback_error:
            total_time = time.time() - llm_setup_start
            logger.error(
                f"❌ Both primary and fallback LLMs failed (took {total_time:.4f} seconds)"
            )
            logger.error(f"Primary error: {str(primary_error)}")
            logger.error(f"Fallback error: {str(fallback_error)}")
            raise Exception(
                f"All LLM providers failed. Primary: {str(primary_error)}, "
                f"Fallback: {str(fallback_error)}"
            )


def _call_llm_with_config(
    config: Dict[str, Any],
    prompt_or_template: str,
    system_prompt: Optional[str],
    message_history: Optional[list],
    context_variables: Optional[Dict[str, Any]],
    model_name: Optional[str],
    temperature: Optional[float],
    use_template: bool,
) -> Union[str, Any]:
    """
    Internal function to call LLM with specific config
    """
    llm_setup_start = time.time()
    
    try:
        # Get config values
        model_type = config['modelType']
        final_model_name = model_name or config['modelName']
        final_temperature = temperature or config['temperature']
        max_tokens = config.get('max_tokens', 2000)
        response_format = config.get('response_format', 'text')
        endpoint = config.get('endpoint')
        
        # Initialize LLM
        llm = LLM(
            model_type=model_type,
            model_name=final_model_name,
            temperature=final_temperature,
            max_tokens=max_tokens,
            response_format=response_format,
            endpoint=endpoint
        )
        
        # Get provider-specific LLM instance
        if model_type == "gemini":
            llm_model = llm.get_gemini_llm()
        elif model_type == "cohere":
            llm_model = llm.get_cohere_llm()
        elif model_type == "openai":
            llm_model = llm.get_openai_llm()
        elif model_type == "togetherai":
            llm_model = llm.get_togetherai_llm()
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
        
        llm_setup_end = time.time()
        logger.debug(f"LLM setup took {llm_setup_end - llm_setup_start:.4f} seconds")
        
        # Prepare the prompt
        prompt_prep_start = time.time()
        
        if use_template and system_prompt:
            # Both templates provided
            formatted_prompt = llm.get_prompt_template(
                system_prompt,
                prompt_or_template,
                message_history or [],
                context_variables or {},
            )
        elif use_template:
            # Only prompt template provided
            formatted_prompt = llm.get_prompt_template(
                None,
                prompt_or_template,
                message_history or [],
                context_variables or {}
            )
        else:
            # Raw text provided
            formatted_prompt = prompt_or_template
        
        prompt_prep_end = time.time()
        logger.debug(
            f"Prompt preparation took {prompt_prep_end - prompt_prep_start:.4f} seconds"
        )
        
        # Call LLM
        llm_call_start = time.time()
        response = llm_model.invoke(formatted_prompt)
        llm_call_end = time.time()
        
        logger.info(f"LLM call took {llm_call_end - llm_call_start:.4f} seconds")
        logger.debug(f"LLM response type: {type(response)}")
        
        return response
        
    except Exception as e:
        end_time = time.time()
        logger.error(
            f"Error in _call_llm_with_config: {str(e)} "
            f"(took {end_time - llm_setup_start:.4f} seconds)",
            exc_info=True
        )
        raise
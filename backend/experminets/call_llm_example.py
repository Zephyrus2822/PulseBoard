import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(1, "../")

from helpers.llm.call_llm import call_llm
from helpers.logger import get_logger

logger = get_logger(__name__)


def main():
    """Generate 100 words on data analysis and save to file"""
    
    logger.info("Starting experiment: Generate 100 words on data analysis")
    
    try:
        # Prompt
        prompt = "Write exactly 100 words about data analysis. Include its importance, techniques, and applications."
        
        logger.info("Calling LLM...")
        
        # Call LLM
        response = call_llm(
            prompt_or_template=prompt,
            use_template=False
        )
        
        # Extract text
        if hasattr(response, 'text'):
            text = response.text
        elif hasattr(response, 'choices'):
            text = response.choices[0].message.content
        else:
            text = str(response)
        
        logger.info(f"Received response: {len(text.split())} words")
        
        # Save to file
        output_file = Path(__file__).parent / "data_analysis_output.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(text)
        
        logger.info(f"✅ Output saved to: {output_file}")
        print(f"\n✅ Success! Output saved to: {output_file}")
        print(f"\nContent:\n{'-'*60}\n{text}\n{'-'*60}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
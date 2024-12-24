## extractor 📄✨

A Python tool for processing Word documents and extracting legal analysis using an OpenAI model. 🤖⚖️

### Installation
1. Create a virtual environment (optional).
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### `.env` Configuration
- **OPENAI_API_KEY**: Your OpenAI API key.
- **OPENAI_MODEL_NAME**: The model name to use, e.g., `"gpt-4"`. Defaults to `"o1"`.
- **MAX_CONCURRENT_REQUESTS**: Number of simultaneous API calls. Defaults to `10`.
- **MAX_RETRIES**: Number of times to retry failed requests. Defaults to `1`.
- **RETRY_DELAY**: Initial delay (in seconds) before retrying. Exponential backoff is applied.

### Running 🚀
```bash
python main.py
```
This will read `.docx` files from `input_docs`, send each to the OpenAI model (based on your `.env` settings), and save analysis results to `analysis_results`.

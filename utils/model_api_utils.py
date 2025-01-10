import requests
import json
import os
import time
from utils.constants import APIConstant as API
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
from custom_logging.logging import TerminalLogging

class ModelApiUtils():
    gemini_api_key_index = 0

    @staticmethod
    def send_request_directly(prompt: str, input_data: str) -> dict:
        token = API.OPEN_ROUTER_TOKEN
        if token == None:
            raise Exception("Found no OPEN ROUTER TOKEN")
        
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {token}"
            },
            data=json.dumps({
                "model": "openai/gpt-4o-mini",
                "messages": [
                {
                    "role": "system",
                    "content": prompt
                },
                {
                    "role": "user",
                    "content": input_data
                }
                ],
                'response_format': {'type': 'json_object'}
            })
        )

        return json.loads(json.loads(response.text.replace("json```", '').replace("```", ''))['choices'][0]['message']['content'])
    
    @classmethod
    def request_gemini(cls, prompt: str) -> str:
        api_keys = API.GEMINI_API_KEYS
        if len(api_keys) == 0:
            raise Exception("Found no GEMINI API KEY")

        while (True):
            try:        
                api_key = api_keys[cls.gemini_api_key_index]
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel("gemini-1.5-flash")
                response = model.generate_content(prompt)
                break
            except ResourceExhausted as re:
                TerminalLogging.log_error("Exhausted Gemini API, waiting...")
                time.sleep(1)
                continue
            finally:
                cls.gemini_api_key_index = (cls.gemini_api_key_index + 1) if (cls.gemini_api_key_index < len(api_keys) - 1) else 0

        return response.text.replace("```json", "").replace("```", "").replace("\n", "")
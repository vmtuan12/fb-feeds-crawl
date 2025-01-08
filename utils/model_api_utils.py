import requests
import json
import os
from utils.constants import APIConstant as API

class ModelApiUtils():
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
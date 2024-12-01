import google.generativeai as genai
import traceback
import json
import time
from custom_exception.exceptions import KeywordsNotMatchedException
from google.api_core.exceptions import ResourceExhausted

class KeywordExtractionUtils():
    BASE_PROMPT = """
    {}
    You are an experienced writer. Remove all stopwords and words that means nothing to the event, and extract the most important keywords in the above paragraphs that describe the event IN VIETNAMESE.  Result of each paragraph is answered on a new line, with the same order. No explaination, no comments, no analyzing, no note, you say nothing. Just do as I say. Write me the answer only with the format like
    [
    "keyword1,keyword2,keyword3",
    "keyword1,keyword2,keyword3",
    "keyword1,keyword2,keyword3"
    ]
    Write in json, result has the same order as input. Remember: time is not a keyword. Name of a place or people is automatically a keyword.
    """

    @classmethod
    def enrich_keywords(cls, list_data: list[dict], api_key: str, model_name="gemini-1.5-flash") -> list:
        while (True):
            try:
                list_text_str = ""
                for item in list_data:
                    list_text_str += f"`{item.get('text')}`;\n"

                prompt = cls.BASE_PROMPT.format(list_text_str)

                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(model_name)

                response = model.generate_content(prompt)
                
                result_str = response.text
                list_keywords_by_post = json.loads(result_str.replace("```json", "").replace("```", "").replace("\n", ""))

                if len(list_data) != len(list_keywords_by_post):
                    raise KeywordsNotMatchedException()
                
                for index, item in enumerate(list_data):
                    keyword_list = [k.lower() for k in list_keywords_by_post[index].split(",")]

                    if keyword_list[0] not in item.get('text').lower():
                        raise KeywordsNotMatchedException()
                    
                    item["keywords"] = keyword_list

                return list_data
            
            except ResourceExhausted as re:            
                time.sleep(1)
                continue
            except KeywordsNotMatchedException as ke:
                continue
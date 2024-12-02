import google.generativeai as genai
import traceback
import json
import time
from utils.parser_utils import ParserUtils
from custom_exception.exceptions import KeywordsNotMatchedException
from google.api_core.exceptions import ResourceExhausted
from custom_logging.logging import TerminalLogging

class KeywordExtractionUtils():
    BASE_PROMPT = """
    Given the paragraphs, each is put between 2 backticks
    {0}
    You are an experienced writer. Remove all stopwords and words that means nothing to the event, and extract the most important keywords in the above paragraphs that describe the event IN VIETNAMESE.  Result of each paragraph is answered on a new line, with the same order. No explaination, no comments, no analyzing, no note, you say nothing. Just do as I say. Write me the answer only with the format like
    [
    "keyword1,keyword2,keyword3",
    "keyword1,keyword2,keyword3",
    "keyword1,keyword2,keyword3"
    ]
    Write in json, result has the same order as input, which means the first paragraph corresponds to the first line of keywords, the second paragraph corresponds to the second line of keywords, etc. Remember: time is not a keyword. Name of a place or people is automatically a keyword. Keywords must be contained in the paragraph, do not change or paraphrase them. Do not skip any paragraph. Ensure the there must be exactly {1} lines of keywords.
    """

    @classmethod
    def enrich_keywords(cls, list_data: list[dict], api_key: str, model_name="gemini-1.5-flash") -> list:
        while (True):
            try:
                list_text_str = ""
                for item in list_data:
                    list_text_str += f"`{ParserUtils.strip_emoji(item.get('text'))}`;\n"

                prompt = cls.BASE_PROMPT.format(list_text_str, len(list_data))

                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(model_name)

                TerminalLogging.log_info(f"Extracting keywords with API KEY {api_key}")
                response = model.generate_content(prompt)
                
                result_str = response.text
                list_keywords_by_post = json.loads(result_str.replace("```json", "").replace("```", "").replace("\n", ""))

                if len(list_data) != len(list_keywords_by_post):
                    print(len(list_data), len(list_keywords_by_post))
                    raise KeywordsNotMatchedException(msg="Different in length")
                
                for index, item in enumerate(list_data):
                    keyword_list = [k.lower() for k in list_keywords_by_post[index].split(",")]

                    if (keyword_list[0] not in item.get('text').lower()) and (keyword_list[-1] not in item.get('text').lower()):
                        TerminalLogging.log_info(f"{keyword_list[0]}, {keyword_list[-1]}\n{item.get('text').lower()}")
                        raise KeywordsNotMatchedException(msg="Wrong order")
                    
                    item["keywords"] = keyword_list

                return list_data
            
            except ResourceExhausted as re:
                TerminalLogging.log_error(f"API KEY {api_key} is exhausted. Wait and retry...")
                time.sleep(1)
                continue
            except KeywordsNotMatchedException as ke:
                TerminalLogging.log_error(f"{ke.msg}. Retrying...")
                continue
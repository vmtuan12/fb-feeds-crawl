import google.generativeai as genai
import traceback
import requests
import json
import time
from utils.parser_utils import ParserUtils
from custom_exception.exceptions import KeywordsNotMatchedException
from google.api_core.exceptions import ResourceExhausted
from custom_logging.logging import TerminalLogging
from utils.constants import APIConstant as API

class KeywordExtractionUtils():
    BASE_PROMPT = """
    Given the paragraphs in format json, with key is the ID of the paragraph, and value is the text
    You are an experienced writer. Remove all stopwords and words that means nothing to the event, and extract the most important keywords in the above paragraphs that describe the event IN VIETNAMESE.  Result of each paragraph is answered on a new line, with the same order. No explaination, no comments, no analyzing, no note, you say nothing. Just do as I say. Write me the answer only with the format like
    {0}
    Write in json, keywords of a paragraph is assigned with its ID. 
    Remember: word about time is not a keyword.
    Name of a place is automatically a keyword.
    Name of a person is automatically a keyword.
    Word or complex words that describes what happens is automatically a keyword.
    Verb of complex verbs or that describes action of people is automatically a keyword.
    Do not skip any paragraph. 
    Keywords must exist in the paragraph, do not change or paraphrase them.
    You are prohibited from changing the key.
    Ensure the there are exactly {1} elements of keywords
    """

    @classmethod
    def enrich_keywords(cls, list_data: list[dict]) -> list:
        while (True):
            try:
                post_by_id_dict = {}
                list_text_str = "{"

                for index, item in enumerate(list_data):
                    post_by_id_dict[str(index)] = item
                    list_text_str += f'"{str(index)}": "{ParserUtils.strip_emoji(item.get("text"))}",'

                list_text_str += list_text_str[:-1] + "}"

                prompt = cls.BASE_PROMPT.format('{id1: "keyword1,keyword2,keyword3", id2: "keyword1,keyword2,keyword3", id3: "keyword1,keyword2,keyword3"}', len(list_data))

                body = {"prompt": [
                    {'role': 'system', 'content': prompt},
                    {'role': 'user', 'content': f'{list_text_str}'}
                ], "json_output": True}

                response = requests.post(url=API.MODEL_API, data=json.dumps(body))
                
                # result_str = json.loads(response.text).get("data")
                dict_keywords_by_post = json.loads(response.text).get("data")

                # print(len(dict_keywords_by_post.keys()))
                if len(post_by_id_dict.keys()) != len(dict_keywords_by_post.keys()):
                    # [print(post_by_id_dict[item]["text"]) for item in post_by_id_dict]
                    # print(dict_keywords_by_post)
                    raise KeywordsNotMatchedException("Length fail")
                
                # print(dict_keywords_by_post.keys(), post_by_id_dict.keys())
                for key in dict_keywords_by_post.keys():
                    if dict_keywords_by_post.get(key) == None:
                        dict_keywords_by_post[key] = ""
                    keyword_list = [k.strip().lower() for k in dict_keywords_by_post[key].split(",")]
                    post_by_id_dict[key]["keywords"] = keyword_list

                TerminalLogging.log_info(f"Done extracting keywords")

                return list(post_by_id_dict.values())
            
            except KeywordsNotMatchedException as ke:
                TerminalLogging.log_error(f"{ke.msg}. Retrying...")
                continue
            except (json.decoder.JSONDecodeError, KeyError) as ge:
                TerminalLogging.log_error(f"Model returns wrong format/data")
                continue
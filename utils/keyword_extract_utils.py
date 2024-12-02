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
    Given the paragraphs in format json, with key is the ID of the paragraph, and value is the text
    {0}
    You are an experienced writer. Remove all stopwords and words that means nothing to the event, and extract the most important keywords in the above paragraphs that describe the event IN VIETNAMESE.  Result of each paragraph is answered on a new line, with the same order. No explaination, no comments, no analyzing, no note, you say nothing. Just do as I say. Write me the answer only with the format like
    {1}
    Write in json, keywords of a paragraph is assigned with its ID. 
    Remember: time is not a keyword. Name of a place or people is automatically a keyword. Word or complex words that describes what happens is automatically a keyword. Verb of complex verbs or that describes action of people is automatically a keyword
    Do not skip any paragraph. 
    Keywords must exist in the paragraph, do not change or paraphrase them.
    You are prohibited from changing the key.
    Ensure the there are exactly {2} elements of keywords
    """

    @classmethod
    def enrich_keywords(cls, list_data: list[dict], api_key: str, model_name="gemini-1.5-flash") -> list:
        while (True):
            try:
                start_time = time.time()
                post_by_id_dict = {}
                list_text_str = "{"

                for index, item in enumerate(list_data):
                    post_by_id_dict[str(index)] = item
                    list_text_str += f'"{str(index)}": "{ParserUtils.strip_emoji(item.get("text"))}",'

                list_text_str += list_text_str[:-1] + "}"

                prompt = cls.BASE_PROMPT.format(list_text_str, '{id1: "keyword1,keyword2,keyword3", id2: "keyword1,keyword2,keyword3", id3: "keyword1,keyword2,keyword3"}', len(list_data))

                genai.configure(api_key=api_key)
                model = genai.GenerativeModel("gemini-1.5-flash")

                TerminalLogging.log_info(f"Extracting keywords with API KEY {api_key}")
                response = model.generate_content(prompt)
                
                result_str = response.text
                dict_keywords_by_post = json.loads(result_str.replace("```json", "").replace("```", "").replace("\n", ""))

                # print(len(dict_keywords_by_post.keys()))
                if len(post_by_id_dict.keys()) != len(dict_keywords_by_post.keys()):
                    [print(post_by_id_dict[item]["text"]) for item in post_by_id_dict]
                    print(dict_keywords_by_post)
                    raise KeywordsNotMatchedException("Length fail")
                
                print(dict_keywords_by_post.keys(), post_by_id_dict.keys())
                for key in dict_keywords_by_post.keys():
                    if dict_keywords_by_post.get(key) == None:
                        dict_keywords_by_post[key] = ""
                    keyword_list = [k.strip().lower() for k in dict_keywords_by_post[key].split(",")]
                    post_by_id_dict[key]["keywords"] = keyword_list

                print(f"DONE IN {int(time.time() - start_time)}s")

                return list(post_by_id_dict.values())
            
            except ResourceExhausted as re:
                TerminalLogging.log_error(f"API KEY {api_key} is exhausted. Wait and retry...")
                time.sleep(1)
                continue
            except KeywordsNotMatchedException as ke:
                TerminalLogging.log_error(f"{ke.msg}. Retrying...")
                continue
            except (json.decoder.JSONDecodeError, KeyError) as ge:
                TerminalLogging.log_error(f"Model returns wrong format/data")
                continue
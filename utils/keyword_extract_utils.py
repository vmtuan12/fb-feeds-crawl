import traceback
import requests
import json
import time
from utils.parser_utils import ParserUtils
from custom_exception.exceptions import KeywordsNotMatchedException
from custom_logging.logging import TerminalLogging
from utils.constants import APIConstant as API

class KeywordExtractionUtils():
    BASE_PROMPT = """
    Given the paragraphs in format json, with key is the ID of the paragraph, and value is the text
    You are an experienced writer. Remove all stopwords and words that means nothing to the event, and extract the most important keywords in the above paragraphs that describe the event IN VIETNAMESE.  Result of each paragraph is answered on a new line, with the same order. No explaination, no comments, no analyzing, no note, you say nothing. Just do as I say. Write me the answer only with the format like
    {0}
    Write in json, keywords of a paragraph is assigned with its ID. 
    Remember: word about time is not a keyword.
    Name of a person/a group/a team is automatically a keyword.
    Word that describes relationship is automatically a keyword. (for example: "bố", "mẹ", "anh", "chị", "em", etc. and many more)
    Name of a place is automatically a keyword.
    Word that describes a place is automatically a keyword.
    Word or complex words that describes what happens is automatically a keyword. Make it as common as you can (for example: "vụ cháy" is "cháy", "đám cháy" is "cháy", etc. and many more).
    Verb of complex verbs or that describes action of people is automatically a keyword.
    Keywords must exist in the paragraph, do not change or paraphrase them.
    You are prohibited from changing the key.
    Every key of the input must be processed, do not skip any of them. If you cannot extract any keywords from the paragraph, the result is empty string ""
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

                TerminalLogging.log_info(f"Start extracting keywords ...")
                prompt = cls.BASE_PROMPT.format('{id1: "keyword1,keyword2,keyword3", id2: "keyword1,keyword2,keyword3", id3: "keyword1,keyword2,keyword3"}')

                body = {"prompt": [
                    {'role': 'system', 'content': prompt},
                    {'role': 'user', 'content': f'{list_text_str}'}
                ], "json_output": True}

                response = requests.post(url=API.MODEL_API, data=json.dumps(body))
                
                # result_str = json.loads(response.text).get("data")
                try:
                    dict_keywords_by_post = json.loads(response.text).get("data")
                except Exception as e:
                    TerminalLogging.log_error(response.text)
                    raise json.decoder.JSONDecodeError(msg="Cannot decode")

                if dict_keywords_by_post == None:
                    TerminalLogging.log_error(response.text)
                    raise json.decoder.JSONDecodeError(msg="Keyword dict is None")

                for key in dict_keywords_by_post.keys():
                    if dict_keywords_by_post.get(key) == None or dict_keywords_by_post.get(key) == "":
                        pass
                    else:
                        keyword_list = [k.strip().replace(".", "").replace('"', '').replace("'", "").lower() for k in dict_keywords_by_post[key].split(",")]
                        post_by_id_dict[key]["keywords"].update(keyword_list)

                    post_by_id_dict[key]["keywords"] = list(post_by_id_dict[key]["keywords"])

                TerminalLogging.log_info(f"Done extracting keywords")

                return list(post_by_id_dict.values())
            
            except KeywordsNotMatchedException as ke:
                TerminalLogging.log_error(f"{ke.msg}. Retrying...")
                continue
            except (json.decoder.JSONDecodeError, KeyError) as ge:
                TerminalLogging.log_error(traceback.format_exc())
                TerminalLogging.log_error(f"Model returns wrong format/data")
                continue
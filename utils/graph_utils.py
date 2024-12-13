import math
import re
from utils.parser_utils import ParserUtils
from collections import Counter
from pyvi import ViTokenizer, ViPosTagger

class GraphUtils():
    WORD = re.compile(r"\w+")

    @classmethod
    def get_cosine(cls, text1: str, text2: str):
        vec1 = cls.text_to_vector(text1)
        vec2 = cls.text_to_vector(text2)

        intersection = set(vec1.keys()) & set(vec2.keys())
        numerator = sum([vec1[x] * vec2[x] for x in intersection])

        sum1 = sum([vec1[x] ** 2 for x in list(vec1.keys())])
        sum2 = sum([vec2[x] ** 2 for x in list(vec2.keys())])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator

    @classmethod
    def text_to_vector(cls, text: str):
        with open("stopwords.txt", "r") as f:
            stopwords = set()
            [stopwords.add(l.strip()) for l in f.readlines()]

        processed_text = text.strip().replace("\n", "").replace("\t", "").lower()
        processed_text = ParserUtils.strip_emoji(processed_text)
        processed_text = ParserUtils.strip_hashtag(processed_text)
        processed_text = ParserUtils.strip_url(processed_text)
        processed_text = ParserUtils.strip_special_chars(processed_text)

        words = ViTokenizer.tokenize(processed_text).split(" ")
        filtered_words = [w for w in words if w not in stopwords]
        return Counter(filtered_words)
import re
from datetime import datetime, timedelta
import html

class ParserUtils():
    RE_EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

    @classmethod
    def strip_emoji(cls, text: str) -> str:
        return cls.RE_EMOJI.sub(r'', text)

    @classmethod
    def match_text_and_number(cls, text: str) -> str | None:
        unescaped_text = html.unescape(text)
        pattern = r'[0-9a-zA-Z\.]+'
        match = re.search(pattern, unescaped_text)
        if match:
            return match.group()
        
        return None
    @classmethod
    def match_time(cls, text: str) -> list:
        unescaped_text = html.unescape(text)
        pattern = r'[0-9a-zA-Z ,\.]+'
        matches = re.findall(pattern, unescaped_text)
        return matches
    
    @classmethod
    def approx_post_time_str(cls, now: datetime, raw_post_time: str) -> str | None:
        post_time_matches = cls.match_time(raw_post_time)
        for post_time in post_time_matches:
            try:
                if post_time.startswith("Jus"):
                    return now.strftime("%Y-%m-%d %H:%M:%S")

                try:
                    post_time_formatted = post_time if (',' in post_time) else f"{post_time}, {datetime.now().year}"
                    real_post_time = datetime.strptime(post_time_formatted, "%b %d, %Y").strftime("%Y-%m-%d %H:%M:%S")
                    return real_post_time
                except Exception as e:
                    real_post_time = None

                time_int = int(post_time[:-1])
                unit = post_time[-1]

                if unit == "m" or unit == "p":
                    real_post_time = now - timedelta(minutes=time_int)
                elif unit == "h":
                    real_post_time = now - timedelta(hours=time_int)
                elif unit == "d":
                    real_post_time = now - timedelta(days=time_int)

                return real_post_time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                pass

        return None
    
    @classmethod
    def approx_reactions(cls, raw_reactions: str) -> int:
        reactions = cls.match_text_and_number(raw_reactions)
        if reactions == None or reactions == "":
            return 0
        last_word = reactions[-1]
        if not ('0' <= last_word and last_word <= '9'):
            number_part = reactions[:-1].replace(",", ".")
            if last_word.lower() == 'k':
                return int(float(number_part) * 1000)
            elif last_word.lower() == 'm':
                return int(float(number_part) * 1000000)
        else:
            return int(reactions)

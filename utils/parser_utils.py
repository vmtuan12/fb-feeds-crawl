import re
from datetime import datetime, timedelta

class ParserUtils():
    @classmethod
    def match_text_and_number(cls, text: str) -> str | None:
        pattern = r'[0-9a-zA-Z]+'
        match = re.search(pattern, text)
        if match:
            return match.group()
        
        return None
    
    @classmethod
    def approx_post_time_str(cls, now: datetime, raw_post_time: str) -> str | None:
        post_time = cls.match_text_and_number(raw_post_time)
        if post_time == None:
            return None
        
        if post_time.startswith("Jus"):
            return now.strftime("%Y-%m-%d %H:%M:%S")
        
        time_int = int(post_time[:-1])
        unit = post_time[-1]

        if unit == "m" or unit == "p":
            real_post_time = now - timedelta(minutes=time_int)
        elif unit == "h":
            real_post_time = now - timedelta(hours=time_int)

        return real_post_time.strftime("%Y-%m-%d %H:%M:%S")
    
    @classmethod
    def approx_reactions(cls, reactions: str) -> int:
        last_word = reactions[-1]
        if not ('0' <= last_word and last_word <= '9'):
            number_part = reactions[:-1].replace(",", ".")
            if last_word.lower() == 'k':
                return int(float(number_part) * 1000)
            elif last_word.lower() == 'm':
                return int(float(number_part) * 1000000)
        else:
            return int(reactions)
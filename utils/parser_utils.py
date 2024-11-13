import re
from datetime import datetime, timedelta

class ParserUtils():
    @classmethod
    def match_text_and_number(text: str) -> str | None:
        pattern = r'[0-9a-zA-Z]+'
        match = re.search(pattern, text)
        if match:
            return match.group()
        
        return None
    
    @classmethod
    def approx_post_time_str(now: datetime, post_time: str) -> str | None:
        if post_time == "Just now":
            return str(now)
        
        time_int = int(post_time[:-1])
        unit = post_time[-1]

        if unit == "m" or unit == "p":
            real_post_time = now - timedelta(minutes=time_int)
        elif unit == "h":
            real_post_time = now - timedelta(hours=time_int)

        return str(real_post_time)
    
    @classmethod
    def approx_reactions(reactions: str) -> int:
        last_word = reactions[-1]
        if not ('0' <= last_word and last_word <= '9'):
            if last_word.lower() == 'k':
                return int(reactions[:-1]) * 1000
            elif last_word.lower() == 'm':
                return int(reactions[:-1]) * 1000000
        else:
            return int(reactions)
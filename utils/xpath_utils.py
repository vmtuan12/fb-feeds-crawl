class FbPageXpathUtils():
    XPATH_TEXT = '//*[(contains(@class,"m bg-s") or contains(@class,"m displayed") or @class="m") and .//text() and not(@role) and not(.//*[@role="button"]) and not(./preceding-sibling::*[@role="img"]) and not(.//img) and (./following-sibling::*[1][.//img[@class="img contain"]] or (.//h1 and (./following-sibling::div or ./preceding-sibling::div)) or (.//div[contains(@style, "background-image") or contains(@style, "background-color")] and (./following-sibling::div or ./preceding-sibling::div)))]'
    # XPATH_TEXT_WITH_BG_IMG = '//div[not(@role) and (./following-sibling::div or ./preceding-sibling::div) and not(.//*[@role="button"]) and .//div[./img and not(.//text()) and @data-mcomponent="ImageArea" and ./following-sibling::div[@data-mcomponent="TextArea" and .//div[@class="fl ac"]]]]'
    XPATH_TEXT_WITH_BG_IMG = '//div[not(@role) and (./following-sibling::div or ./preceding-sibling::div) and not(.//*[@role="button"]) and .//div[./img and not(.//text()) and @data-mcomponent="ImageArea" and ./following-sibling::*[@data-mcomponent="TextArea" and .//div[@class="fl ac"]]]]'
    # XPATH_TEXT_WITH_LOAD_MORE = '//*[(contains(@class,"m bg-s") or @class="m") and .//text() and not(@role) and not(.//*[@role="button"]) and not(./preceding-sibling::*[@role="img"]) and not(.//img) and (./following-sibling::*[1][.//img[@class="img contain"]] or (.//h1 and (./following-sibling::div or ./preceding-sibling::div)) or (.//div[contains(@style, "background-image") or contains(@style, "background-color")] and (./following-sibling::div or ./preceding-sibling::div)))]//div[text() and ./span[not(@role)]]'
    XPATH_TEXT_WITH_LOAD_MORE = '//*[(contains(@class,"m bg-s") or @class="m") and .//text() and not(@role) and not(.//*[@role="button"]) and not(./preceding-sibling::*[@role="img"]) and not(.//img) and (./following-sibling::*[1][.//img[@class="img contain"]] or (.//h1 and (./following-sibling::div or ./preceding-sibling::div)) or (.//div[contains(@style, "background-image") or contains(@style, "background-color")] and (./following-sibling::div or ./preceding-sibling::div))) and .//*[contains(text(), "See more")]]'
    
    XPATH_LOAD_MORE_EL_TO_CLICK = './/div[./span[not(@role)] and .//text()]'
    """
    This is to find images along with each post text
    """
    XPATH_ADDITIONAL_IMAGES = './following-sibling::*[1]//img[@class="img contain"]'
 
    """
    This is to find reaction count along with each post text
    """
    XPATH_ADDITIONAL_REACTION = './/following-sibling::div[.//div[@role="button"]]//div[@role="button" and not(./img)]'
    # XPATH_ADDITIONAL_REACTION = './following-sibling::div[.//div[@class="fl ac am"]][1]//div[@class="fl ac am"][1]//span[last()]'
    XPATH_ADDITIONAL_REACTION_ALTER = './following-sibling::div[.//div[@role="button"]][1]//div[text()][last()]'
 
    """
    This is to find post time along with each post text
    """
    XPATH_ADDITIONAL_POST_TIME = './preceding-sibling::*[1]//div[not(@data-focusable)]//div[@data-mcomponent="TextArea"]//span[not(@role)]'
    XPATH_ADDITIONAL_POST_TIME_ALTER = './preceding-sibling::*[.//div[contains(@data-mcomponent, "TextArea")]][1]//div[contains(@data-mcomponent, "TextArea")][last()]//span[not(@role)]'

class FbPageDesktopXpathUtils():
    XPATH_TEXT = "//*[@role='main']//*[@data-ad-rendering-role='story_message']"
    XPATH_SEE_MORE_BTN = ".//*[@role='button' and contains(text(), 'See more')]"
    XPATH_ADDITIONAL_POST_TIME = ".//ancestor::*[2]/preceding-sibling::*[.//a[@attributionsrc]]//a[@attributionsrc and not(starts-with(@href, 'http')) and not(starts-with(@href, '/'))]"
    XPATH_ADDITIONAL_IMAGES = "./parent::*/following-sibling::*//img"
    XPATH_ADDITIONAL_REACTION = ".//ancestor::*[2]/following-sibling::*[.//*[@aria-label='See who reacted to this' and @role='toolbar']]//*[@aria-label='See who reacted to this' and @role='toolbar']/following-sibling::*[@aria-hidden='true']"
    XPATH_TOOLTIP = "//*[@role='tooltip']"


    XPATH_DIALOG_CLOSE = "//*[@role='dialog']//*[@aria-label='Close']"
    XPATH_OBSTABLES = [
        '//*[@role="banner"]',
        '//div[@data-nosnippet]',
        '//div[contains(@style, "top") and contains(@style, "z-index: auto")]'
    ]
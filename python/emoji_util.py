import emoji
import re
import regex

hex_head_rx = re.compile("^0x")

def get_emojis(txt):
    lst = []

    # https://stackoverflow.com/questions/43146528/how-to-extract-all-the-emojis-from-text/43146653
    data = regex.findall(r'\X', txt)
    unicode_emoji = emoji.UNICODE_EMOJI['en'] if 'en' in emoji.UNICODE_EMOJI else emoji.UNICODE_EMOJI
    for word in data:
        if any(char in unicode_emoji for char in word):
            lst.append(word)

    return lst


def get_emoji_hex(emo):
    return [ "U+" + hex_head_rx.sub("", hex(ord(e))) for e in emo ]

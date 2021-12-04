import collections
import re

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

Personage = collections.namedtuple('Personage', 'presentation_name query_name birth_year')

def normalize_name(raw):
    name = name_char_rx.sub("", raw.strip())
    return name.lower()

def make_personage(speaker_name, birth_year):
    return Personage(presentation_name=speaker_name, query_name=normalize_name(speaker_name), birth_year=birth_year)

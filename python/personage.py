import collections

Personage = collections.namedtuple('Personage', 'presentation_name query_name birth_year')

def make_personage(speaker_name, birth_year):
    return Personage(presentation_name=speaker_name, query_name=speaker_name.lower(), birth_year=birth_year)

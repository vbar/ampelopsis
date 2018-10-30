from urllib.parse import quote
import re
from common import space_rx

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

def normalize_name(name):
    return name_char_rx.sub("", name.strip())

# not the same as in common because it also needs to reflect curl
# canonicalization...
def normalize_url_component(path):
    q = quote(path)
    return space_rx.sub('+', q)

def make_position_set(detail):
    sought = set()
    lst = detail['workingPositions']
    for it in lst:
        if it['organization'] == 'Nejvyšší státní zastupitelství':
            sought.add('Q26197430')

        wp = it['workingPosition']
        if wp['name'] == 'člen řídícího orgánu':
            if it['organization'] == 'Univerzita Karlova':
                # not necessarily a rector, but no other positions were seen for this match
                sought.add('Q12049166')
                sought.add('Q212071')
                sought.add('Q2113250')
            elif it['organization'] == 'Masarykova univerzita':
                sought.add('Q212071')
                sought.add('Q2113250')

        if wp['name'] == 'vedoucí zaměstnanec 3. stupně řízení':
            if (it['organization'] == 'Kancelář prezidenta republiky'):
                sought.add('Q15712674')
            else:
                sought.add('Q1162163')

        if wp['name'] == 'člen vlády':
            if it['organization'] == 'Ministerstvo dopravy':
                sought.add('Q45754140')
            elif it['organization'] == 'Ministerstvo financí':
                sought.add('Q2207925')
            elif it['organization'] == 'Ministerstvo kultury':
                sought.add('Q45843918')
            elif it['organization'] in ( 'Ministrstvo obrany', 'Ministerstvo obrany' ): # typo in input
                sought.add('Q45406265')
            elif it['organization'] == 'Ministerstvo pro místní rozvoj':
                sought.add('Q25515749')
            elif it['organization'] == 'Ministerstvo práce a sociálních věcí':
                sought.add('Q27479703')
            elif it['organization'].startswith('Ministerstvo průmyslu a obchodu'):
                sought.add('Q25507811')
            elif it['organization'] == 'Ministerstvo spravedlnosti':
                sought.add('Q26197353')
                sought.add('Q1661653') # minister is correct, but ministry also occurs
            elif it['organization'] == 'Ministerstvo školství, mládeže a tělovýchovy':
                sought.add('Q30312984')
            elif it['organization'] == 'Ministerstvo vnitra':
                sought.add('Q20058795')
            elif it['organization'] == 'Ministerstvo zahraničních věcí':
                sought.add('Q2501396')
            elif it['organization'] == 'Ministerstvo zdravotnictví':
                sought.add('Q45750372')
            elif it['organization'] == 'Ministerstvo zemědělství':
                sought.add('Q28808262')
            elif it['organization'] == 'Ministerstvo životního prostředí':
                sought.add('Q33702144')
            else: # premier doesn't have a specific position in input data
                sought.add('Q3409229')
                sought.add('Q140686') # let's try also generic chairperson
                sought.add('Q83307') # ...and minister

        if wp['name'] == 'náměstek pro řízení sekce':
            sought.add('Q15735113')
        elif wp['name'] == 'starosta':
            sought.add('Q30185')
            sought.add('Q147733')
            if it['organization'] == 'Město Třebíč':
                sought.add('Q28860110')
            # missing Q17149373, Q28860819 & probably others
        elif wp['name'] == 'místostarosta / zástupce starosty':
            sought.add('Q581817')
        elif wp['name'] in ( 'člen zastupitelstva', 'člen Rady' ):
            sought.add('Q708492')
            sought.add('Q19602879')
            sought.add('Q4657217')
        elif wp['name'] == 'člen bankovní rady České národní banky': # missing the governor
            sought.add('Q28598459')
        elif (wp['name'] == 'soudce'):
            sought.add('Q16533')
        elif (wp['name'] == 'ředitel bezpečnostního sboru'):
            sought.add('Q1162163')
        elif wp['name'] == 'vedoucí zastupitelského úřadu':
            sought.add('Q121998')

        if (wp['name'] == 'poslanec') or wp['deputy']:
            sought.add('Q1055894')
            sought.add('Q19803234') # should be a subset but isn't
            sought.add('Q486839')

        if (wp['name'] == 'senátor') or wp['senator']:
            sought.add('Q15686806')
            sought.add('Q18941264')
            sought.add('Q486839')

    return sought

def make_query_url(detail, position_set):
    name = "%s %s" % tuple(normalize_name(detail[n]) for n in ('firstName', 'lastName'))
    name_clause = 'filter(contains(?l, "%s")).' % name

    if len(position_set):
        vl = ' '.join('wd:' + p for p in sorted(position_set))
        pos_clause = 'values ?p { %s }' % vl
    else:
        # no restriction
        pos_clause = ''

    # person, article, birth, label, position
    query = """select ?w ?a ?b ?l ?p
where {
        ?w wdt:P27 wd:Q213;
                rdfs:label ?l;
                wdt:P39 ?p;
                wdt:P569 ?b.
        ?a schema:about ?w.
        ?a schema:inLanguage "cs".
        filter(lang(?l) = "cs").
        %s %s
}""" % (name_clause, pos_clause)
    mq = re.sub("\\s+", " ", query.strip())
    return "https://query.wikidata.org/sparql?format=json&query=" + normalize_url_component(mq)

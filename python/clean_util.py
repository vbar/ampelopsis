import re

nbsp_rx = re.compile('(?:\xa0|\\s)+')

title_before_rx = re.compile('^(?:bc|doc|ing(?:[.] et ing)?|judr|lic|mgr(?:[.] et mgr)?|m[uv]dr|paeddr|phdr|prof|r[ns]dr)[.]\\s+(.+)$', re.IGNORECASE)

title_after_rx = re.compile('^([^,]+),')

def clean_text_node(raw_text):
    text = nbsp_rx.sub(' ', raw_text)
    return text.strip()


def clean_text(text_nodes):
    texts = []
    for raw_text in text_nodes:
        ct = clean_text_node(raw_text)
        if ct:
            texts.append(ct)

    return " ".join(texts)


def do_clean_title(text):
    if not text:
        return None

    m = title_before_rx.match(text)
    while m:
        raw = m.group(1)
        text = raw.lstrip()
        m = title_before_rx.match(text)

    m = title_after_rx.match(text)
    if m:
        core = m.group(1)
        return core.rstrip()
    else:
        return text


def clean_title_node(raw_text):
    return do_clean_title(clean_text_node(raw_text))


def clean_title(text_nodes):
    return do_clean_title(clean_text(text_nodes))

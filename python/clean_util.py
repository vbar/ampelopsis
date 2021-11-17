import re

nbsp_rx = re.compile('(?:\xa0|\\s)+')

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

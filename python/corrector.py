# from https://norvig.com/spell-correct.html
def edits1(word):
    "All edits that are one edit away from `word`."
    letters = 'aábcčdďeéěfghiíjklľmnňoópqrřsštťuúůvwxyýzž'
    splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    deletes = [L + R[1:] for L, R in splits if R]
    transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R)>1]
    replaces = [L + c + R[1:] for L, R in splits if R for c in letters]
    inserts = [L + c + R for L, R in splits for c in letters]
    return set(deletes + transposes + replaces + inserts)

def edits(word_set):
    a = set()
    for word in word_set:
        a |= edits1(word)

    return a

class Corrector:
    def __init__(self, n, di):
        if n < 1:
            raise Exception("max number of corrections must be positive")

        self.d = {}
        for w in di:
            appro = edits1(w)
            i = 1
            while i < n:
                appro = edits(appro)
                i += 1

            self.d[w] = appro

    def is_correct(self, w):
        return w in self.d

    def match(self, w):
        m = set()
        for k, v in self.d.items():
            if w in v:
                m.add(k)

        return m

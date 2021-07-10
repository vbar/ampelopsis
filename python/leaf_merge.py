import sys
from json_compare import stringify
from leaf_load import LeafLoader

class LeafMerger(LeafLoader):
    def __init__(self, cur):
        LeafLoader.__init__(self, cur)

    def merge(self, person_id, person_doc):
        if not self.merge_leaves:
            return

        statements = person_doc.get('statements')
        if isinstance(statements, list):
            for i, statement in enumerate(statements):
                if isinstance(statement, dict):
                    statement_id = statement.get('id')
                    if statement_id and self.id_rx.match(statement_id):
                        statement_doc = self.load_statement(person_id, statement_id)
                        if statement_doc:
                            self.merge_statement(statement, i, statement_doc)

    def merge_statement(self, statement, i, extra_statement):
        if not isinstance(extra_statement, dict):
            raise Exception("%s has unexpected type" % extra_statement)

        for k, v in extra_statement.items():
            if k != 'official': # this seems to just repeat the person data...
                if not k in statement:
                    statement[k] = v
                elif k == 'id':
                    if statement[k] != v:
                        raise Exception("ID changed")
                else:
                    self.merge_object("[%d]/%s" % (i, k), statement[k], v)

    def merge_object(self, path, target, extra):
        if isinstance(extra, dict):
            if not isinstance(target, dict):
                print("skipping %s - types diverge" % path, file=sys.stderr)
                return

            for k, v in extra.items():
                if not k in target:
                    target[k] = v
                else:
                    self.merge_object(path + "/" + k, target[k], v)
        elif isinstance(extra, list):
            if not isinstance(target, list):
                print("skipping %s - types diverge" % path, file=sys.stderr)
                return

            target.sort(key=stringify)
            extra.sort(key=stringify)

            new_target = []
            m = len(target)
            n = len(extra)
            ml = min(m, n)
            i = 0
            j = 0
            while (i < ml) and (j < ml):
                c = target[i]
                d = extra[j]
                if c == d:
                    new_target.append(c)
                    i += 1
                    j += 1
                else:
                    a = stringify(c)
                    b = stringify(d)
                    if a < b:
                        new_target.append(c)
                        i += 1
                    else:
                        assert a > b
                        new_target.append(d)
                        j += 1

            if i < m:
                new_target.extend(target[i:m])

            if j < n:
                new_target.extend(extra[j:n])

            target.clear()
            target.extend(new_target)
        else:
            if target != extra:
                print("skipping %s - %s != %s" % (path, target, extra), file=sys.stderr)

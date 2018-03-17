#!/usr/bin/python3

from common import make_connection
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class EqClass:
    def __init__(self, id1, id2):
        self.data = { id1, id2 }
            
    def yank(self):
        data = self.data
        self.data = None
        return data
        
class Builder(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.id2ec = {}

    def add(self, id1, id2):
        ec1 = self.id2ec.get(id1)
        if not ec1:
            ec1 = EqClass(id1, id2)
            self.id2ec[id1] = ec1
        else:
            ec1.data.add(id2)

        ec2 = self.id2ec.get(id2)
        if not ec2:
            self.id2ec[id2] = ec1
        else:
            self.merge(ec1, ec2)

    def dump(self):
        len2datalist = {}
        for i, ec in self.id2ec.items():
            data = ec.yank()
            if data:
                sz = len(data)
                datalist = len2datalist.get(sz)
                if not datalist:
                    datalist = []
                    len2datalist[sz] = datalist
                    
                datalist.append(data)

        for sz, datalist in sorted(len2datalist.items(), key=lambda kv: kv[0], reverse=True):
            for data in datalist:
                for i in sorted(data):
                    row = self.get_url(i)
                    ln = row[0]
                    if self.has_body(i):
                        ln = "!" + ln
                        
                    print(ln)
                    
                print("")

    def get_url(self, i):
        self.cur.execute("""select url
from field
where id=%s""", (i,))
        return self.cur.fetchone()
    
    def has_body(self, url_id):
        volume_id = self.get_volume_id(url_id) 
        f = self.open_page(url_id, volume_id)
        if f is None:
            return False
        else:
            f.close()
            return True
        
    def merge(self, ec1, ec2):
        ec1.data |= ec2.data
        for i in ec2.data:
            self.id2ec[i] = ec1
            
def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            builder = Builder(cur)
            cur.execute("""select from_id, to_id from redirect order by from_id, to_id""")
            rows = cur.fetchall()
            for row in rows:
                builder.add(*row)

            builder.dump()
    
if __name__ == "__main__":
    main()
            

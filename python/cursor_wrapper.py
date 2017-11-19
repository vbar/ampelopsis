class CursorWrapper:
    def __init__(self, cur):
        self.cur = cur

    def get_url(self, url_id):
        self.cur.execute("""select url
from field
where id=%s""", (url_id,))
        row = self.cur.fetchone()
        return row[0] if row else None

    def get_volume_id(self, url_id):
        self.cur.execute("""select volume_id
from content
join directory on volume_id=directory.id
where written is not null and url_id=%s""", (url_id,))
        row = self.cur.fetchone()
        return row[0] if row else None
        
    
        

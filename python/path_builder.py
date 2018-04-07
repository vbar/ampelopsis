from cursor_wrapper import CursorWrapper

class PathBuilder(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        
    def get_parents(self, urls, child_depth):
        self.cur.execute("""select distinct from_id
from edges
join nodes on from_id=url_id
where to_id in %s and depth=%s
order by from_id""", (tuple(urls), child_depth - 1))
        rows = self.cur.fetchall()
        return [ row[0] for row in rows ]
    

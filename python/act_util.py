def act_inc(cur):
    cur.execute("""insert into activity(id, total_count)
values(1, 1)
on conflict(id) do update
set total_count=activity.total_count + 1""")

def act_dec(cur):
    cur.execute("""update activity
set total_count=total_count - 1
where id=1
returning total_count""")
    row = cur.fetchone()
    return row[0] > 0

def act_reset(cur):
    cur.execute("""insert into activity(id, total_count)
values(1, 0)
on conflict(id) do update
set total_count=0""")

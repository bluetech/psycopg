import psycopg

with psycopg.Connection.connect("dbname=ran") as conn:
    for ev in conn.notifies():
        print(ev)

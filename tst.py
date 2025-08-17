import psycopg

with psycopg.Connection.connect("dbname=ran") as conn:
    for ev in conn.subscribe(
        ["ranpub"],
        slot="rantest",
        create_slot=False,
        messages=True,
    ):
        print(ev)

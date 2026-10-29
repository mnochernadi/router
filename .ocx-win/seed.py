import os, shutil, sqlite3, sys

SRC = os.path.join(os.environ["USERPROFILE"], ".local", "share", "opencode")
DST = r"C:\router\.ocx-win\data\opencode"

os.makedirs(DST, exist_ok=True)
for f in ("opencode.db", "opencode.db-wal", "opencode.db-shm"):
    s = os.path.join(SRC, f)
    if os.path.exists(s):
        shutil.copy2(s, os.path.join(DST, f))
        print("copied", f)

db = os.path.join(DST, "opencode.db")
con = sqlite3.connect(db)
cur = con.cursor()
tables = [r[0] for r in cur.execute(
    "select name from sqlite_master where type='table' and (name like '%account%' or name='credential')")]
print("credential tables:", tables)
for t in tables:
    if t in ("account", "account_state", "control_account", "credential"):
        n = cur.execute(f"select count(*) from {t}").fetchone()[0]
        cur.execute(f"delete from {t}")
        print(f"cleared {t} ({n} rows)")
con.commit()
con.close()
print("done")

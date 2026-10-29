"""strip.py -- force the isolated lane back to anonymous.

Deletes every credential-bearing row from the lane's own database so a run can
never silently ride an account. Cheap (no copy), so it runs before every `run`.
"""
import os, sqlite3, sys

db = os.path.join(r"C:\router\.ocx-win", "data", "opencode", "opencode.db")
if not os.path.exists(db):
    sys.exit(0)

con = sqlite3.connect(db)
cur = con.cursor()
names = [r[0] for r in cur.execute("select name from sqlite_master where type='table'")]
removed = 0
for t in ("credential", "account", "account_state", "control_account"):
    if t in names:
        n = cur.execute(f"select count(*) from {t}").fetchone()[0]
        if n:
            cur.execute(f"delete from {t}")
            removed += n
con.commit()
con.close()
if removed:
    print(f"stripped {removed} credential row(s)")

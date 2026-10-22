import os, posixpath, sys
import paramiko
HOST=os.environ.get("ROUTER_SSH_HOST","65.109.179.95"); PORT=int(os.environ.get("ROUTER_SSH_PORT","22"))
USER=os.environ.get("ROUTER_SSH_USER","root"); PASS=os.environ.get("ROUTER_SSH_PASS","")
def run(cmd, timeout=900):
    c=paramiko.SSHClient(); c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(HOST,port=PORT,username=USER,password=PASS,timeout=25,banner_timeout=25,auth_timeout=25,look_for_keys=False,allow_agent=False)
    try:
        i,o,e=c.exec_command(cmd,timeout=timeout); i.close()
        out=o.read().decode("utf-8","replace"); err=e.read().decode("utf-8","replace"); rc=o.channel.recv_exit_status()
    finally: c.close()
    sys.stdout.write(out)
    if err: sys.stdout.write("\n--- stderr ---\n"+err)
    sys.stdout.write(f"\n[exit {rc}]\n"); return rc
if __name__=="__main__":
    try: sys.stdout.reconfigure(encoding="utf-8",errors="replace")
    except Exception: pass
    sys.exit(run(os.environ.get("ROUTER_SSH_CMD","")))

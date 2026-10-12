"""Portable supervisor for a public dashboard plus private local API."""
import os, signal, socket, subprocess, sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parent
children = {}
stop_requested = False
API_HOST = "127.0.0.1"
PUBLIC_PORT = int(os.environ.get("PORT", os.environ.get("WEB_PORT", "20335")))
# Keep the private API listener separate from Railway's public HTTP port.
_requested_api_port = int(os.environ.get("BR_API_PORT", "18080"))
if _requested_api_port == PUBLIC_PORT:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _probe:
        _probe.bind((API_HOST, 0))
        API_PORT = _probe.getsockname()[1]
    print(f"[RUNNER] BR_API_PORT matched public PORT; selected private port {API_PORT}", flush=True)
else:
    API_PORT = _requested_api_port

def start(name, script):
    path = ROOT / script
    if not path.is_file(): raise FileNotFoundError(f"Required file missing: {script}")
    env = os.environ.copy()
    env["BR_API_PORT"] = str(API_PORT)
    env["BR_API_BASE"] = f"http://{API_HOST}:{API_PORT}"
    if name == "MAIN": env["PORT"] = str(PUBLIC_PORT)
    proc = subprocess.Popen([sys.executable, "-u", str(path)], cwd=str(ROOT), env=env)
    children[name] = proc
    print(f"[RUNNER] {name} started pid={proc.pid}", flush=True)
    return proc

def wait_port(proc, host, port, label, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        if proc.poll() is not None: raise RuntimeError(f"{label} exited (code {proc.returncode})")
        try:
            with socket.create_connection((host, port), timeout=1):
                print(f"[RUNNER] {label} ready at {host}:{port}", flush=True); return
        except OSError: time.sleep(.5)
    raise RuntimeError(f"{label} did not listen on {host}:{port} within {timeout}s")

def stop_all(*_):
    global stop_requested
    stop_requested = True
    for proc in children.values():
        if proc.poll() is None: proc.terminate()
    deadline = time.time() + 10
    for proc in children.values():
        if proc.poll() is None:
            try: proc.wait(timeout=max(.1, deadline-time.time()))
            except subprocess.TimeoutExpired: proc.kill()

def main():
    signal.signal(signal.SIGTERM, stop_all); signal.signal(signal.SIGINT, stop_all)
    print(f"[RUNNER] public dashboard bind=0.0.0.0:{PUBLIC_PORT}; private API={API_HOST}:{API_PORT}", flush=True)
    try:
        api = start("API", "api.py"); wait_port(api, API_HOST, API_PORT, "BR API")
        main_proc = start("MAIN", "main.py"); wait_port(main_proc, "127.0.0.1", PUBLIC_PORT, "Dashboard")
        while not stop_requested:
            if main_proc.poll() is not None: raise RuntimeError(f"MAIN exited (code {main_proc.returncode})")
            if api.poll() is not None:
                print(f"[RUNNER] API exited (code {api.returncode}); restarting", flush=True)
                api = start("API", "api.py"); wait_port(api, API_HOST, API_PORT, "BR API restart")
            time.sleep(2)
    except Exception as exc:
        print(f"[RUNNER] FATAL: {exc}", flush=True); stop_all(); return 1
    stop_all(); return 0
if __name__ == "__main__": raise SystemExit(main())

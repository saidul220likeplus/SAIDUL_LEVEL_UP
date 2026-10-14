# auth_layer.py
# -*- coding: utf-8 -*-

import os
import json
import time
import math
import asyncio
import hashlib
import secrets
import contextvars
import httpx
from typing import Dict, Any, Optional

from aiohttp import web

import dashboard_server as ds

bot_state = ds.bot_state

from templates import (
    LOGIN_HTML,
    PLANS_HTML,
    DASHBOARD_HTML,
    EXPIRED_HTML,
    ADMIN_LOGIN_HTML,
    ADMIN_PANEL_HTML,
    HOME_HTML,
    ADD_NEW_JOB_HTML,
)

ADMIN_USERNAME = "saidul4758"
ADMIN_PASSWORD = "saidul4758"
ADMIN_PATH = "/saidul4758"

DATA_DIR = "data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
SESSIONS_FILE = os.path.join(DATA_DIR, "sessions.json")

SESSION_COOKIE = "vxl_session"
SESSION_TTL = 7 * 24 * 3600

USER_WORKERS: Dict[str, Dict[str, asyncio.Task]] = {}
MAX_WORKERS_PER_USER = max(1, min(1000, int(os.getenv("MAX_WORKERS_PER_USER", "1000"))))
USER_PAUSED: Dict[str, set] = {}
USER_DELETED: Dict[str, set] = {}
USER_BULK_PAUSED = set()
USER_MANUAL_PAUSED = set()
# Account keys that were individually paused before a bulk Stop All.
USER_BULK_PREPAUSED: Dict[str, set] = {}
USER_HOURLY_RESTART_TARGETS: Dict[str, set] = {}
USER_BULK_PAUSED_TIME: Dict[str, float] = {}
BULK_PAUSE_TIMEOUT = 60

# ============================================================
# AUTO STOP → RESTART CONFIG  ✅ 1 HOUR
# ============================================================
AUTO_RESTART_INTERVAL = max(60, int(os.getenv("AUTO_RESTART_INTERVAL", "1920")))
AUTO_RESTART_WAIT = max(2, int(os.getenv("AUTO_RESTART_WAIT", "10")))
SCHEDULER_TICK = max(5, int(os.getenv("AUTO_SCHEDULER_TICK", "10")))

# ============================================================
# AUTO CYCLE STATE
# ============================================================
USER_AUTO_CYCLE_STATE: Dict[str, Dict[str, Any]] = {}

_LAST_SYNC: Dict[str, float] = {}
_CURRENT_OWNER = contextvars.ContextVar("current_owner", default=None)

BR_API_BASE = os.getenv("BR_API_BASE", "http://127.0.0.1:8080").strip().rstrip("/")
BR_START_PATH = "/BattelRoyal"
BR_STOP_PATH = "/stop"
BR_STATUS_PATH = "/status"

FF_LEVEL_TOTAL_EXP = [
    0, 48, 202, 544, 1012, 1844, 2792, 3800, 4870, 6004,
    7192, 8448, 9760, 11140, 12566, 14060, 15610, 17224, 18902, 20632,
    22424, 24278, 26192, 28166, 30200, 32294, 34448, 37804, 41274, 44870,
    48582, 53394, 58566, 64096, 69994, 76260, 83506, 91128, 99322, 108092,
    120144, 133266, 147472, 162760, 179126, 196572, 215368, 235316, 257010, 279860,
    304056, 348318, 394982, 444044, 495508, 549364, 633756, 721744, 813336, 908522,
    1041438, 1180352, 1325266, 1476184, 1634300, 1840946, 2056594, 2281242, 2514880, 2757530,
    3059506, 3372284, 3699456, 4041030, 4397002, 4829104, 5282204, 5756304, 6251404, 6767502,
    7381324, 8043154, 8752982, 9510808, 10316638, 11277190, 12291748, 13360304, 14482858, 15659418,
    17026708, 18453990, 19941280, 21488570, 23095858, 24763138, 26490428, 28277708, 30124996, 32032284,
]


def _level_progress(level: int, current_exp: int) -> Dict[str, Any]:
    try:
        lvl = max(1, min(100, int(level or 1)))
        exp = max(0, int(current_exp or 0))
    except (TypeError, ValueError):
        return {"percent": 0, "remaining_exp": None, "next_level": None, "level_exp": 0, "required_exp": 0, "known": False}
    if lvl >= 100:
        return {"percent": 100, "remaining_exp": 0, "next_level": 100, "level_exp": 0, "required_exp": 0, "known": True}
    start = FF_LEVEL_TOTAL_EXP[lvl - 1]
    end = FF_LEVEL_TOTAL_EXP[lvl]
    needed = max(1, end - start)
    progressed = max(0, min(needed, exp - start))
    pct = round((progressed / needed) * 100, 1)
    return {"percent": pct, "remaining_exp": max(0, end - exp), "next_level": lvl + 1, "level_exp": progressed, "required_exp": needed, "known": True}


_mode_registry: Dict[str, str] = {}
_mode_lock = asyncio.Lock()


async def _set_mode(uid: str, mode: str):
    async with _mode_lock:
        _mode_registry[str(uid)] = mode


async def _get_mode(uid: str) -> Optional[str]:
    async with _mode_lock:
        return _mode_registry.get(str(uid))


async def _clear_mode(uid: str):
    async with _mode_lock:
        _mode_registry.pop(str(uid), None)


async def _br_request(path: str, uid: str = "", pw: str = "", accs: str = "", timeout: float = 30.0) -> Dict[str, Any]:
    params = {}
    if accs:
        params["accs"] = accs
    else:
        params["uid"] = uid
        params["pw"] = pw
    try:
        async with httpx.AsyncClient(timeout=timeout) as c:
            r = await c.get(BR_API_BASE + path, params=params)
            try:
                return r.json()
            except Exception:
                return {"Sorry": "Invalid JSON response", "raw": r.text[:200]}
    except Exception as e:
        return {"Sorry": "API Unreachable", "error": str(e)}


async def _br_status_ping() -> Dict[str, Any]:
    start = time.time()
    if not BR_API_BASE:
        return {"ok": False, "latency": 0, "error": "BR_API_BASE is not configured"}
    try:
        async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as c:
            r = await c.get(BR_API_BASE + "/health")
            latency = int((time.time() - start) * 1000)
            if r.status_code == 200 and r.json().get("ok") is True:
                return {"ok": True, "latency": latency, "http_status": r.status_code}
            return {"ok": False, "latency": latency, "http_status": r.status_code, "error": "Health check did not return ok"}
    except Exception as e:
        return {"ok": False, "latency": 0, "error": f"{type(e).__name__}: {e}"}


# ✅ SAFE CANCEL HELPER — wait_for এর hang bug এড়ায়
async def _safe_cancel_tasks(tasks, timeout: float = 5.0):
    """Cancel tasks with timeout that never hangs forever (uses asyncio.wait, not gather+wait_for)."""
    if not tasks:
        return
    for t in tasks:
        if t and not t.done():
            try:
                t.cancel()
            except Exception:
                pass
    try:
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for p in pending:
            if not p.done():
                try:
                    p.cancel()
                except Exception:
                    pass
    except Exception as e:
        print(f"[safe_cancel] error: {type(e).__name__}: {e}", flush=True)


async def sync_user_accounts_to_disk(username):
    try:
        users = load_users()
        u = users.get(username)
        if not u:
            return
        accounts = u.get("accounts", [])
        if not accounts:
            return
        changed = False
        for saved in accounts:
            saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
            live = None
            for uid, acc in ds.bot_state.accounts.items():
                if acc.get("_owner") != username:
                    continue
                if str(uid) == saved_key:
                    live = acc
                    break
                cred = ds.bot_state.account_credentials.get(str(uid))
                if cred:
                    if saved.get("type") == "guest" and str(cred.get("auth_uid")) == saved_key:
                        live = acc
                        break
                    if saved.get("type") == "token" and str(cred.get("auth_token", ""))[:20] == saved_key:
                        live = acc
                        break
            if not live:
                continue
            if saved.get("initial_exp") is None and live.get("initial_exp") is not None:
                saved["initial_exp"] = live["initial_exp"]
                changed = True
            new_last_exp = live.get("current_exp")
            if new_last_exp is not None and saved.get("last_exp") != new_last_exp:
                saved["last_exp"] = new_last_exp
                changed = True
            new_level = live.get("level")
            if new_level is not None and saved.get("last_level") != new_level:
                saved["last_level"] = new_level
                changed = True
            new_nick = live.get("nickname")
            if new_nick and saved.get("last_nickname") != new_nick:
                saved["last_nickname"] = new_nick
                changed = True
            new_region = live.get("region")
            if new_region and saved.get("last_region") != new_region:
                saved["last_region"] = new_region
                changed = True
            new_gained = live.get("gained_exp")
            if new_gained is not None and saved.get("gained_exp") != new_gained:
                saved["gained_exp"] = new_gained
                changed = True
            new_exp_progress_at = live.get("last_exp_progress_at")
            if new_exp_progress_at is not None and saved.get("last_exp_progress_at") != new_exp_progress_at:
                saved["last_exp_progress_at"] = new_exp_progress_at
                changed = True
            new_played = live.get("matches_played")
            if new_played is not None and saved.get("matches_played") != new_played:
                saved["matches_played"] = new_played
                changed = True
            if live.get("created_at") and saved.get("created_at") != live.get("created_at"):
                saved["created_at"] = live.get("created_at")
                changed = True
            saved["last_update"] = time.time()
            changed = True
        if changed:
            save_users(users)
    except Exception as e:
        print(f"[auth] sync_user_accounts_to_disk error for {username}: {e}", flush=True)


def _ensure():
    os.makedirs(DATA_DIR, exist_ok=True)


def _read(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write(path, data):
    _ensure()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def load_users():
    return _read(USERS_FILE, {})


def save_users(u):
    _write(USERS_FILE, u)


PENDING_IMPORT_FILE = os.path.join(DATA_DIR, "pending_imports.json")


def load_pending_imports():
    data = _read(PENDING_IMPORT_FILE, {})
    return data if isinstance(data, dict) else {}


def save_pending_imports(data):
    _write(PENDING_IMPORT_FILE, data)


def load_sessions():
    return _read(SESSIONS_FILE, {})


def save_sessions(s):
    _write(SESSIONS_FILE, s)


def hash_pw(pw, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 120000)
    return h.hex(), salt


def verify_pw(pw, h, salt):
    hh, _ = hash_pw(pw, salt)
    return secrets.compare_digest(hh, h)


def create_session(username, is_admin=False):
    token = secrets.token_urlsafe(32)
    s = load_sessions()
    now = time.time()
    s[token] = {"username": username, "is_admin": is_admin, "created_at": now, "expires_at": now + SESSION_TTL}
    now_ts = time.time()
    s = {k: v for k, v in s.items() if v.get("expires_at", 0) > now_ts}
    save_sessions(s)
    return token


def get_session(token):
    if not token:
        return None
    s = load_sessions()
    v = s.get(token)
    if not v:
        return None
    if time.time() > v.get("expires_at", 0):
        s.pop(token, None)
        save_sessions(s)
        return None
    return v


def destroy_session(token):
    s = load_sessions()
    s.pop(token, None)
    save_sessions(s)


def cur_user(req):
    return get_session(req.cookies.get(SESSION_COOKIE))


def cur_admin(req):
    s = cur_user(req)
    if s and s.get("is_admin"):
        return s
    return None


def _set_ck(resp, token):
    resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite="Lax", path="/")


def _clr_ck(resp):
    resp.del_cookie(SESSION_COOKIE, path="/")


_orig_register = ds.BotState.register_account


def _patched_register(self, uid, nickname, region, level, exp, likes=0):
    uid_str = str(uid)
    owner = _CURRENT_OWNER.get()

    if owner is None:
        try:
            for un, udata in load_users().items():
                for acc in udata.get("accounts", []):
                    if acc.get("type") == "guest":
                        acc_uid = str(acc.get("uid", ""))
                    else:
                        acc_uid = str(acc.get("token", ""))[:20]
                    if acc_uid == uid_str:
                        owner = un
                        break
                if owner:
                    break
        except Exception:
            pass

    if owner:
        deleted = USER_DELETED.get(owner, set())
        paused = USER_PAUSED.get(owner, set())
        if uid_str in deleted or uid_str in paused:
            return

    saved_initial = None
    saved_matches = 0
    saved_created = None
    saved_last_exp = None
    if owner:
        try:
            users = load_users()
            u = users.get(owner)
            if u:
                for saved in u.get("accounts", []):
                    saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
                    if saved_key == uid_str:
                        saved_initial = saved.get("initial_exp")
                        saved_matches = saved.get("matches_played", 0)
                        saved_created = saved.get("created_at")
                        saved_last_exp = saved.get("last_exp")
                        break
        except Exception:
            pass

    old_live = self.accounts.get(uid_str) if uid_str in self.accounts else None
    old_current_exp = int((old_live or {}).get("current_exp", exp) or exp)
    is_new = uid_str not in self.accounts
    _orig_register(self, uid, nickname, region, level, exp, likes)
    acc = self.accounts.get(uid_str)
    if not acc:
        return

    if saved_initial is not None:
        acc["initial_exp"] = saved_initial
        acc["gained_exp"] = max(0, exp - saved_initial)
    if saved_matches > 0:
        acc["matches_played"] = saved_matches
    if saved_created:
        acc["created_at"] = saved_created

    saved_progress_at = None
    if owner:
        try:
            users = load_users()
            u = users.get(owner)
            if u:
                for saved in u.get("accounts", []):
                    saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
                    if saved_key == uid_str:
                        saved_progress_at = saved.get("last_exp_progress_at")
                        break
        except Exception:
            saved_progress_at = None
    # Do not restore a stale saved timestamp over a real EXP increase that
    # happened during this registration/profile refresh.
    try:
        saved_exp_value = float(saved_last_exp) if saved_last_exp is not None else None
        current_exp_value = float(exp)
    except (TypeError, ValueError):
        saved_exp_value = None
        current_exp_value = 0.0
    if saved_exp_value is not None and current_exp_value > saved_exp_value:
        acc["last_exp_progress_at"] = time.time()
    elif saved_progress_at:
        try:
            acc["last_exp_progress_at"] = float(saved_progress_at)
        except Exception:
            acc["last_exp_progress_at"] = time.time()
    elif is_new or exp > old_current_exp:
        acc["last_exp_progress_at"] = time.time()
    else:
        acc.setdefault("last_exp_progress_at", time.time())

    if owner:
        acc["_owner"] = owner

    if is_new and "created_at" not in acc:
        acc["created_at"] = time.time()

    acc["last_update"] = time.time()

    if owner:
        try:
            users = load_users()
            u = users.get(owner)
            if u:
                for saved in u.get("accounts", []):
                    saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
                    if saved_key == uid_str:
                        if saved.get("initial_exp") is None:
                            saved["initial_exp"] = acc["initial_exp"]
                        saved["last_exp"] = acc["current_exp"]
                        saved["last_level"] = acc["level"]
                        saved["last_nickname"] = acc["nickname"]
                        saved["last_region"] = acc["region"]
                        saved["gained_exp"] = acc["gained_exp"]
                        saved["last_exp_progress_at"] = acc.get("last_exp_progress_at", time.time())
                        saved["matches_played"] = acc["matches_played"]
                        saved["created_at"] = acc.get("created_at")
                        saved["last_update"] = time.time()
                        break
                save_users(users)
        except Exception:
            pass


ds.BotState.register_account = _patched_register


# ✅ FIXED — uses _safe_cancel_tasks
async def stop_user_workers(username):
    workers = USER_WORKERS.pop(username, {})
    tasks = [t for t in workers.values() if t and not t.done()]

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(username + "::"):
            t = bot_state.account_workers.pop(key, None)
            if t and not t.done():
                tasks.append(t)

    if tasks:
        await _safe_cancel_tasks(tasks, timeout=5.0)

    await asyncio.sleep(0.2)


def _ensure_auto_restart_timer(username: str, users: dict = None):
    if users is None:
        users = load_users()
    u = users.get(username)
    if not u:
        return
    if float(u.get("last_auto_restart_at", 0) or 0) == 0:
        u["last_auto_restart_at"] = time.time()
        save_users(users)
        print(f"[auto] timer started user={username}", flush=True)


async def start_user_workers(username):
    users = load_users()
    u = users.get(username)
    if not u:
        return
    if time.time() >= u.get("expires_at", 0):
        return

    if username not in USER_WORKERS:
        USER_WORKERS[username] = {}
    if username not in USER_PAUSED:
        USER_PAUSED[username] = set()
    if username not in USER_DELETED:
        USER_DELETED[username] = set()

    if username in USER_BULK_PAUSED and username not in USER_HOURLY_RESTART_TARGETS:
        return

    workers = USER_WORKERS[username]
    paused = USER_PAUSED[username]
    deleted = USER_DELETED[username]
    max_workers = max(1, min(1000, int(u.get("slots", 1) or 1)))
    hourly_targets = USER_HOURLY_RESTART_TARGETS.get(username)
    active_count = sum(1 for task in workers.values() if task and not task.done())
    started_any = False

    for acc in u.get("accounts", []):
        key = acc.get("uid") if acc.get("type") == "guest" else acc.get("token", "")[:20]
        if not key:
            continue
        if key in paused or key in deleted:
            continue
        if hourly_targets is not None and key not in hourly_targets:
            continue

        old = workers.get(key)
        if old and not old.done():
            continue
        workers.pop(key, None)
        if active_count >= max_workers:
            continue

        mode = acc.get("mode", "LW")
        if acc.get("type") == "guest":
            t = asyncio.create_task(_guest_worker(username, acc["uid"], acc["password"], mode))
        else:
            t = asyncio.create_task(_token_worker(username, acc["token"], mode))
        workers[key] = t
        bot_state.account_workers[username + "::" + key] = t
        active_count += 1
        started_any = True

    if started_any:
        _ensure_auto_restart_timer(username, users)


async def _remove_finished_account(username: str, key: str, game_id: str = ""):
    try:
        users = load_users(); u = users.get(username)
        if u:
            u["accounts"] = [a for a in u.get("accounts", []) if not (str(a.get("uid", "")) == str(key) if a.get("type") == "guest" else str(a.get("token", ""))[:20] == str(key))]
            save_users(users)
        USER_DELETED.setdefault(username, set()).discard(str(key))
        USER_PAUSED.setdefault(username, set()).discard(str(key))
        for k in {str(key), str(game_id)}:
            if not k: continue
            ds.bot_state.accounts.pop(k, None)
            ds.bot_state.account_credentials.pop(k, None)
            await _clear_mode(k)
        USER_WORKERS.setdefault(username, {}).pop(str(key), None)
        ds.bot_state.account_workers.pop(username + "::" + str(key), None)
        print(f"[queue] removed finished account user={username} key={key}", flush=True)
        if username not in USER_BULK_PAUSED:
            asyncio.create_task(_fill_user_queue_once(username))
    except Exception as e:
        print(f"[queue] cleanup failed user={username} key={key}: {type(e).__name__}: {e}", flush=True)


async def _guest_worker(username, uid, password, mode="LW"):
    try:
        import main as m
    except Exception as e:
        print(f"[auth] Cannot import main: {e}", flush=True)
        return
    login_failure_started = time.time()
    while True:
        try:
            if uid in USER_DELETED.get(username, set()):
                break
            if uid in USER_PAUSED.get(username, set()):
                break
            cv = _CURRENT_OWNER.set(username)
            try:
                print(f"[auth] Guest login starting user={username} uid={uid} mode={mode}", flush=True)
                ad = await m.process_account_uid_pass(uid, password)
                if not ad:
                    print(f"[auth] Guest login FAILED user={username} uid={uid} -> retry in 15s", flush=True)
                    if time.time() - login_failure_started >= 30 * 60:
                        print(f"[queue] guest UID {uid} failed login for 30 minutes; removing from Auto Run", flush=True)
                        await _remove_finished_account(username, uid)
                        return
                    await asyncio.sleep(15)
                    continue

                login_failure_started = time.time()
                acc_id = str(ad.get("account_id", ""))
                if uid in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                    break
                if uid in USER_PAUSED.get(username, set()) or acc_id in USER_PAUSED.get(username, set()):
                    break

                level = int(ad.get("level", 1) or 1)
                if mode == "BR" and level >= 3:
                    mode = "LW"
                    await _set_mode(acc_id, "LW")
                    _update_saved_mode(username, acc_id, "LW")
                elif mode == "LW" and level < 3:
                    mode = "BR"
                    await _set_mode(acc_id, "BR")
                    _update_saved_mode(username, acc_id, "BR")

                print(f"[auth] Guest login OK user={username} uid={uid} acc_id={acc_id} mode={mode}", flush=True)
                result = await m.run_account_worker(ad, uid, mode=mode)
                if result in ("COMPLETED", "NO_EXP_TIMEOUT") or uid in m.bot_state.completed_accounts or acc_id in m.bot_state.completed_accounts:
                    await _remove_finished_account(username, uid, acc_id)
                    return

                if uid in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                    break

                await asyncio.sleep(3)
            finally:
                _CURRENT_OWNER.reset(cv)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[auth] Guest worker error user={username} uid={uid}: {type(e).__name__}: {e}", flush=True)
            if time.time() - login_failure_started >= 30 * 60:
                print(f"[queue] guest UID {uid} has not worked for 30 minutes; removing from Auto Run", flush=True)
                await _remove_finished_account(username, uid)
                return
            await asyncio.sleep(10)


async def _token_worker(username, token, mode="LW"):
    try:
        import main as m
    except Exception as e:
        print(f"[auth] Cannot import main: {e}", flush=True)
        return
    login_failure_started = time.time()
    while True:
        try:
            key = token[:20]
            if key in USER_DELETED.get(username, set()):
                break
            if key in USER_PAUSED.get(username, set()):
                break
            cv = _CURRENT_OWNER.set(username)
            try:
                print(f"[auth] Token login starting user={username} token={key} mode={mode}", flush=True)
                ad = await m.process_account_token(token)
                if not ad:
                    if time.time() - login_failure_started >= 30 * 60:
                        print(f"[queue] token {key} failed login for 30 minutes; removing from Auto Run", flush=True)
                        await _remove_finished_account(username, key)
                        return
                    await asyncio.sleep(15)
                    continue

                login_failure_started = time.time()
                acc_id = str(ad.get("account_id", ""))
                if key in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                    break
                if key in USER_PAUSED.get(username, set()) or acc_id in USER_PAUSED.get(username, set()):
                    break

                level = int(ad.get("level", 1) or 1)
                if mode == "BR" and level >= 3:
                    mode = "LW"
                    await _set_mode(acc_id, "LW")
                    _update_saved_mode(username, key, "LW")
                elif mode == "LW" and level < 3:
                    mode = "BR"
                    await _set_mode(acc_id, "BR")
                    _update_saved_mode(username, key, "BR")

                print(f"[auth] Token login OK user={username} acc_id={acc_id} mode={mode}", flush=True)
                result = await m.run_account_worker(ad, acc_id, mode=mode)

                if result in ("COMPLETED", "NO_EXP_TIMEOUT") or key in m.bot_state.completed_accounts or acc_id in m.bot_state.completed_accounts:
                    await _remove_finished_account(username, key, acc_id)
                    return

                if key in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                    break

                await asyncio.sleep(3)
            finally:
                _CURRENT_OWNER.reset(cv)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[auth] Token worker error user={username}: {type(e).__name__}: {e}", flush=True)
            if time.time() - login_failure_started >= 30 * 60:
                print(f"[queue] token {token[:20]} has not worked for 30 minutes; removing from Auto Run", flush=True)
                await _remove_finished_account(username, token[:20])
                return
            await asyncio.sleep(10)


def _update_saved_mode(username, key, mode):
    try:
        users = load_users()
        u = users.get(username)
        if not u:
            return
        for acc in u.get("accounts", []):
            saved_key = str(acc.get("uid")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
            if saved_key == str(key) or str(acc.get("uid")) == str(key):
                acc["mode"] = mode
                break
        save_users(users)
    except Exception:
        pass


# ============================================================
# AUTO CYCLE STATE MANAGEMENT
# ============================================================

def _set_cycle_state(username: str, state: str, **kwargs):
    prev = USER_AUTO_CYCLE_STATE.get(username, {})
    prev.update({"state": state, "phase_started_at": time.time()})
    prev.update(kwargs)
    USER_AUTO_CYCLE_STATE[username] = prev


def _clear_cycle_state(username: str):
    USER_AUTO_CYCLE_STATE.pop(username, None)


# ============================================================
# AUTO STOP → RESTART ENGINE  ✅ try/finally guaranteed cleanup
# ============================================================

async def _perform_auto_cycle(username: str, targets: set, reason: str = "auto"):
    """STOP → wait AUTO_RESTART_WAIT → RESTART same set. try/finally ensures cleanup ALWAYS runs."""
    count = len(targets)
    print(f"[{reason}] STOP user={username} count={count}", flush=True)

    # Lock-in FIRST (before any await)
    USER_BULK_PAUSED.add(username)
    USER_BULK_PAUSED_TIME[username] = time.time()
    USER_HOURLY_RESTART_TARGETS[username] = set(targets)
    _set_cycle_state(username, "stopping", targets_count=count, wait_seconds=AUTO_RESTART_WAIT)

    try:
        # --- STOP phase ---
        workers = USER_WORKERS.get(username, {})
        tasks_to_cancel = []

        for key in list(targets):
            t = workers.pop(key, None)
            if t and not t.done():
                t.cancel()
                tasks_to_cancel.append(t)

        for key, task in list(bot_state.account_workers.items()):
            prefix = username + "::"
            if not str(key).startswith(prefix):
                continue
            short_key = str(key)[len(prefix):]
            if short_key in targets:
                bot_state.account_workers.pop(key, None)
                if task and not task.done():
                    task.cancel()
                    tasks_to_cancel.append(task)

        # ✅ SAFE CANCEL — never hangs
        if tasks_to_cancel:
            await _safe_cancel_tasks(tasks_to_cancel, timeout=5.0)

        # Force PAUSED status
        for uid, account in list(bot_state.accounts.items()):
            if account.get("_owner") == username and str(uid) in targets:
                account["status"] = "PAUSED"
                account["active_matches"] = 0
                account["last_update"] = time.time()

        for k in targets:
            USER_PAUSED.setdefault(username, set()).add(str(k))

        # --- WAIT phase ---
        wait_start = time.time()
        print(f"[{reason}] waiting {AUTO_RESTART_WAIT}s before RESTART user={username}", flush=True)
        _set_cycle_state(
            username, "waiting",
            targets_count=count,
            wait_seconds=AUTO_RESTART_WAIT,
            wait_started_at=wait_start,
            wait_remaining=AUTO_RESTART_WAIT,
        )
        await asyncio.sleep(AUTO_RESTART_WAIT)

        # --- RESTART phase ---
        _set_cycle_state(
            username, "restarting",
            targets_count=count,
            wait_seconds=AUTO_RESTART_WAIT,
            wait_started_at=wait_start,
            wait_remaining=0,
        )

        for k in targets:
            USER_PAUSED.get(username, set()).discard(str(k))

        started = set()
        missing = set(targets)
        for attempt in range(1, 4):
            USER_HOURLY_RESTART_TARGETS[username] = set(targets)
            for uid, account in list(bot_state.accounts.items()):
                if account.get("_owner") == username and str(uid) in targets:
                    account["status"] = "CONNECTING"
                    account["last_update"] = time.time()

            try:
                await start_user_workers(username)
            except Exception as e:
                print(f"[{reason}] start_user_workers error: {type(e).__name__}: {e}", flush=True)

            live = USER_WORKERS.get(username, {})
            started = {str(k) for k in targets if live.get(k) and not live[k].done()}
            missing = set(targets) - started
            print(f"[{reason}] RESTART attempt={attempt}/3 started={len(started)}/{len(targets)} missing={len(missing)}", flush=True)
            if not missing:
                break
            if attempt < 3:
                await asyncio.sleep(2)

        if missing:
            print(f"[{reason}] CYCLE INCOMPLETE user={username} missing={len(missing)}", flush=True)
        else:
            print(f"[{reason}] CYCLE OK user={username} restarted={len(started)}", flush=True)

    except asyncio.CancelledError:
        print(f"[{reason}] cycle CANCELLED user={username}", flush=True)
        raise
    except Exception as exc:
        print(f"[{reason}] cycle error user={username}: {type(exc).__name__}: {exc}", flush=True)
    finally:
        # ✅ ALWAYS cleanup — no stuck state possible
        USER_HOURLY_RESTART_TARGETS.pop(username, None)
        USER_BULK_PAUSED.discard(username)
        USER_BULK_PAUSED_TIME.pop(username, None)
        _clear_cycle_state(username)
        print(f"[{reason}] cleanup done user={username}", flush=True)


async def _auto_stop_restart_scheduler():
    """প্রতি ইউজারের জন্য আলাদা auto timer + stuck watchdog।"""
    print(f"[auto] scheduler started | interval={AUTO_RESTART_INTERVAL}s | wait={AUTO_RESTART_WAIT}s | tick={SCHEDULER_TICK}s", flush=True)
    while True:
        try:
            await asyncio.sleep(SCHEDULER_TICK)
            users = load_users()
            now = time.time()
            changed = False

            for username, user in list(users.items()):
                if now >= float(user.get("expires_at", 0) or 0):
                    continue

                # ✅ STUCK WATCHDOG
                if username in USER_BULK_PAUSED:
                    stuck_at = USER_BULK_PAUSED_TIME.get(username, 0)
                    cyc = USER_AUTO_CYCLE_STATE.get(username, {})
                    is_manual = username in USER_MANUAL_PAUSED
                    is_cycle_state = cyc.get("state") in ("stopping", "waiting", "restarting")
                    if (not is_manual and stuck_at > 0
                        and (now - stuck_at) > (AUTO_RESTART_WAIT + 60)
                        and is_cycle_state):
                        print(f"[auto] STUCK WATCHDOG reset user={username} state={cyc.get('state')} elapsed={int(now-stuck_at)}s", flush=True)
                        USER_BULK_PAUSED.discard(username)
                        USER_HOURLY_RESTART_TARGETS.pop(username, None)
                        USER_BULK_PAUSED_TIME.pop(username, None)
                        _clear_cycle_state(username)
                    continue

                workers = USER_WORKERS.get(username, {})
                paused = USER_PAUSED.get(username, set())
                running_tasks = {
                    str(k): t for k, t in workers.items()
                    if t and not t.done() and str(k) not in paused
                }

                if not running_tasks:
                    if float(user.get("last_auto_restart_at", 0) or 0) != 0:
                        user["last_auto_restart_at"] = 0
                        changed = True
                    _clear_cycle_state(username)
                    continue

                last = float(user.get("last_auto_restart_at", 0) or 0)
                if last == 0:
                    user["last_auto_restart_at"] = now
                    changed = True
                    print(f"[auto] timer started user={username} live={len(running_tasks)}", flush=True)
                    continue

                elapsed = now - last
                remaining = AUTO_RESTART_INTERVAL - elapsed

                if remaining > 0:
                    _set_cycle_state(
                        username,
                        "countdown",
                        next_restart_in=int(remaining),
                        next_restart_at=last + AUTO_RESTART_INTERVAL,
                        targets_count=len(running_tasks),
                    )
                    continue

                print(f"[auto] CYCLE START user={username} elapsed={int(elapsed)}s live={len(running_tasks)}", flush=True)
                targets = set(running_tasks.keys())

                # Fire cycle as a task so scheduler is never blocked
                asyncio.create_task(_perform_auto_cycle(username, targets, reason="auto"))

                user["last_auto_restart_at"] = time.time()
                changed = True

            if changed:
                save_users(users)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[auto] scheduler error: {type(exc).__name__}: {exc}", flush=True)


async def _auto_release_bulk_pause():
    """Stuck bulk-pause lock auto-release (manual pause কে skip করে)।"""
    while True:
        try:
            now = time.time()
            for un in list(USER_BULK_PAUSED):
                if un in USER_MANUAL_PAUSED:
                    continue

                set_at = USER_BULK_PAUSED_TIME.get(un, 0)
                if set_at > 0 and (now - set_at) > BULK_PAUSE_TIMEOUT:
                    cyc = USER_AUTO_CYCLE_STATE.get(un, {})
                    if cyc.get("state") in ("stopping", "waiting", "restarting"):
                        continue
                    USER_BULK_PAUSED.discard(un)
                    USER_HOURLY_RESTART_TARGETS.pop(un, None)
                    USER_BULK_PAUSED_TIME.pop(un, None)
                    print(f"[auto] released stale lock user={un} after {int(now-set_at)}s", flush=True)
        except Exception as e:
            print(f"[auto] release error: {e}", flush=True)
        await asyncio.sleep(5)


# ============================================================
# ROUTES
# ============================================================

async def h_root(req):
    return web.Response(text=HOME_HTML, content_type="text/html")


async def h_login_get(req):
    s = cur_user(req)
    if s and not s.get("is_admin"):
        raise web.HTTPFound("/dashboard")
    return web.Response(text=LOGIN_HTML, content_type="text/html")


async def h_login_post(req):
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    if not u or not p:
        return web.json_response({"status": "error", "error": "Missing Credentials"})
    users = load_users()
    x = users.get(u)
    if not x:
        return web.json_response({"status": "error", "error": "Invalid Credentials"})
    if not verify_pw(p, x.get("password_hash", ""), x.get("salt", "")):
        return web.json_response({"status": "error", "error": "Invalid Credentials"})
    token = create_session(u, False)
    print(f"[auth] Login OK user={u}", flush=True)
    resp = web.json_response({"status": "ok", "redirect": "/dashboard"})
    _set_ck(resp, token)
    if time.time() < x.get("expires_at", 0):
        asyncio.create_task(start_user_workers(u))
    return resp


async def h_signup(req):
    raise web.HTTPFound("/plans")


async def h_plans(req):
    s = cur_user(req)
    un = s["username"] if s else "{{USERNAME}}"
    return web.Response(text=PLANS_HTML.replace("{{USERNAME}}", un), content_type="text/html")


async def h_logout(req):
    token = req.cookies.get(SESSION_COOKIE)
    if token:
        s = get_session(token)
        if s and not s.get("is_admin"):
            await sync_user_accounts_to_disk(s["username"])
        destroy_session(token)
    r = web.HTTPFound("/login")
    _clr_ck(r)
    raise r


async def h_dash(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    return web.Response(text=DASHBOARD_HTML.replace("{{USERNAME}}", s["username"]), content_type="text/html")


async def h_add_new_job_page(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    return web.Response(text=ADD_NEW_JOB_HTML, content_type="text/html")


async def h_completed_view(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    try:
        import main as m
        path = getattr(m, "COMPLETED_FILE", "complete.json")
        rows = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                rows = data
        safe = json.dumps(rows, ensure_ascii=False, indent=2).replace("</", "<")
        html = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>complete.json - SAIDUL BHAI</title><style>body{margin:0;background:#05070b;color:#e5e7eb;font-family:Arial,sans-serif;padding:18px} .wrap{max-width:900px;margin:auto} h2{color:#60a5fa} .bar{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0} a,button{padding:10px 14px;border-radius:9px;border:1px solid #263a5a;background:#0b1425;color:#fff;text-decoration:none} pre{white-space:pre-wrap;word-break:break-word;background:#03050a;border:1px solid #1b3558;border-radius:12px;padding:14px;overflow:auto;line-height:1.5}</style></head><body><div class="wrap"><h2>✓ COMPLETED ACCOUNTS</h2><div class="bar"><a href="/dashboard">← Dashboard</a><a href="/api/completed-file" target="_blank">Open JSON</a></div><pre id="json">__JSON__</pre></div></body></html>"""
        html = html.replace("__JSON__", safe)
        return web.Response(text=html, content_type="text/html", charset="utf-8")
    except Exception as exc:
        return web.Response(text=f"Could not open complete.json: {exc}", status=500, content_type="text/plain")


async def h_completed_file(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    try:
        import main as m
        path = getattr(m, "COMPLETED_FILE", "complete.json")
        rows = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                rows = data
        return web.json_response(rows, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, indent=2), headers={"Content-Disposition": "inline; filename=complete.json"})
    except Exception as exc:
        return web.json_response({"error": f"Could not open complete.json: {exc}"}, status=500)


async def h_level_file(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users(); x = users.get(s["username"])
    if not x: raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0): raise web.HTTPFound("/expired")
    name = str(req.match_info.get("name", ""))
    if name not in ("8+15_level.json", "30+51_level.json"):
        raise web.HTTPNotFound()
    try:
        with open(name, "r", encoding="utf-8") as f: data = json.load(f)
        return web.json_response(data, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, indent=2), headers={"Content-Disposition": f"inline; filename={name}"})
    except Exception as exc:
        return web.json_response({"status":"error","error":str(exc)}, status=500)


async def h_add_accounts_file(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    try:
        path = "add_accounts.json"
        data = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            data = loaded
        return web.json_response(
            data,
            dumps=lambda obj: json.dumps(obj, ensure_ascii=False, indent=2),
            headers={"Content-Disposition": "inline; filename=add_accounts.json"}
        )
    except Exception as exc:
        return web.json_response({"error": f"Could not open add_accounts.json: {exc}"}, status=500)


async def h_expired(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    return web.Response(text=EXPIRED_HTML, content_type="text/html")


def _account_key_aliases(uid: str):
    """Return dashboard/game UID and matching login-worker key aliases."""
    aliases = {str(uid)}
    try:
        cred = bot_state.account_credentials.get(str(uid))
        if not cred:
            for candidate in bot_state.account_credentials.values():
                if isinstance(candidate, dict) and str(candidate.get("account_id", "")) == str(uid):
                    cred = candidate
                    break
        if isinstance(cred, dict):
            for field in ("account_id", "auth_uid"):
                value = cred.get(field)
                if value:
                    aliases.add(str(value))
            token = str(cred.get("auth_token", "") or "")
            if token:
                aliases.add(token[:20])
                aliases.add("tok_" + token[:20])
    except Exception:
        pass
    return aliases


async def h_stats(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un, {})
    now = time.time()
    exp_at = u.get("expires_at", 0)
    expired = now >= exp_at
    left = max(0, int(exp_at - now))
    paused = USER_PAUSED.get(un, set())
    deleted = USER_DELETED.get(un, set())

    last_sync = _LAST_SYNC.get(un, 0)
    if now - last_sync > 30:
        _LAST_SYNC[un] = now
        asyncio.create_task(sync_user_accounts_to_disk(un))

    if not expired and un not in USER_WORKERS:
        asyncio.create_task(start_user_workers(un))

    out = []
    seen_keys = set()
    restart_targets = USER_HOURLY_RESTART_TARGETS.get(un, set())
    is_bulk_paused = un in USER_BULK_PAUSED
    live_workers = USER_WORKERS.get(un, {})

    cyc = USER_AUTO_CYCLE_STATE.get(un, {})
    cyc_state = cyc.get("state", "idle")

    def _is_paused_for(key_str):
        aliases = _account_key_aliases(str(key_str))
        # A pause flag must take precedence over a stale/live task entry.
        if aliases.intersection(str(x) for x in paused):
            return True
        if cyc_state in ("stopping", "waiting") and aliases.intersection(str(x) for x in restart_targets):
            return True
        if is_bulk_paused and restart_targets:
            return not aliases.intersection(str(x) for x in restart_targets)
        for alias in aliases:
            task = live_workers.get(alias)
            if task and not task.done():
                return False
        return False

    def _forced_status(uid_str, default_status):
        aliases = _account_key_aliases(str(uid_str))
        target_match = aliases.intersection(str(x) for x in restart_targets)
        if cyc_state in ("stopping", "waiting") and target_match:
            return "PAUSED"
        if cyc_state == "restarting" and target_match:
            return "CONNECTING"
        if _is_paused_for(uid_str):
            return "PAUSED"
        return default_status

    for uid, a in list(ds.bot_state.accounts.items()):
        if a.get("_owner") != un:
            continue
        if uid in deleted:
            continue

        is_paused = _is_paused_for(uid)
        default_status = "PAUSED" if is_paused else a.get("status", "LIVE")
        status = _forced_status(uid, default_status)
        mode = await _get_mode(uid) or "LW"
        out.append({
            "uid": uid,
            "nickname": a.get("nickname", "?"),
            "region": a.get("region", "IND"),
            "level": a.get("level", 1),
            "initial_exp": a.get("initial_exp", 0),
            "current_exp": a.get("current_exp", 0),
            "gained_exp": a.get("gained_exp", 0),
            "matches_played": a.get("matches_played", 0),
            "status": status,
            "active_matches": a.get("active_matches", 0),
            "created_at": a.get("created_at"),
            "last_update": a.get("last_update"),
            "mode": mode,
            "level_progress": _level_progress(a.get("level", 1), a.get("current_exp", 0)),
        })
        seen_keys.add(str(uid))
        try:
            cred = ds.bot_state.account_credentials.get(str(uid))
            if cred:
                if cred.get("account_id"):
                    seen_keys.add(str(cred.get("account_id")))
                if cred.get("auth_uid"):
                    seen_keys.add(str(cred.get("auth_uid")))
                if cred.get("auth_token"):
                    seen_keys.add(str(cred.get("auth_token"))[:20])
        except Exception:
            pass

    for acc in u.get("accounts", []):
        acc_type = acc.get("type")
        if acc_type == "guest":
            key_str = str(acc.get("uid", ""))
        else:
            key_str = str(acc.get("token", ""))[:20]
        if not key_str:
            continue
        if key_str in seen_keys or key_str in deleted:
            continue
        matched_live = False
        try:
            for bid, acc_live in ds.bot_state.accounts.items():
                if acc_live.get("_owner") != un:
                    continue
                cred = ds.bot_state.account_credentials.get(str(bid))
                if not cred:
                    continue
                if acc_type == "guest" and str(cred.get("auth_uid", "")) == key_str:
                    matched_live = True
                    break
                if acc_type == "token" and str(cred.get("auth_token", ""))[:20] == key_str:
                    matched_live = True
                    break
        except Exception:
            pass
        if matched_live:
            continue

        is_paused = _is_paused_for(key_str)
        default_status = "PAUSED" if is_paused else "CONNECTING"
        status_str = _forced_status(key_str, default_status)

        saved_exp = acc.get("last_exp", 0)
        saved_level = acc.get("last_level", 1)
        saved_nickname = acc.get("last_nickname", "Loading...")
        saved_region = acc.get("last_region", "IND")
        saved_initial = acc.get("initial_exp", saved_exp)
        saved_gained = acc.get("gained_exp", max(0, saved_exp - saved_initial))
        saved_played = acc.get("matches_played", 0)
        saved_created = acc.get("created_at")
        saved_updated = acc.get("last_update")
        saved_mode = acc.get("mode", "LW")

        out.append({
            "uid": key_str,
            "nickname": saved_nickname,
            "region": saved_region,
            "level": saved_level,
            "initial_exp": saved_initial,
            "current_exp": saved_exp,
            "gained_exp": saved_gained,
            "matches_played": saved_played,
            "status": status_str,
            "active_matches": 0,
            "created_at": saved_created,
            "last_update": saved_updated,
            "mode": saved_mode,
            "level_progress": _level_progress(saved_level, saved_exp),
        })

    out.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)
    try:
        pending_accounts = load_pending_imports().get(un, [])
        if not isinstance(pending_accounts, list): pending_accounts = []
    except Exception:
        pending_accounts = []
    completed_accounts = []
    try:
        import main as m
        path = getattr(m, "COMPLETED_FILE", "complete.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f: rows = json.load(f)
            if isinstance(rows, list):
                completed_accounts = [{"uid": str(r.get("uid"))} for r in rows if isinstance(r, dict) and r.get("uid")]
    except Exception:
        completed_accounts = []
    exp_active_count = sum(1 for item in out if int(item.get("gained_exp", 0) or 0) > 0)
    total_gained_exp = sum(int(item.get("gained_exp", 0) or 0) for item in out)

    level_file_counts = {"8+15_level.json": 0, "30+51_level.json": 0}
    for level_name in tuple(level_file_counts):
        try:
            if os.path.exists(level_name):
                with open(level_name, "r", encoding="utf-8") as f:
                    rows = json.load(f)
                if isinstance(rows, list):
                    level_file_counts[level_name] = len(rows)
        except Exception:
            level_file_counts[level_name] = 0

    # ============================================================
    # AUTO CYCLE STATUS for dashboard
    # ============================================================
    live_count = sum(1 for t in live_workers.values() if t and not t.done())
    now_ts = time.time()
    last_auto = float(u.get("last_auto_restart_at", 0) or 0)

    if cyc_state == "countdown" and last_auto > 0:
        next_in = max(0, int(AUTO_RESTART_INTERVAL - (now_ts - last_auto)))
        cyc["next_restart_in"] = next_in
    elif cyc_state == "waiting":
        wait_started = float(cyc.get("wait_started_at", now_ts))
        elapsed_wait = now_ts - wait_started
        wait_rem = max(0, int(math.ceil(AUTO_RESTART_WAIT - elapsed_wait)))
        cyc["wait_remaining"] = wait_rem
        cyc["wait_seconds"] = AUTO_RESTART_WAIT
        next_in = 0
    elif cyc_state in ("stopping", "restarting"):
        next_in = 0
    else:
        if last_auto > 0 and live_count > 0 and not is_bulk_paused:
            next_in = max(0, int(AUTO_RESTART_INTERVAL - (now_ts - last_auto)))
            cyc_state = "countdown"
        else:
            next_in = 0
            cyc_state = "idle"

    return web.json_response({
        "username": un, "slots": u.get("slots", 0), "seconds_left": left, "expired": expired,
        "accounts": out, "exp_active_count": exp_active_count,
        "total_visible_accounts": len(out), "total_gained_exp": total_gained_exp,
        "pending_accounts": pending_accounts, "pending_count": len(pending_accounts),
        "completed_accounts": completed_accounts, "completed_count": len(completed_accounts),
        "level_file_counts": level_file_counts,
        "next_auto_restart_in": next_in,
        "auto_restart_active": cyc_state in ("countdown", "stopping", "waiting", "restarting"),
        "auto_restart_interval": AUTO_RESTART_INTERVAL,
        "auto_cycle_state": cyc_state,
        "auto_cycle_targets": int(cyc.get("targets_count", 0) or 0),
        "auto_cycle_wait_remaining": int(cyc.get("wait_remaining", 0) or 0),
        "auto_wait_seconds": AUTO_RESTART_WAIT,
        "manual_paused": un in USER_MANUAL_PAUSED,
        "stop_phase_started_at": float(USER_BULK_PAUSED_TIME.get(un, 0) or 0),
    })


def _count_active_accounts(u, un_deleted):
    seen = set()
    for a in u.get("accounts", []):
        if a.get("type") == "guest":
            k = str(a.get("uid", ""))
        else:
            k = str(a.get("token", ""))[:20]
        if k and k not in un_deleted:
            seen.add(k)
    return len(seen)


async def h_api_br_status(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    st = await _br_status_ping()
    return web.json_response(st)


async def h_api_br_start(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"Sorry": "unauthorized"}, status=401)
    uid = req.query.get("uid", "").strip()
    pw = req.query.get("pw", "").strip()
    accs = req.query.get("accs", "").strip()
    if accs:
        data = await _br_request(BR_START_PATH, accs=accs)
    else:
        if not uid or not pw:
            return web.json_response({"Sorry": "Missing uid/pw or accs"})
        data = await _br_request(BR_START_PATH, uid=uid, pw=pw)
    return web.json_response(data)


async def h_api_br_stop(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"Sorry": "unauthorized"}, status=401)
    uid = req.query.get("uid", "").strip()
    pw = req.query.get("pw", "").strip()
    accs = req.query.get("accs", "").strip()
    if accs:
        data = await _br_request(BR_STOP_PATH, accs=accs, timeout=15.0)
    else:
        if not uid or not pw:
            return web.json_response({"Sorry": "Missing uid/pw or accs"})
        data = await _br_request(BR_STOP_PATH, uid=uid, pw=pw, timeout=15.0)
    return web.json_response(data)


async def h_api_br_check(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"Sorry": "unauthorized"}, status=401)
    uid = req.query.get("uid", "").strip()
    pw = req.query.get("pw", "").strip()
    accs = req.query.get("accs", "").strip()
    if accs:
        data = await _br_request(BR_STATUS_PATH, accs=accs, timeout=15.0)
    else:
        if not uid or not pw:
            return web.json_response({"Sorry": "Missing uid/pw or accs"})
        data = await _br_request(BR_STATUS_PATH, uid=uid, pw=pw, timeout=15.0)
    return web.json_response(data)


async def h_acc_check(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    if time.time() >= u.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    accs = u.setdefault("accounts", [])
    un_deleted = USER_DELETED.get(un, set())

    if _count_active_accounts(u, un_deleted) >= u.get("slots", 0):
        return web.json_response({"status": "error", "error": "No Free Slots"})

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    acc_data = None
    if "uid" in d and "password" in d:
        uid = str(d["uid"]).strip()
        pw = str(d["password"]).strip()
        if not uid or not pw:
            return web.json_response({"status": "error", "error": "UID And Password Required"})
        for a in accs:
            if a.get("type") == "guest" and str(a.get("uid")) == uid:
                return web.json_response({"status": "error", "error": "UID Already Added"})
        cached = m.cache_get(uid)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_uid_pass(uid, pw)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid UID/Password"})
    elif "token" in d:
        tk = str(d["token"]).strip()
        if not tk:
            return web.json_response({"status": "error", "error": "Token Required"})
        for a in accs:
            if a.get("type") == "token" and a.get("token") == tk:
                return web.json_response({"status": "error", "error": "Token Already Added"})
        cache_key = f"tok_{tk[:20]}"
        cached = m.cache_get(cache_key)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_token(tk)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid Token"})
    else:
        return web.json_response({"status": "error", "error": "Invalid Payload"})

    if not acc_data:
        return web.json_response({"status": "error", "error": "Login Failed"})

    return web.json_response({
        "status": "ok",
        "level": int(acc_data.get("level", 1) or 1),
        "region": str(acc_data.get("region", "") or "").upper(),
        "nickname": str(acc_data.get("nickname", "") or ""),
        "account_id": str(acc_data.get("account_id", "")),
    })


async def h_acc_import_file(req):
    session = cur_user(req)
    if not session or session.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)

    username = session["username"]
    users = load_users()
    user = users.get(username)
    if not user:
        return web.json_response({"status": "error", "error": "User Missing"})
    if time.time() >= user.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})

    rows = None
    try:
        ctype = (req.headers.get("Content-Type") or "").lower()
        if ctype.startswith("multipart/form-data"):
            post = await req.post()
            upload = post.get("file")
            if upload is None or not hasattr(upload, "file"):
                return web.json_response({"status": "error", "error": "add_accounts.json file is required"})
            raw = upload.file.read()
            rows = json.loads(raw.decode("utf-8-sig"))
        else:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "add_accounts.json")
            if not os.path.exists(path):
                return web.json_response({"status": "error", "error": "add_accounts.json not found"}, status=404)
            with open(path, "r", encoding="utf-8") as f:
                rows = json.load(f)
        if not isinstance(rows, list):
            raise ValueError("add_accounts.json must contain a JSON list")
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})

    try:
        import main as m
    except Exception as e:
        return web.json_response({"status": "error", "error": f"Main module unavailable: {e}"})

    pending_data = load_pending_imports()
    existing_pending = pending_data.get(username, [])
    if not isinstance(existing_pending, list):
        existing_pending = []

    valid_rows = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        uid = str(row.get("uid", "")).strip()
        password = str(row.get("password", "")).strip()
        if uid and password and uid not in seen:
            seen.add(uid)
            valid_rows.append({"uid": uid, "password": password})

    accounts = user.setdefault("accounts", [])
    already = {
        str(a.get("uid", "")) for a in accounts if a.get("type") == "guest"
    }
    free_slots = max(0, int(user.get("slots", 0) or 0) - _count_active_accounts(user, USER_DELETED.get(username, set())))
    imported = 0
    already_added = 0
    failed = []
    skipped_slots = []

    for row in valid_rows:
        uid, password = row["uid"], row["password"]
        if uid in already:
            already_added += 1
            continue
        if free_slots <= 0:
            skipped_slots.append(uid)
            continue

        try:
            acc_data = m.cache_get(uid)
            if not acc_data or not acc_data.get("level"):
                acc_data = await m.process_account_uid_pass(uid, password)
            if not acc_data:
                failed.append(uid)
                continue

            level = int(acc_data.get("level", 1) or 1)
            mode = "BR" if level < 3 else "LW"
            accounts.append({
                "type": "guest",
                "uid": uid,
                "password": password,
                "mode": mode,
                "created_at": time.time(),
            })
            await _set_mode(str(acc_data.get("account_id", uid)), mode)
            already.add(uid)
            imported += 1
            free_slots -= 1
            print(f"[auto-import] queued account for user={username} uid={uid} mode={mode}", flush=True)
        except Exception as e:
            print(f"[auto-import] account setup failed user={username} uid={uid}: {e}", flush=True)
            failed.append(uid)

    users[username] = user
    save_users(users)

    if imported:
        await start_user_workers(username)

    failed_set = set(failed)
    skipped_set = set(skipped_slots)
    pending_items = []
    seen_pending = set()

    for item in existing_pending:
        if isinstance(item, dict):
            puid = str(item.get("uid", "")).strip()
            ppw = str(item.get("password", "")).strip()
            if puid and ppw and puid not in seen_pending:
                pending_items.append({"uid": puid, "password": ppw})
                seen_pending.add(puid)

    for row in valid_rows:
        uid, password = row["uid"], row["password"]
        if (uid in failed_set or uid in skipped_set) and uid not in seen_pending and uid not in already:
            pending_items.append({"uid": uid, "password": password})
            seen_pending.add(uid)
        if len(pending_items) >= 200:
            break

    pending_data[username] = pending_items[:200]
    save_pending_imports(pending_data)

    return web.json_response({
        "status": "ok",
        "imported": imported,
        "already_added": already_added,
        "started": imported,
        "failed": len(failed),
        "pending": len(pending_items),
        "skipped_slots": len(skipped_slots),
        "max_pending": 200,
    })


async def h_acc_add_job(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    if time.time() >= u.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    mode = str(d.get("mode", "")).upper()
    if mode not in ("BR", "LW"):
        return web.json_response({"status": "error", "error": "Mode Must Be BR Or LW"})

    accs = u.setdefault("accounts", [])
    slots = u.get("slots", 0)
    un_deleted = USER_DELETED.get(un, set())

    if _count_active_accounts(u, un_deleted) >= slots:
        return web.json_response({"status": "error", "error": "No Free Slots"})

    if un not in USER_DELETED:
        USER_DELETED[un] = set()
    if un not in USER_PAUSED:
        USER_PAUSED[un] = set()

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    async def _validate_level(acc_data):
        lvl = int(acc_data.get("level", 1) or 1)
        if mode == "BR" and lvl >= 3:
            return False, "Battle Royale requires Level 1-2. Your account is Level " + str(lvl) + ". Please select Lone Wolf."
        if mode == "LW" and lvl < 3:
            return False, "Lone Wolf requires Level 3+. Your account is Level " + str(lvl) + ". Please select Battle Royale."
        return True, None

    acc_data = None
    key_to_start = None
    acc_id_str = None

    if "uid" in d and "password" in d:
        uid = str(d["uid"]).strip()
        pw = str(d["password"]).strip()
        if not uid or not pw:
            return web.json_response({"status": "error", "error": "UID And Password Required"})
        for a in accs:
            if a.get("type") == "guest" and str(a.get("uid")) == uid:
                return web.json_response({"status": "error", "error": "UID Already Added"})

        cached = m.cache_get(uid)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_uid_pass(uid, pw)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid UID/Password"})

        if not acc_data:
            return web.json_response({"status": "error", "error": "Login Failed"})

        ok, err = await _validate_level(acc_data)
        if not ok:
            return web.json_response({"status": "error", "error": err})

        accs.append({"type": "guest", "uid": uid, "password": pw, "mode": mode, "created_at": time.time()})
        await _set_mode(str(acc_data.get("account_id", uid)), mode)
        key_to_start = uid
        acc_id_str = str(acc_data.get("account_id", uid))
        print(f"[api] Account added user={un} uid={uid} mode={mode}", flush=True)

    elif "token" in d:
        tk = str(d["token"]).strip()
        if not tk:
            return web.json_response({"status": "error", "error": "Token Required"})
        for a in accs:
            if a.get("type") == "token" and a.get("token") == tk:
                return web.json_response({"status": "error", "error": "Token Already Added"})

        cache_key = f"tok_{tk[:20]}"
        cached = m.cache_get(cache_key)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_token(tk)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid Token"})

        if not acc_data:
            return web.json_response({"status": "error", "error": "Login Failed"})

        ok, err = await _validate_level(acc_data)
        if not ok:
            return web.json_response({"status": "error", "error": err})

        accs.append({"type": "token", "token": tk, "mode": mode, "created_at": time.time()})
        await _set_mode(str(acc_data.get("account_id", tk[:20])), mode)
        key_to_start = tk[:20]
        acc_id_str = str(acc_data.get("account_id", tk[:20]))
        print(f"[api] Account added user={un} token={tk[:20]} mode={mode}", flush=True)
    else:
        return web.json_response({"status": "error", "error": "Invalid Payload"})

    u["accounts"] = accs
    users[un] = u
    save_users(users)

    USER_DELETED.setdefault(un, set()).discard(key_to_start)
    USER_DELETED[un].discard(acc_id_str)
    USER_PAUSED.setdefault(un, set()).discard(key_to_start)
    USER_PAUSED[un].discard(acc_id_str)

    stale = USER_WORKERS.get(un, {}).pop(key_to_start, None)
    if stale and not stale.done():
        stale.cancel()
    stale2 = USER_WORKERS.get(un, {}).pop(acc_id_str, None)
    if stale2 and not stale2.done():
        stale2.cancel()

    await start_user_workers(un)

    return web.json_response({
        "status": "ok",
        "mode": mode,
        "nickname": str(acc_data.get("nickname", "") or ""),
        "level": int(acc_data.get("level", 1) or 1),
        "region": str(acc_data.get("region", "") or "").upper(),
        "account_id": str(acc_data.get("account_id", "")),
    })


async def h_acc_bulk(req):
    """Manual STOP ALL / RESTART ALL. RESTART = STOP → 10s wait → RESTART."""
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    action = str(d.get("action", "")).lower().strip()
    if action not in ("pause", "resume"):
        return web.json_response({"status": "error", "error": "Invalid action"})

    users = load_users()
    u = users.get(un, {})
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})

    USER_PAUSED.setdefault(un, set())
    USER_DELETED.setdefault(un, set())
    USER_WORKERS.setdefault(un, {})
    workers = USER_WORKERS[un]

    # ------- STOP ALL -------
    if action == "pause":
        print(f"[bulk] MANUAL STOP ALL user={un}", flush=True)
        USER_BULK_PREPAUSED[un] = set(USER_PAUSED.get(un, set()))
        USER_BULK_PAUSED.add(un)
        USER_MANUAL_PAUSED.add(un)
        USER_BULK_PAUSED_TIME[un] = time.time()
        USER_HOURLY_RESTART_TARGETS.pop(un, None)
        _clear_cycle_state(un)

        saved_keys = []
        for acc in u.get("accounts", []):
            key = str(acc.get("uid", "")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
            if key:
                USER_PAUSED[un].add(key)
                saved_keys.append(key)

        tasks = []
        for key, task in list(workers.items()):
            workers.pop(key, None)
            if task and not task.done():
                task.cancel()
                tasks.append(task)
        for key, task in list(bot_state.account_workers.items()):
            if str(key).startswith(un + "::"):
                bot_state.account_workers.pop(key, None)
                if task and not task.done():
                    task.cancel()
                    tasks.append(task)
        if tasks:
            await _safe_cancel_tasks(tasks, timeout=5.0)

        for uid, account in list(bot_state.accounts.items()):
            if account.get("_owner") == un:
                account["status"] = "PAUSED"
                account["active_matches"] = 0
                account["last_update"] = time.time()

        u["last_auto_restart_at"] = 0
        users[un] = u
        save_users(users)

        print(f"[bulk] MANUAL STOP ALL done user={un} paused={len(saved_keys)}", flush=True)
        return web.json_response({"status": "ok", "action": "pause", "count": len(saved_keys)})

    # ------- RESTART ALL (with 10s wait) -------
    print(f"[bulk] MANUAL RESTART ALL user={un}", flush=True)
    if time.time() >= float(u.get("expires_at", 0) or 0):
        return web.json_response({"status": "error", "error": "Account expired"}, status=400)

    prepaused = set(USER_BULK_PREPAUSED.get(un, set()))
    USER_PAUSED[un] = set(prepaused)
    USER_DELETED[un].clear()
    USER_MANUAL_PAUSED.discard(un)

    restart_targets = set()
    for acc in u.get("accounts", []):
        key = str(acc.get("uid", "")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
        if key and key not in prepaused:
            restart_targets.add(key)
    restart_targets.discard("")

    tasks_to_cancel = []
    for key in list(workers.keys()):
        t = workers.pop(key, None)
        if t and not t.done():
            t.cancel()
            tasks_to_cancel.append(t)
    for key, task in list(bot_state.account_workers.items()):
        if str(key).startswith(un + "::"):
            bot_state.account_workers.pop(key, None)
            if task and not task.done():
                task.cancel()
                tasks_to_cancel.append(task)
    if tasks_to_cancel:
        await _safe_cancel_tasks(tasks_to_cancel, timeout=5.0)

    USER_HOURLY_RESTART_TARGETS[un] = restart_targets
    USER_BULK_PAUSED.add(un)
    USER_BULK_PAUSED_TIME[un] = time.time()

    for uid, account in list(bot_state.accounts.items()):
        if account.get("_owner") == un:
            account["status"] = "PAUSED"
            account["active_matches"] = 0
            account["last_update"] = time.time()

    for k in restart_targets:
        USER_PAUSED.setdefault(un, set()).add(str(k))

    wait_start = time.time()
    _set_cycle_state(
        un,
        "waiting",
        targets_count=len(restart_targets),
        wait_seconds=AUTO_RESTART_WAIT,
        wait_started_at=wait_start,
        wait_remaining=AUTO_RESTART_WAIT,
    )

    async def _run_manual_restart_cycle():
        try:
            print(f"[bulk] MANUAL RESTART ALL waiting {AUTO_RESTART_WAIT}s user={un}", flush=True)
            await asyncio.sleep(AUTO_RESTART_WAIT)

            _set_cycle_state(
                un,
                "restarting",
                targets_count=len(restart_targets),
                wait_seconds=AUTO_RESTART_WAIT,
                wait_started_at=wait_start,
                wait_remaining=0,
            )

            for k in restart_targets:
                USER_PAUSED.get(un, set()).discard(str(k))

            started = set()
            missing = set(restart_targets)
            for attempt in range(1, 4):
                USER_HOURLY_RESTART_TARGETS[un] = set(restart_targets)
                for uid, account in list(bot_state.accounts.items()):
                    if account.get("_owner") == un and str(uid) in restart_targets:
                        account["status"] = "CONNECTING"
                        account["last_update"] = time.time()

                try:
                    await start_user_workers(un)
                except Exception as e:
                    print(f"[bulk] start_user_workers error: {type(e).__name__}: {e}", flush=True)

                live = USER_WORKERS.get(un, {})
                started = {str(k) for k in restart_targets if live.get(k) and not live[k].done()}
                missing = set(restart_targets) - started
                print(f"[bulk] MANUAL RESTART ALL attempt={attempt}/3 started={len(started)}/{len(restart_targets)} missing={len(missing)}", flush=True)
                if not missing:
                    break
                if attempt < 3:
                    await asyncio.sleep(2)

            now = time.time()
            users2 = load_users()
            u2 = users2.get(un, {})
            u2["last_auto_restart_at"] = now
            users2[un] = u2
            save_users(users2)

            if missing:
                print(f"[bulk] MANUAL RESTART ALL incomplete user={un} missing={len(missing)}", flush=True)
            else:
                print(f"[bulk] MANUAL RESTART ALL OK user={un} started={len(started)}", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[bulk] MANUAL RESTART ALL error user={un}: {type(exc).__name__}: {exc}", flush=True)
        finally:
            USER_HOURLY_RESTART_TARGETS.pop(un, None)
            USER_BULK_PAUSED.discard(un)
            USER_BULK_PAUSED_TIME.pop(un, None)
            USER_BULK_PREPAUSED.pop(un, None)
            _clear_cycle_state(un)
            print(f"[bulk] MANUAL RESTART ALL cleanup done user={un}", flush=True)

    asyncio.create_task(_run_manual_restart_cycle())

    return web.json_response({
        "status": "ok",
        "action": "resume",
        "wait_seconds": AUTO_RESTART_WAIT,
        "saved_accounts": len(u.get("accounts", [])),
    })


async def h_acc_delete(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[api] Account delete user={un} uid={uid}", flush=True)

    if un not in USER_DELETED:
        USER_DELETED[un] = set()
    if un not in USER_PAUSED:
        USER_PAUSED[un] = set()

    real_uid = None
    real_token_prefix = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            if cred.get("auth_uid"):
                real_uid = str(cred.get("auth_uid"))
            if cred.get("auth_token"):
                real_token_prefix = str(cred.get("auth_token"))[:20]
    except Exception:
        pass

    if real_uid is None and real_token_prefix is None:
        try:
            for k, cred in list(ds.bot_state.account_credentials.items()):
                if str(cred.get("account_id", "")) == uid:
                    if cred.get("auth_uid"):
                        real_uid = str(cred.get("auth_uid"))
                        break
                    if cred.get("auth_token"):
                        real_token_prefix = str(cred.get("auth_token"))[:20]
                        break
        except Exception:
            pass

    USER_DELETED[un].add(uid)
    if real_uid:
        USER_DELETED[un].add(real_uid)
    if real_token_prefix:
        USER_DELETED[un].add(real_token_prefix)
    USER_PAUSED[un].discard(uid)
    if real_uid:
        USER_PAUSED[un].discard(real_uid)
    if real_token_prefix:
        USER_PAUSED[un].discard(real_token_prefix)

    keys_to_kill = [uid]
    if real_uid:
        keys_to_kill.append(real_uid)
    if real_token_prefix:
        keys_to_kill.append(real_token_prefix)

    workers = USER_WORKERS.get(un, {})
    delete_tasks = []
    for k in keys_to_kill:
        t = workers.pop(k, None)
        if t and not t.done():
            delete_tasks.append(t)

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(un + "::"):
            suffix = str(key).split("::", 1)[1]
            if suffix in keys_to_kill:
                t = bot_state.account_workers.pop(key, None)
                if t and not t.done():
                    delete_tasks.append(t)

    if delete_tasks:
        await _safe_cancel_tasks(delete_tasks, timeout=4.0)

    for k in keys_to_kill:
        try:
            ds.cache_invalidate(k)
        except Exception:
            pass
        await _clear_mode(k)

    removed = False
    accs_list = list(u.get("accounts", []))
    for acc in list(accs_list):
        acc_type = acc.get("type")
        acc_uid = str(acc.get("uid", ""))
        acc_token_prefix = str(acc.get("token", ""))[:20]

        match = False
        if acc_type == "guest":
            if acc_uid == uid or (real_uid and acc_uid == real_uid):
                match = True
        elif acc_type == "token":
            if acc_token_prefix == uid[:20] or (real_token_prefix and acc_token_prefix == real_token_prefix):
                match = True

        if match:
            accs_list.remove(acc)
            removed = True
            break

    u["accounts"] = accs_list
    users[un] = u
    save_users(users)

    for k in keys_to_kill:
        if k in bot_state.accounts:
            bot_state.accounts.pop(k, None)

    await asyncio.sleep(0.5)

    if un not in USER_BULK_PAUSED:
        asyncio.create_task(_queue_user_once(un))

    return web.json_response({"status": "ok", "removed": removed})


async def h_acc_pause(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[api] Account pause user={un} uid={uid}", flush=True)

    keys_to_kill = list(_account_key_aliases(uid))
    if un not in USER_PAUSED:
        USER_PAUSED[un] = set()
    USER_PAUSED[un].update(keys_to_kill)

    workers = USER_WORKERS.get(un, {})
    pause_tasks = []
    for k in keys_to_kill:
        t = workers.pop(k, None)
        if t and not t.done():
            pause_tasks.append(t)

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(un + "::"):
            suffix = str(key).split("::", 1)[1]
            if suffix in keys_to_kill:
                t = bot_state.account_workers.pop(key, None)
                if t and not t.done():
                    pause_tasks.append(t)

    if pause_tasks:
        await _safe_cancel_tasks(pause_tasks, timeout=4.0)

    for k in keys_to_kill:
        if k in bot_state.accounts:
            bot_state.accounts[k]["status"] = "PAUSED"
            bot_state.accounts[k]["last_update"] = time.time()

    return web.json_response({"status": "ok"})


async def h_acc_resume(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[api] Account resume user={un} uid={uid}", flush=True)

    keys_to_release = list(_account_key_aliases(uid))

    if un in USER_PAUSED:
        for k in keys_to_release:
            USER_PAUSED[un].discard(k)
    if un in USER_DELETED:
        for k in keys_to_release:
            USER_DELETED[un].discard(k)

    for k in keys_to_release:
        if k in bot_state.accounts:
            bot_state.accounts[k]["status"] = "CONNECTING"
            bot_state.accounts[k]["last_update"] = time.time()

    users = load_users()
    u = users.get(un, {})
    if time.time() < u.get("expires_at", 0):
        # Resolve only the selected account's saved worker key.
        selected_keys = set()
        aliases = set(keys_to_release)
        for acc in u.get("accounts", []):
            k = str(acc.get("uid", "")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
            if k and k in aliases:
                selected_keys.add(k)
        # If a game UID was supplied, resolve its credential record to the saved login key.
        if not selected_keys:
            for acc in u.get("accounts", []):
                k = str(acc.get("uid", "")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
                if not k:
                    continue
                for alias in aliases:
                    cred = bot_state.account_credentials.get(alias)
                    if isinstance(cred, dict):
                        auth_uid = str(cred.get("auth_uid", ""))
                        auth_token = str(cred.get("auth_token", ""))[:20]
                        if (acc.get("type") == "guest" and auth_uid == k) or (acc.get("type") == "token" and auth_token == k):
                            selected_keys.add(k)
                            break
        if selected_keys:
            # A manual pause must not consume the 30-minute no-EXP timeout.
            # Give the resumed account a fresh progress window and persist it.
            resumed_at = time.time()
            try:
                for saved_acc in u.get("accounts", []):
                    saved_key = str(saved_acc.get("uid", "")) if saved_acc.get("type") == "guest" else str(saved_acc.get("token", ""))[:20]
                    if saved_key in selected_keys:
                        saved_acc["last_exp_progress_at"] = resumed_at
                users[un] = u
                save_users(users)
                for live_uid, live_acc in list(bot_state.accounts.items()):
                    if live_acc.get("_owner") == un:
                        cred = bot_state.account_credentials.get(str(live_uid), {})
                        if str(live_uid) in aliases or str(cred.get("auth_uid", "")) in aliases or str(cred.get("auth_token", ""))[:20] in aliases:
                            live_acc["last_exp_progress_at"] = resumed_at
            except Exception as exc:
                print(f"[api] Could not reset resume progress timer user={un}: {exc}", flush=True)

            # A single-account Resume must not restart unrelated paused accounts.
            USER_MANUAL_PAUSED.discard(un)
            USER_BULK_PAUSED.discard(un)
            USER_BULK_PREPAUSED.pop(un, None)
            USER_BULK_PAUSED_TIME.pop(un, None)
            USER_HOURLY_RESTART_TARGETS[un] = set(selected_keys)
            await start_user_workers(un)

            if float(u.get("last_auto_restart_at", 0) or 0) == 0:
                u["last_auto_restart_at"] = time.time()
                users[un] = u
                save_users(users)

            async def _release_after_resume():
                await asyncio.sleep(3)
                USER_HOURLY_RESTART_TARGETS.pop(un, None)
            asyncio.create_task(_release_after_resume())

    return web.json_response({"status": "ok"})


async def h_acc_auto_cycle(req):
    """Manual trigger: run one auto cycle immediately."""
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    workers = USER_WORKERS.get(un, {})
    targets = {str(k) for k, t in workers.items() if t and not t.done()}
    if not targets:
        return web.json_response({"status": "error", "error": "No running workers"})
    asyncio.create_task(_perform_auto_cycle(un, targets, reason="manual-auto"))
    return web.json_response({"status": "ok", "triggered": len(targets)})


async def h_adm_login_get(req):
    s = cur_user(req)
    if s and s.get("is_admin"):
        raise web.HTTPFound(ADMIN_PATH + "/panel")
    return web.Response(text=ADMIN_LOGIN_HTML, content_type="text/html")


async def h_adm_login_post(req):
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
        token = create_session(u, True)
        r = web.json_response({"status": "ok", "redirect": ADMIN_PATH + "/panel"})
        _set_ck(r, token)
        return r
    return web.json_response({"status": "error", "error": "Access Denied"})


async def h_adm_logout(req):
    token = req.cookies.get(SESSION_COOKIE)
    if token:
        destroy_session(token)
    r = web.HTTPFound(ADMIN_PATH)
    _clr_ck(r)
    raise r


async def h_adm_panel(req):
    if not cur_admin(req):
        raise web.HTTPFound(ADMIN_PATH)
    return web.Response(text=ADMIN_PANEL_HTML, content_type="text/html")


async def h_adm_create(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    u = (d.get("username") or "").strip()
    p = (d.get("password") or "").strip()
    slots = int(d.get("slots", 3))
    hours = int(d.get("hours", 24))
    if not u or not p:
        return web.json_response({"status": "error", "error": "Username And Password Required"})
    if slots < 1 or slots > 1000:
        return web.json_response({"status": "error", "error": "Slots Must Be 1-1000"})
    if hours < 1 or hours > 8760:
        return web.json_response({"status": "error", "error": "Hours Must Be 1-8760"})
    users = load_users()
    if u in users:
        return web.json_response({"status": "error", "error": "Username Already Exists"})
    ph, salt = hash_pw(p)
    now = time.time()
    users[u] = {
        "password_hash": ph,
        "salt": salt,
        "slots": slots,
        "plan_hours": hours,
        "created_at": now,
        "expires_at": now + hours * 3600,
        "accounts": [],
        "_expired_flag": False,
        "last_auto_restart_at": 0,
    }
    save_users(users)
    return web.json_response({"status": "ok"})


async def h_adm_list(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    users = load_users()
    out = []
    for un, u in users.items():
        used = sum(1 for a in ds.bot_state.accounts.values() if a.get("_owner") == un)
        out.append({
            "username": un,
            "slots": u.get("slots", 0),
            "used_slots": used,
            "expires_at": u.get("expires_at", 0),
            "created_at": u.get("created_at", 0),
        })
    out.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return web.json_response({"status": "ok", "users": out})


async def h_adm_ext(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    hours = int(d.get("hours", 24))
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    now = time.time()
    base = max(now, u.get("expires_at", 0))
    u["expires_at"] = base + hours * 3600
    u["_expired_flag"] = False
    save_users(users)
    if un in USER_DELETED:
        USER_DELETED[un].clear()
    asyncio.create_task(start_user_workers(un))
    return web.json_response({"status": "ok"})


async def h_adm_slot(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    count = int(d.get("count", 1))
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    u["slots"] = min(1000, u.get("slots", 0) + count)
    save_users(users)
    return web.json_response({"status": "ok"})


async def h_adm_del(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    users = load_users()
    if un not in users:
        return web.json_response({"status": "error", "error": "User Not Found"})
    await stop_user_workers(un)
    users.pop(un, None)
    save_users(users)
    USER_PAUSED.pop(un, None)
    USER_DELETED.pop(un, None)
    USER_BULK_PAUSED.discard(un)
    USER_MANUAL_PAUSED.discard(un)
    USER_BULK_PAUSED_TIME.pop(un, None)
    USER_HOURLY_RESTART_TARGETS.pop(un, None)
    USER_AUTO_CYCLE_STATE.pop(un, None)
    USER_WORKERS.pop(un, None)
    return web.json_response({"status": "ok"})


async def h_adm_reset_pw(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    new_pw = d.get("password", "").strip()
    if not un or not new_pw:
        return web.json_response({"status": "error", "error": "Username And Password Required"})
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    ph, salt = hash_pw(new_pw)
    u["password_hash"] = ph
    u["salt"] = salt
    users[un] = u
    save_users(users)
    return web.json_response({"status": "ok"})


async def expiry_watcher():
    while True:
        try:
            users = load_users()
            now = time.time()
            changed = False
            for un, u in users.items():
                exp = now >= u.get("expires_at", 0)
                was = u.get("_expired_flag", False)
                if exp and not was:
                    await stop_user_workers(un)
                    u["_expired_flag"] = True
                    changed = True
                elif not exp and was:
                    u["_expired_flag"] = False
                    changed = True
                    asyncio.create_task(start_user_workers(un))
            if changed:
                save_users(users)
        except Exception:
            pass
        await asyncio.sleep(5)


async def _queue_user_once(username: str):
    await asyncio.sleep(0.05)
    if username in USER_BULK_PAUSED:
        return
    await _fill_user_queue_once(username)


async def _fill_user_queue_once(username):
    if username in USER_BULK_PAUSED:
        return 0

    users = load_users()
    user = users.get(username)
    if not user or time.time() >= float(user.get("expires_at", 0) or 0):
        return 0

    max_slots = max(1, min(1000, int(user.get("slots", 1) or 1)))
    deleted = USER_DELETED.setdefault(username, set())
    paused = USER_PAUSED.setdefault(username, set())

    await start_user_workers(username)
    workers = USER_WORKERS.get(username, {})

    saved_keys = set()
    for acc in user.get("accounts", []):
        key = str(acc.get("uid", "")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
        if key and key not in deleted:
            saved_keys.add(key)

    occupied_keys = set(saved_keys)
    for k, task in workers.items():
        sk = str(k)
        if task and not task.done() and sk not in deleted:
            occupied_keys.add(sk)

    free = max(0, max_slots - len(occupied_keys))
    if free <= 0:
        return 0

    pending_data = load_pending_imports()
    raw = pending_data.get(username, [])
    if not isinstance(raw, list) or not raw:
        return 0

    saved = user.setdefault("accounts", [])
    existing = {
        str(a.get("uid", "")) if a.get("type") == "guest" else str(a.get("token", ""))[:20]
        for a in saved
    }

    accepted = []
    accepted_uids = set()
    for item in raw:
        if len(accepted) >= free:
            break
        if not isinstance(item, dict):
            continue
        uid = str(item.get("uid", "")).strip()
        pw = str(item.get("password", "")).strip()
        if not uid or not pw or uid in existing or uid in deleted or uid in paused:
            continue

        job = {
            "type": "guest",
            "uid": uid,
            "password": pw,
            "mode": str(item.get("mode", "LW") or "LW").upper(),
            "created_at": time.time(),
        }
        saved.append(job)
        existing.add(uid)
        accepted.append(job)
        accepted_uids.add(uid)

    if not accepted:
        return 0

    pending_data[username] = [
        x for x in raw
        if not (isinstance(x, dict) and str(x.get("uid", "")).strip() in accepted_uids)
    ][:200]
    users[username] = user
    save_users(users)
    save_pending_imports(pending_data)

    await start_user_workers(username)
    return len(accepted)


async def _worker_queue_scheduler():
    while True:
        try:
            users = load_users()
            now = time.time()
            for username, user in users.items():
                if now >= float(user.get("expires_at", 0) or 0):
                    continue
                if username in USER_BULK_PAUSED:
                    continue
                await _fill_user_queue_once(username)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[queue] scheduler error: {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(3)


async def start_web_dashboard(host="0.0.0.0", port=20335):
    app = web.Application()

    app.router.add_get("/", h_root)
    app.router.add_get("/login", h_login_get)
    app.router.add_post("/login", h_login_post)
    app.router.add_get("/signup", h_signup)
    app.router.add_get("/plans", h_plans)
    app.router.add_get("/logout", h_logout)
    app.router.add_get("/dashboard", h_dash)
    app.router.add_get("/add-new-job", h_add_new_job_page)
    app.router.add_get("/expired", h_expired)
    app.router.add_get("/completed", h_completed_view)
    app.router.add_get("/api/completed-file", h_completed_file)
    app.router.add_get("/api/add-accounts-file", h_add_accounts_file)
    app.router.add_get("/api/level-file/{name}", h_level_file)

    app.router.add_get("/api/stats", h_stats)
    app.router.add_get("/api/br-status", h_api_br_status)
    app.router.add_get("/api/br-start", h_api_br_start)
    app.router.add_get("/api/br-stop", h_api_br_stop)
    app.router.add_get("/api/br-check", h_api_br_check)

    app.router.add_post("/api/account/check", h_acc_check)
    app.router.add_post("/api/account/add-job", h_acc_add_job)
    app.router.add_post("/api/account/import-file", h_acc_import_file)
    app.router.add_post("/api/account/delete", h_acc_delete)
    app.router.add_post("/api/account/bulk", h_acc_bulk)
    app.router.add_post("/api/account/pause", h_acc_pause)
    app.router.add_post("/api/account/resume", h_acc_resume)
    app.router.add_post("/api/account/auto-cycle", h_acc_auto_cycle)

    app.router.add_get(ADMIN_PATH, h_adm_login_get)
    app.router.add_post(ADMIN_PATH, h_adm_login_post)
    app.router.add_get(ADMIN_PATH + "/logout", h_adm_logout)
    app.router.add_get(ADMIN_PATH + "/panel", h_adm_panel)

    app.router.add_post("/api/admin/create-user", h_adm_create)
    app.router.add_get("/api/admin/list-users", h_adm_list)
    app.router.add_post("/api/admin/extend-time", h_adm_ext)
    app.router.add_post("/api/admin/add-slot", h_adm_slot)
    app.router.add_post("/api/admin/delete-user", h_adm_del)
    app.router.add_post("/api/admin/reset-password", h_adm_reset_pw)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    print(f"Web Dashboard Running On http://{host}:{port}", flush=True)
    print(f"Admin Panel: http://{host}:{port}{ADMIN_PATH}", flush=True)
    print(f"[auto] interval={AUTO_RESTART_INTERVAL}s wait={AUTO_RESTART_WAIT}s tick={SCHEDULER_TICK}s", flush=True)

    asyncio.create_task(expiry_watcher())
    asyncio.create_task(_worker_queue_scheduler())
    asyncio.create_task(_auto_stop_restart_scheduler())
    asyncio.create_task(_auto_release_bulk_pause())

    try:
        users = load_users()
        now = time.time()
        for un, u in users.items():
            if now < u.get("expires_at", 0):
                asyncio.create_task(start_user_workers(un))
    except Exception:
        pass


__all__ = ["bot_state", "start_web_dashboard"]

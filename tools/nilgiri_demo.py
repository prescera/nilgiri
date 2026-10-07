#!/usr/bin/env python3
"""
Nilgiri demo server — live network visualization of an Inspect AI eval run.

Usage:
    python3 tools/nilgiri_demo.py [--port 8765] [--logs-dir PATH]

Opens http://localhost:8765 — an animated network topology that highlights
which subsystem the agent is currently probing and marks flags captured in red.
Watches inspect/nilgiri/logs/*.eval for new or updated eval files and
replays their messages in the browser via SSE.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import re
import sys
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOGS = ROOT / "inspect" / "nilgiri" / "logs"
MANIFEST = ROOT / "flags" / "manifest.yaml"

# ---------------------------------------------------------------------------
# Flag / node metadata
# ---------------------------------------------------------------------------

FLAG_TO_NODE: dict[str, str] = {
    "m1.s1": "vpn-portal", "m1.s2": "vpn-portal",
    "m1.s3": "vpn-portal", "m1.s4": "vpn-portal",
    "m2.s1": "fs-charlie", "m2.s2": "fs-charlie", "m2.s3": "fs-charlie",
    "m3.s1": "areuben-ws", "m3.s2": "areuben-ws",
    "m4.s1": "wiki-charlie", "m4.s2": "wiki-charlie",
    "m4.s3": "fs-charlie", "m4.s4": "fs-charlie",
    "m5.s1": "web-oscar", "m5.s2": "db-oscar",
    "m5.s3": "db-oscar", "m5.s4": "db-oscar",
    "m5.s5": "dc1-oscar", "m5.s6": "dc1-oscar",
    "m6.s1": "operator-ws1", "m6.s2": "operator-ws1", "m6.s3": "operator-ws1",
    "m7.s1": "c2-oscar",
    "m7.s2": "ws-alpha", "m7.s3": "ws-alpha", "m7.s4": "ws-alpha",
    "m7.s5": "gitlab-alpha",
    "m8.s1": "dc1-alpha",
    "m9.s1": "secrets-alpha", "m9.s2": "secrets-alpha",
    "m9.s3": "secrets-alpha", "m9.s4": "secrets-alpha",
    "m10.s1": "dc1-alpha", "m10.s2": "teamcity-alpha",
    "m10.s3": "teamcity-alpha", "m10.s4": "secrets-alpha",
    "m10.s5": "teamcity-alpha",
}

IP_TO_NODE: dict[str, str] = {
    "10.99.0.10": "kali",
    "10.10.0.10": "vpn-portal",
    "10.20.0.10": "dc1-charlie",
    "10.20.0.20": "fs-charlie",
    "10.20.0.21": "wiki-charlie",
    "10.20.0.100": "areuben-ws",
    "10.30.0.10": "dc1-oscar",
    "10.30.0.20": "web-oscar",
    "10.30.0.21": "db-oscar",
    "10.30.0.100": "operator-ws1",
    "10.30.0.30": "c2-oscar",
    "10.40.0.10": "dc1-alpha",
    "10.40.0.20": "gitlab-alpha",
    "10.40.0.21": "teamcity-alpha",
    "10.40.0.30": "secrets-alpha",
    "10.40.0.100": "ws-alpha",
}

HOST_TO_NODE: list[tuple[str, str]] = [
    ("vpn-portal", "vpn-portal"), ("vpn_portal", "vpn-portal"),
    ("dc1.charlie", "dc1-charlie"), ("charlie.local", "dc1-charlie"),
    ("fs.charlie", "fs-charlie"),
    ("wiki.charlie", "wiki-charlie"),
    ("areuben-ws", "areuben-ws"),
    ("dc1.oscar", "dc1-oscar"), ("oscar.local", "dc1-oscar"),
    ("web.oscar", "web-oscar"),
    ("db.oscar", "db-oscar"),
    ("operator-ws1", "operator-ws1"),
    ("c2.oscar", "c2-oscar"),
    ("dc1.alpha", "dc1-alpha"), ("alpha.local", "dc1-alpha"),
    ("gitlab.alpha", "gitlab-alpha"),
    ("teamcity.alpha", "teamcity-alpha"),
    ("secrets.alpha", "secrets-alpha"),
    ("ws.alpha", "ws-alpha"),
]

UUID_RE = re.compile(
    r"\b([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\b"
)
IP_RE = re.compile(r"\b(10\.\d+\.\d+\.\d+)\b")

# ---------------------------------------------------------------------------
# Load flag UUIDs from manifest
# ---------------------------------------------------------------------------

def load_flag_uuids() -> dict[str, str]:
    """Return {uuid_lower: step_id} mapping."""
    try:
        import yaml
        with MANIFEST.open() as f:
            doc = yaml.safe_load(f)
        return {e["uuid"].lower(): e["id"] for e in doc["flags"]}
    except Exception:
        return {}


FLAG_UUIDS: dict[str, str] = load_flag_uuids()


def extract_nodes_from_text(text: str) -> list[str]:
    """Return node IDs mentioned in a command / output string."""
    nodes: list[str] = []
    for ip in IP_RE.findall(text):
        if ip in IP_TO_NODE:
            nodes.append(IP_TO_NODE[ip])
    for pattern, node in HOST_TO_NODE:
        if pattern.lower() in text.lower():
            if node not in nodes:
                nodes.append(node)
    return nodes


def extract_flags_from_text(text: str) -> list[str]:
    """Return step IDs whose UUID appears in text."""
    found = []
    for uuid in UUID_RE.findall(text):
        step = FLAG_UUIDS.get(uuid.lower())
        if step:
            found.append(step)
    return found


# ---------------------------------------------------------------------------
# Eval log parser
# ---------------------------------------------------------------------------

def parse_eval_log(path: Path) -> dict[str, Any] | None:
    """Open a completed .eval zip and return parsed content, or None."""
    try:
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            sample_name = next((n for n in names if n.startswith("samples/")), None)
            if not sample_name:
                return None
            sample_data = json.loads(z.read(sample_name))

            header = {}
            if "header.json" in names:
                header = json.loads(z.read("header.json"))
            elif "_journal/start.json" in names:
                header = json.loads(z.read("_journal/start.json"))

            return {"sample": sample_data, "header": header, "path": str(path)}
    except Exception:
        return None


def messages_to_events(messages: list[dict]) -> list[dict]:
    """Convert Inspect message list to demo SSE events."""
    events = []
    total_chars = 0

    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content", "")
        tool_calls = msg.get("tool_calls", [])
        model = msg.get("model", "")

        if role == "system":
            continue

        if role == "tool":
            text = content if isinstance(content, str) else json.dumps(content)
            total_chars += len(text)
            nodes = extract_nodes_from_text(text)
            flags = extract_flags_from_text(text)
            # Truncate to 30 lines or 1200 chars — the feed is for following the
            # attack chain, not reading full command output.
            lines = text.split("\n")
            display = "\n".join(lines[:30])
            if len(display) > 1200:
                display = display[:1200]
            if display != text:
                display += f"\n… ({len(lines)} lines total)"
            ev: dict = {
                "type": "tool_result",
                "content": display,
                "nodes": nodes,
                "tokens": total_chars // 4,
            }
            if flags:
                ev["flags"] = flags
            events.append(ev)
            for f in flags:
                events.append({
                    "type": "flag_captured",
                    "step": f,
                    "node": FLAG_TO_NODE.get(f, ""),
                })
            continue

        if role == "user":
            text = content if isinstance(content, str) else ""
            if "You produced no tool call" in text:
                events.append({"type": "nudge", "content": text})
            continue

        if role == "assistant":
            # Extract text content
            text = ""
            if isinstance(content, str):
                text = content
            elif isinstance(content, list):
                parts = [
                    c.get("text", "") for c in content
                    if isinstance(c, dict) and c.get("type") == "text"
                ]
                text = "\n".join(parts)

            if text.strip():
                total_chars += len(text)
                nodes = extract_nodes_from_text(text)
                flags = extract_flags_from_text(text)
                ev = {
                    "type": "assistant_text",
                    "content": text[:2000],
                    "model": model,
                    "nodes": nodes,
                    "tokens": total_chars // 4,
                }
                if flags:
                    ev["flags"] = flags
                events.append(ev)

            for tc in tool_calls:
                if isinstance(tc, dict):
                    fn_name = tc.get("function", "")
                    args = tc.get("arguments", {})
                    if isinstance(fn_name, dict):
                        args = fn_name.get("arguments", {})
                        fn_name = fn_name.get("name", "")
                    cmd_text = json.dumps(args) if isinstance(args, dict) else str(args)
                    total_chars += len(cmd_text)
                    nodes = extract_nodes_from_text(cmd_text)
                    flags = extract_flags_from_text(cmd_text)
                    ev = {
                        "type": "tool_call",
                        "function": fn_name,
                        "args": args,
                        "nodes": nodes,
                        "tokens": total_chars // 4,
                    }
                    if flags:
                        ev["flags"] = flags
                    events.append(ev)

    events.append({"type": "tokens", "total": total_chars // 4})
    return events


# ---------------------------------------------------------------------------
# Log watcher thread
# ---------------------------------------------------------------------------

class LogWatcher(threading.Thread):
    def __init__(self, logs_dir: Path, event_queue: queue.Queue,
                 replay: Path | None = None, mode: str = "stream"):
        super().__init__(daemon=True)
        self.logs_dir = logs_dir
        self.replay_file = replay
        self.mode = mode  # "stream" or "replay"
        self.q = event_queue
        self._last_file: str = ""        # path of the last complete file handed to _replay
        self._last_msg_count: int = 0
        self._in_progress: str = ""      # name of the currently in-progress file (separate from _last_file)

    def run(self):
        if self.replay_file:
            self.q.put({"type": "watching", "msg": f"Replaying {self.replay_file.name}", "mode": self.mode})
        else:
            self.q.put({"type": "watching", "msg": f"Watching {self.logs_dir}", "mode": self.mode})
        while True:
            try:
                if self.replay_file:
                    self._scan_single(self.replay_file)
                else:
                    self._scan()
            except Exception as exc:
                self.q.put({"type": "error", "msg": str(exc)})
            time.sleep(2)

    def _scan_single(self, path: Path):
        data = parse_eval_log(path)
        if data is None:
            if self._in_progress != path.name:
                self._in_progress = path.name
                self.q.put({"type": "eval_running", "file": path.name})
            return
        self._in_progress = ""
        self._replay(path, data)

    def _scan(self):
        # Defer to ControlChannelPoller while a live eval is streaming.
        with LIVE.lock:
            is_active = LIVE.active
            just_finished = LIVE.just_finished
            if just_finished:
                LIVE.just_finished = False

        if is_active:
            return

        if just_finished:
            # Mark the newest completed file as seen without replaying it —
            # the live stream already showed all its events.
            if self.logs_dir.exists():
                evals = sorted(
                    self.logs_dir.glob("*.eval"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                for path in evals:
                    data = parse_eval_log(path)
                    if data is not None:
                        messages = data["sample"].get("messages", [])
                        self._last_file = str(path)
                        self._last_msg_count = len(messages)
                        self._in_progress = ""
                        break
            return

        if not self.logs_dir.exists():
            return
        evals = sorted(
            self.logs_dir.glob("*.eval"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not evals:
            return

        newest = evals[0]
        data = parse_eval_log(newest)

        if data is None:
            if self.mode == "replay":
                # In replay mode skip incomplete files; find the latest complete one
                for candidate in evals[1:]:
                    cdata = parse_eval_log(candidate)
                    if cdata is not None:
                        self._replay(candidate, cdata)
                        return
                return

            # stream mode: announce the in-progress eval ONCE (keyed by name, not
            # _last_file — keeping them separate prevents _replay from thrashing the
            # buffer every 2 s when the fallback complete file hasn't changed).
            if self._in_progress != newest.name:
                self._in_progress = newest.name
                self.q.put({"type": "eval_running", "file": newest.name})
            # Serve the most recent COMPLETE log while the new one is still writing
            for candidate in evals[1:]:
                cdata = parse_eval_log(candidate)
                if cdata is not None:
                    self._replay(candidate, cdata)
                    break
            return

        self._in_progress = ""
        self._replay(newest, data)

    def _replay(self, path: Path, data: dict):
        messages = data["sample"].get("messages", [])
        header = data["header"]
        eval_info = header.get("eval", {})
        model = eval_info.get("model", "unknown")
        eval_id = eval_info.get("eval_id", "")

        if str(path) != self._last_file:
            self._last_file = str(path)
            self._last_msg_count = 0
            CONNECTIONS.reset_buffer()
            self.q.put({
                "type": "eval_start",
                "model": model,
                "eval_id": eval_id,
                "file": path.name,
            })

        if len(messages) > self._last_msg_count:
            new_msgs = messages[self._last_msg_count:]
            self._last_msg_count = len(messages)
            events = messages_to_events(new_msgs)
            for ev in events:
                self.q.put(ev)
                time.sleep(0.05)  # small delay for visual effect


# ---------------------------------------------------------------------------
# SSE connections registry
# ---------------------------------------------------------------------------

class Connections:
    BUFFER_MAX = 500

    def __init__(self):
        self._lock = threading.Lock()
        self._conns: list[queue.Queue] = []
        self._buffer: list[str] = []  # replay buffer for late-joining clients

    def add(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=1000)
        with self._lock:
            # Send buffered events to new client
            for data in self._buffer:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    break
            self._conns.append(q)
        return q

    def remove(self, q: queue.Queue):
        with self._lock:
            try:
                self._conns.remove(q)
            except ValueError:
                pass

    def broadcast(self, event: dict):
        data = json.dumps(event)
        with self._lock:
            self._buffer.append(data)
            if len(self._buffer) > self.BUFFER_MAX:
                self._buffer = self._buffer[-self.BUFFER_MAX:]
            for q in list(self._conns):
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

    def reset_buffer(self):
        with self._lock:
            self._buffer.clear()


CONNECTIONS = Connections()


# ---------------------------------------------------------------------------
# Live-state coordinator (shared between LogWatcher and ControlChannelPoller)
# ---------------------------------------------------------------------------

class LiveState:
    """Thread-safe flags coordinating live vs. replay modes."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active = False        # True while a live control channel is streaming
        self.just_finished = False # Set briefly when the live run ends


LIVE = LiveState()


# ---------------------------------------------------------------------------
# Inspect control-channel poller (real-time streaming)
# ---------------------------------------------------------------------------

class ControlChannelPoller(threading.Thread):
    """Polls Inspect's AF_UNIX control-channel HTTP server for live eval events.

    When `inspect eval` (or `make eval`) is running, Inspect writes a discovery
    JSON at `<inspect_data_dir>/control/<pid>.json` pointing to an AF_UNIX
    socket.  The socket speaks HTTP; `GET /evals/{run_id}/sample/events` returns
    a page of transcript events.  We poll it every second, deduplicate by uuid,
    and push completed model/tool events into the SSE broadcast queue.
    """

    POLL_INTERVAL = 1.0

    def __init__(self, event_queue: queue.Queue):
        super().__init__(daemon=True)
        self.q = event_queue
        self._run_id: str | None = None   # discovery run_id (process identity)
        self._eval_id: str | None = None  # eval_id used in HTTP endpoints (≠ run_id)
        self._socket_path = None
        self._emitted: set[str] = set()  # event uuids already broadcast

    def run(self):
        while True:
            try:
                self._poll()
            except Exception:
                pass
            time.sleep(self.POLL_INTERVAL)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    @staticmethod
    def _find_server():
        """Return (socket_path, run_id) for the most-recently-started live eval, or (None, None).

        The discovery run_id is a process-level identity; the per-eval eval_id
        (needed for HTTP endpoints) is fetched separately via GET /tasks.
        """
        try:
            from inspect_ai._control.discovery import list_discovered_servers
            servers = list_discovered_servers()
            if servers:
                s = servers[0]
                return s.socket_path, s.run_id
        except Exception:
            pass
        return None, None

    @staticmethod
    def _get_eval_id(client: "httpx.Client", run_id: str) -> str | None:
        """Query GET /tasks and return the eval_id whose run_id matches."""
        try:
            resp = client.get("/tasks")
            if resp.status_code != 200:
                return None
            for task in resp.json():
                if task.get("run_id") == run_id:
                    return task.get("eval_id")
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Main poll tick
    # ------------------------------------------------------------------

    def _poll(self):
        try:
            import httpx
        except ImportError:
            return

        sock, run_id = self._find_server()

        if sock is None or run_id is None:
            if self._run_id is not None:
                # Live eval just ended — hand back to LogWatcher
                self._run_id = None
                self._eval_id = None
                self._socket_path = None
                self._emitted.clear()
                with LIVE.lock:
                    LIVE.active = False
                    LIVE.just_finished = True
            return

        try:
            transport = httpx.HTTPTransport(uds=str(sock))
            with httpx.Client(
                transport=transport, base_url="http://localhost", timeout=5.0
            ) as client:
                # Resolve the eval_id (≠ run_id) needed for endpoint URLs.
                if run_id != self._run_id:
                    eval_id = self._get_eval_id(client, run_id)
                    if eval_id is None:
                        return  # task not registered yet; retry next tick
                    self._run_id = run_id
                    self._eval_id = eval_id
                    self._socket_path = sock
                    self._emitted.clear()
                    with LIVE.lock:
                        LIVE.active = True
                        LIVE.just_finished = False
                    CONNECTIONS.reset_buffer()
                    self.q.put({"type": "eval_start", "model": "",
                                "eval_id": eval_id, "file": "live"})

                # Fetch events from offset 0 with full content, dedup by uuid.
                # full=true returns output/result; completed is an ISO timestamp
                # when done, None when the event is still in progress.
                resp = client.get(
                    f"/evals/{self._eval_id}/sample/events",
                    params={"sample_id": "1", "epoch": "1", "full": "true"},
                )
                if resp.status_code != 200:
                    return
                data = resp.json()
        except Exception:
            return

        events = data.get("events", [])
        done = data.get("done", False)
        first_model: str = ""

        for ev in events:
            if ev.get("completed") is None:  # still in progress — skip
                continue
            uuid = ev.get("uuid", "")
            if not uuid or uuid in self._emitted:
                continue
            self._emitted.add(uuid)

            ev_type = ev.get("event", "")
            if ev_type == "model":
                if not first_model:
                    first_model = ev.get("model", "")
                self._emit_model_event(ev)
            elif ev_type == "tool":
                self._emit_tool_event(ev)

        if first_model:
            self.q.put({"type": "model_info", "model": first_model})

        if done:
            self._run_id = None
            self._eval_id = None
            self._emitted.clear()
            with LIVE.lock:
                LIVE.active = False
                LIVE.just_finished = True

    # ------------------------------------------------------------------
    # Event converters
    # ------------------------------------------------------------------

    def _emit_model_event(self, ev: dict):
        output = ev.get("output", {})
        choices = output.get("choices", [])
        if not choices:
            return
        msg = choices[0].get("message", {})

        # Text content
        content = msg.get("content", "")
        if isinstance(content, list):
            parts = [c.get("text", "") for c in content
                     if isinstance(c, dict) and c.get("type") == "text"]
            text = "\n".join(p for p in parts if p)
        else:
            text = str(content) if content else ""

        if text.strip():
            nodes = extract_nodes_from_text(text)
            flags = extract_flags_from_text(text)
            out: dict = {
                "type": "assistant_text",
                "content": text[:2000],
                "model": ev.get("model", ""),
                "nodes": nodes,
            }
            if flags:
                out["flags"] = flags
            self.q.put(out)
            for f in flags:
                self.q.put({"type": "flag_captured", "step": f, "node": FLAG_TO_NODE.get(f, "")})

        # Tool calls
        for tc in msg.get("tool_calls", []):
            if not isinstance(tc, dict):
                continue
            fn_name = tc.get("function", "")
            args = tc.get("arguments", {})
            if isinstance(fn_name, dict):
                args = fn_name.get("arguments", {})
                fn_name = fn_name.get("name", "")
            cmd_text = json.dumps(args) if isinstance(args, dict) else str(args)
            nodes = extract_nodes_from_text(cmd_text)
            flags = extract_flags_from_text(cmd_text)
            out = {"type": "tool_call", "function": fn_name, "args": args, "nodes": nodes}
            if flags:
                out["flags"] = flags
            self.q.put(out)
            for f in flags:
                self.q.put({"type": "flag_captured", "step": f, "node": FLAG_TO_NODE.get(f, "")})

    def _emit_tool_event(self, ev: dict):
        result = ev.get("result", "") or ""
        if isinstance(result, (dict, list)):
            result = json.dumps(result)
        else:
            result = str(result)

        lines = result.split("\n")
        display = "\n".join(lines[:30])
        if len(display) > 1200:
            display = display[:1200]
        if display != result:
            display += f"\n… ({len(lines)} lines total)"

        nodes = extract_nodes_from_text(result)
        flags = extract_flags_from_text(result)
        out: dict = {"type": "tool_result", "content": display, "nodes": nodes}
        if flags:
            out["flags"] = flags
        self.q.put(out)
        for f in flags:
            self.q.put({"type": "flag_captured", "step": f, "node": FLAG_TO_NODE.get(f, "")})


def broadcast_loop(event_queue: queue.Queue):
    """Forward events from LogWatcher to all SSE connections."""
    while True:
        ev = event_queue.get()
        CONNECTIONS.broadcast(ev)


# ---------------------------------------------------------------------------
# HTML page (loaded from nilgiri_demo.html)
# ---------------------------------------------------------------------------

HTML = (Path(__file__).parent / "nilgiri_demo.html").read_text()


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default access log

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/" or path == "/index.html":
            self._serve_html()
        elif path == "/events":
            self._serve_sse()
        else:
            self.send_error(404)

    def _serve_html(self):
        body = HTML.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        q = CONNECTIONS.add()
        try:
            while True:
                try:
                    data = q.get(timeout=20)
                    self.wfile.write(f"data: {data}\n\n".encode())
                    self.wfile.flush()
                except queue.Empty:
                    # Send heartbeat
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            CONNECTIONS.remove(q)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Nilgiri demo server")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--logs-dir", type=Path, default=DEFAULT_LOGS)
    ap.add_argument("--replay", type=Path, default=None,
                    help="Replay a specific .eval file instead of watching the logs dir")
    ap.add_argument("--mode", choices=["stream", "replay"], default="stream",
                    help="stream: pick up in-progress evals as they run (default); "
                         "replay: only serve completed .eval logs")
    args = ap.parse_args()

    eq: queue.Queue = queue.Queue()

    watcher = LogWatcher(args.logs_dir, eq, replay=args.replay, mode=args.mode)
    watcher.start()

    poller = ControlChannelPoller(eq)
    poller.start()

    broadcast_thread = threading.Thread(target=broadcast_loop, args=(eq,), daemon=True)
    broadcast_thread.start()

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Nilgiri demo → http://localhost:{args.port}", flush=True)
    print(f"Mode: {args.mode}  |  Watching: {args.logs_dir}", flush=True)
    print("Run  make eval MODEL=...  in another terminal to start a live run.", flush=True)
    if args.mode == "replay":
        print("(replay mode: only completed .eval logs are served)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()

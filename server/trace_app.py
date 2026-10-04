"""FastAPI app exposing sync orchestration and a replayable trace comparison view.

Run with::

    uvicorn server.trace_app:app

Endpoints:

* ``POST /sync``  - run one sync round against both a LocalStore and a (possibly
  fakeredis-fallback) RedisStore backend; one structured event per backend.
* ``GET /trace``  - JSON view of recorded events, filterable by ``backend`` / ``sync_id``.
* ``GET /trace/{event_id}`` - one event with its diff snapshot and partial marker.
* ``GET /``      - an interactive comparison page (click an event for its detail).
"""

from typing import ClassVar, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from diffsync import Adapter, DiffSyncModel
from diffsync.store.local import LocalStore
from diffsync.store.redis import RedisStore
from diffsync.tracing import TracedStore, get_tracer, traced_sync


# ---------------------------------------------------------------------------
# Demo domain models
# ---------------------------------------------------------------------------
class Site(DiffSyncModel):
    """A site that contains devices."""

    _modelname = "site"
    _identifiers = ("name",)
    _children = {"device": "devices"}

    name: str
    devices: List = []


class Device(DiffSyncModel):
    """A device with a role."""

    _modelname = "device"
    _identifiers = ("name",)
    _attributes: ClassVar = ("role",)

    name: str
    role: str = ""


def _build_adapter(data: Dict[str, Dict[str, str]], name: str) -> Adapter:
    """Build a fresh, loaded source adapter from a ``{site: {device: role}}`` map."""

    class _Backend(Adapter):
        site = Site
        device = Device
        top_level = ["site"]

    adapter = _Backend(name=name)
    for site_name, devices in data.items():
        site = adapter.site(name=site_name)
        adapter.add(site)
        for device_name, role in devices.items():
            device = adapter.device(name=device_name, role=role)
            adapter.add(device)
            site.add_child(device)
    return adapter


class _Destination(Adapter):
    """Persistent destination adapter; its concrete store is injected per backend."""

    site = Site
    device = Device
    top_level = ["site"]


# The two rounds used by default by POST /sync.
ROUND_DATA = [
    {"nyc": {"nyc-r1": "spine", "nyc-r2": "leaf"}},
    {
        "nyc": {"nyc-r1": "spine", "nyc-r2": "super-spine"},
        "sfo": {"sfo-r1": "leaf"},
    },
]


# ---------------------------------------------------------------------------
# Persistent destinations (created once, reused across POST /sync rounds)
# ---------------------------------------------------------------------------
def _make_local_destination() -> _Destination:
    return _Destination(name="dest-local", internal_storage_engine=TracedStore(LocalStore(), backend="local"))


def _make_redis_destination() -> _Destination:
    inner = RedisStore(store_id="trace-demo", host="127.0.0.1", port=6379)
    return _Destination(name="dest-redis", internal_storage_engine=TracedStore(inner, backend="redis"))


LOCAL_DST = _make_local_destination()
REDIS_DST = _make_redis_destination()


class SyncRequestBody(BaseModel):
    """Optional body for POST /sync."""

    round: int = 1
    data: Optional[Dict[str, Dict[str, str]]] = None
    redis_fail_step: Optional[int] = None


app = FastAPI(title="diffsync sync tracer")


@app.post("/sync")
def run_sync(body: Optional[SyncRequestBody] = None) -> Dict:
    """Run one sync round against both backends and return the pair of event ids."""
    body = body or SyncRequestBody()
    round_number = body.round if body.data is None else -1
    if body.data is not None:
        data = body.data
    else:
        data = ROUND_DATA[(round_number - 1) % len(ROUND_DATA)]

    sync_id = f"round-{round_number}"

    # Reset per-sync failure injection, then optionally arm the Redis one.
    LOCAL_DST.store.fail_on_write = None
    REDIS_DST.store.fail_on_write = body.redis_fail_step

    local_event = traced_sync(source=_build_adapter(data, f"source-{sync_id}"), destination=LOCAL_DST, sync_id=sync_id)
    redis_event = traced_sync(source=_build_adapter(data, f"source-{sync_id}"), destination=REDIS_DST, sync_id=sync_id)

    return {
        "sync_id": sync_id,
        "round": round_number,
        "events": {
            "local": local_event.event_id,
            "redis": redis_event.event_id,
        },
    }


@app.get("/trace")
def list_trace(
    backend: Optional[str] = Query(default=None),
    sync_id: Optional[str] = Query(default=None),
) -> Dict:
    """Return recorded events (newest first), optionally filtered."""
    events = get_tracer().events(backend=backend, sync_id=sync_id)
    return {"count": len(events), "events": [event.as_dict() for event in events]}


@app.get("/trace/{event_id}")
def get_event(event_id: str) -> Dict:
    """Return a single event (diff snapshot, writes, partial marker, readback)."""
    try:
        return get_tracer().get_event(event_id).as_dict()
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown event {event_id}") from None


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>diffsync sync tracer</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, sans-serif; margin: 0; background: #0f1420; color: #e6e9ef; }
  header { padding: 16px 24px; background: #161d2e; border-bottom: 1px solid #2a3550; }
  h1 { font-size: 18px; margin: 0 0 4px; }
  .sub { color: #93a0bd; font-size: 13px; }
  .wrap { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 16px 24px; }
  .col h2 { font-size: 14px; color: #9fb0d8; margin: 0 0 8px; }
  .card { background: #161d2e; border: 1px solid #2a3550; border-radius: 10px; margin-bottom: 10px; overflow: hidden; }
  .card.head { display: flex; align-items: center; gap: 8px; padding: 10px 12px; cursor: pointer; }
  .card.head:hover { background: #1c2540; }
  .pill { font-size: 11px; padding: 2px 8px; border-radius: 999px; font-weight: 600; }
  .pill.partial { background: #5a2a22; color: #ffb4a6; }
  .pill.success { background: #1e462f; color: #88e0a0; }
  .pill.fallback { background: #4a3a12; color: #f0cf7a; }
  .pill.backend { background: #26304d; color: #b9c6ec; }
  .meta { color: #8a97b8; font-size: 12px; }
  .body { display: none; padding: 0 12px 12px; border-top: 1px solid #2a3550; }
  .card.open .body { display: block; }
  pre { background: #0d1322; border: 1px solid #232e49; border-radius: 8px; padding: 10px; overflow:auto; font-size: 12px; max-height: 340px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #232e49; vertical-align: top; }
  th { color: #9fb0d8; font-weight: 600; }
  .err { color: #ff9d8c; }
  .ok { color: #7fdc9b; }
  button { background: #2b57d9; color: white; border: 0; border-radius: 8px; padding: 8px 14px; cursor: pointer; font-size: 13px; }
  button:hover { background: #3a66ec; }
  .empty { color: #8a97b8; font-size: 13px; padding: 12px; }
</style>
</head>
<body>
<header>
  <h1>diffsync persistent sync tracer</h1>
  <div class="sub">LocalStore vs RedisStore &middot; click an event for its diff snapshot, replay prefix and partial marker</div>
  <div style="margin-top:10px"><button onclick="runSync(1)">POST /sync (round 1)</button>
  <button onclick="runSync(2)">POST /sync (round 2: redis partial)</button>
  <button onclick="load()">Refresh GET /trace</button></div>
</header>
<div class="wrap">
  <div class="col"><h2>LocalStore</h2><div id="local"></div></div>
  <div class="col"><h2>RedisStore</h2><div id="redis"></div></div>
</div>
<script>
function esc(s){return String(s==null?"":s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));}
function pill(cls, text){return `<span class="pill ${cls}">${esc(text)}</span>`;}
function card(ev){
  const status = ev.partial ? "partial" : "success";
  let pills = pill("backend", ev.store_backend) + pill(status, ev.partial ? "PARTIAL" : "OK");
  if(ev.fallback) pills += pill("fallback", "FALLBACK");
  const head = `<div class="card head" onclick="this.parentElement.classList.toggle('open')">
    <div>${pills}</div>
    <div class="meta">${esc(ev.sync_id)} &middot; ${esc(ev.source)} → ${esc(ev.destination)}
    &middot; ${ev.writes.length} writes &middot; readback ${ev.readback.equal ? '<span class=ok>equal</span>' : '<span class=err>diverges</span>'}
    &middot; ${ev.duration_ms ? ev.duration_ms.toFixed(1)+"ms" : ""}</div></div>`;
  const writes = ev.writes.map(w=>`<tr>
      <td>${w.seq}</td><td>${esc(w.op)}</td><td>${esc(w.model)}</td><td>${esc(w.uid)}</td>
      <td class="${w.acknowledged?'ok':'err'}">${w.acknowledged?'committed':'FAILED'}</td>
      <td>${w.ancestors.map(a=>esc(a.join('/'))).join('<br>')}</td>
      <td>${w.error?esc(w.error):''}</td></tr>`).join('');
  const prefix = ev.replay_prefix.map(w=>`${w.seq}:${w.op} ${w.model}/${w.uid}`).join(' → ') || '(empty)';
  const body = `<div class="body">
    <div class="meta">expected application order (parent → child)</div>
    <pre>${esc(ev.expected_order.map(o=>`${o.action.padEnd(6)} ${o.model}/${o.uid}`).join('\\n'))}</pre>
    <div class="meta">writes (replay prefix: ${esc(prefix)})</div>
    <table><tr><th>#</th><th>op</th><th>model</th><th>uid</th><th>result</th><th>ancestors</th><th>error</th></tr>${writes}</table>
    <div class="meta">diff snapshot</div>
    <pre>${esc(JSON.stringify(ev.diff_snapshot,null,2))}</pre>
    <div class="meta">readback: ${ev.readback.equal ? 'equal' : 'NOT equal'} (${ev.readback.divergent.length} divergent)</div>
    <pre>${esc(JSON.stringify(ev.readback.divergent,null,2))}</pre>
  </div>`;
  return `<div class="card">${head}${body}</div>`;
}
async function load(){
  const r = await fetch('/trace'); const data = await r.json();
  for(const b of ['local','redis']){
    const evs = data.events.filter(e=>e.store_backend===b);
    document.getElementById(b).innerHTML = evs.length ? evs.map(card).join('') : '<div class=empty>no events yet</div>';
  }
}
async function runSync(n){
  const opts = n===2 ? {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({round:2,redis_fail_step:2})}
                     : {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({round:1})};
  await fetch('/sync',opts); load();
}
load();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """Render the interactive trace comparison page."""
    return INDEX_HTML

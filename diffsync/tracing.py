"""Persistent sync tracing layer for diffsync.

This module wraps the :class:`~diffsync.store.BaseStore` protocol with a
decorator (:class:`TracedStore`) and records one structured :class:`SyncEvent`
per ``sync_from``/``sync_to`` call. Each event captures:

* the full diff sequence (``diff_snapshot``),
* the parent-before-child application order (``expected_order`` + per-write frames),
* the persisted write results and a replayable prefix (``writes``),
* a post-sync readback comparison against the source adapter (``readback``),
* ``partial`` / ``fallback`` flags.

The three design tradeoffs documented in ``docs/tracing.md`` are implemented here:

1. A RedisStore incremental write that fails on the 2nd+ step is **not** considered
   committed; the event is marked ``partial`` and ``replay_prefix`` holds the writes
   already acknowledged.
2. If a child diff is applied outside of its parent's application window, the
   **wrapper** (this module) raises :class:`TraceOrderError`; stores stay dumb.
3. A ``create -> update -> delete`` sequence for the same path is kept as
   **per-write** traces (never folded), so a single sync's readback comparison is
   based on the acknowledged writes rather than a folded terminal state.
"""

import contextvars
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

from diffsync.enum import DiffSyncActions, DiffSyncModelFlags
from diffsync.exceptions import ObjectNotFound, ObjectStoreException

if TYPE_CHECKING:
    from diffsync import Adapter, DiffSyncModel
    from diffsync.diff import DiffElement
    from diffsync.store import BaseStore

from diffsync.store import BaseStore


class TraceError(Exception):
    """Base class for errors raised by the tracing layer."""


class TraceOrderError(TraceError):
    """Raised by the wrapper when a child diff is applied outside of its parent window."""


class StoreWriteError(ObjectStoreException):
    """Raised to simulate/represent a failed incremental store write."""


@dataclass
class SyncFrame:
    """Application frame for one :class:`~diffsync.diff.DiffElement` during a sync."""

    model: str
    uid: str
    action: Optional[str]
    ancestors: List[Tuple[str, str]] = field(default_factory=list)
    natural_delete: bool = False

    @property
    def node(self) -> Tuple[str, str]:
        """Identify this frame as a ``(model, uid)`` tuple."""
        return (self.model, self.uid)


@dataclass
class WriteRecord:
    """One acknowledged (or attempted) store write in application order."""

    seq: int
    op: str
    model: str
    uid: str
    store: str
    backend: str
    acknowledged: bool
    error: Optional[str] = None
    ancestors: List[Tuple[str, str]] = field(default_factory=list)
    payload: Optional[Dict[str, Any]] = None

    def as_dict(self) -> Dict[str, Any]:
        """Render this record as a JSON-safe dict."""
        return {
            "seq": self.seq,
            "op": self.op,
            "model": self.model,
            "uid": self.uid,
            "store": self.store,
            "backend": self.backend,
            "acknowledged": self.acknowledged,
            "error": self.error,
            "ancestors": [list(node) for node in self.ancestors],
            "payload": self.payload,
        }


@dataclass
class SyncEvent:
    """A structured trace of one diffsync sync."""

    event_id: str
    started_at: float
    sync_id: str
    direction: str
    source: str
    destination: str
    store_backend: str
    store_name: str
    fallback: bool = False
    fallback_reason: Optional[str] = None
    flags: int = 0
    diff_summary: Dict[str, int] = field(default_factory=dict)
    diff_snapshot: Dict[str, Any] = field(default_factory=dict)
    expected_order: List[Dict[str, str]] = field(default_factory=list)
    writes: List[WriteRecord] = field(default_factory=list)
    status: str = "running"
    partial: bool = False
    error: Optional[str] = None
    changed: Optional[bool] = None
    duration_ms: Optional[float] = None
    readback: Dict[str, Any] = field(default_factory=dict)
    finished_at: Optional[float] = None

    @property
    def replay_prefix(self) -> List[WriteRecord]:
        """Acknowledged writes, in application order, safe to replay."""
        return [record for record in self.writes if record.acknowledged]

    def as_dict(self) -> Dict[str, Any]:
        """Render this event (including the replayable prefix) as a JSON-safe dict."""
        return {
            "event_id": self.event_id,
            "sync_id": self.sync_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "direction": self.direction,
            "source": self.source,
            "destination": self.destination,
            "store_backend": self.store_backend,
            "store_name": self.store_name,
            "status": self.status,
            "partial": self.partial,
            "fallback": self.fallback,
            "fallback_reason": self.fallback_reason,
            "error": self.error,
            "changed": self.changed,
            "flags": self.flags,
            "diff_summary": self.diff_summary,
            "diff_snapshot": self.diff_snapshot,
            "expected_order": self.expected_order,
            "writes": [record.as_dict() for record in self.writes],
            "replay_prefix": [record.as_dict() for record in self.replay_prefix],
            "readback": self.readback,
        }


# Context holding (tracer, event_id) while a traced sync is in flight.
_sync_ctx: "contextvars.ContextVar[Optional[Tuple[SyncTracer, str]]]" = contextvars.ContextVar(
    "diffsync_sync_ctx", default=None
)
# Open SyncFrame stack for the in-flight sync.
_frame_stack: "contextvars.ContextVar[List[SyncFrame]]" = contextvars.ContextVar("diffsync_frame_stack", default=[])


def current_sync() -> Optional[Tuple["SyncTracer", str]]:
    """Return the active ``(tracer, event_id)`` tuple, if a traced sync is running."""
    return _sync_ctx.get()


def push_frame(frame: SyncFrame) -> None:
    """Push an application frame onto the sync stack."""
    stack = list(_frame_stack.get())
    stack.append(frame)
    _frame_stack.set(stack)


def pop_frame() -> None:
    """Pop the most recent application frame."""
    stack = list(_frame_stack.get())
    if stack:
        stack.pop()
    _frame_stack.set(stack)


def _frame_for(model: str, uid: str) -> Tuple[Optional[SyncFrame], List[SyncFrame]]:
    stack = _frame_stack.get()
    match = next((frame for frame in reversed(stack) if frame.model == model and frame.uid == uid), None)
    return match, stack


def record_write(
    *,
    store: "BaseStore",
    op: str,
    obj: Optional["DiffSyncModel"] = None,
    modelname: Optional[str] = None,
    uid: Optional[str] = None,
    simulate_failure: bool = False,
    fail_message: str = "simulated incremental store write failure",
    delegated: bool = False,
) -> WriteRecord:
    """Record one store write; validate parent/child order and surface failures.

    Args:
        store: The traced store receiving the write.
        op: One of ``add``/``update``/``remove``.
        obj: The model being written (for add/update/remove of an object).
        modelname: Explicit target model name (for ``remove_item``).
        uid: Explicit target unique id (for ``remove_item``).
        simulate_failure: If True, raise :class:`StoreWriteError` instead of committing.
        fail_message: Message attached to the simulated failure.
        delegated: True for a write recorded on behalf of the syncer (e.g. an update
            that diffsync applies in-memory and the tracing layer additionally persists).

    Returns:
        The recorded :class:`WriteRecord`.

    Raises:
        TraceOrderError: if the write lands outside of its parent's application window.
        StoreWriteError: if ``simulate_failure`` is set.
    """
    if obj is not None:
        modelname = obj.get_type()
        uid = obj.get_unique_id()
    if modelname is None or uid is None:
        raise ValueError("record_write requires either obj or both modelname and uid")

    ctx = current_sync()
    if ctx is None:
        # Writes outside of a traced sync (load(), manual edits) are not recorded.
        if simulate_failure:
            raise StoreWriteError(fail_message)
        return WriteRecord(
            seq=-1,
            op=op,
            model=modelname,
            uid=uid,
            store=str(store),
            backend=getattr(store, "backend", store.__class__.__name__),
            acknowledged=False,
            error="not-traced",
        )

    tracer, event_id = ctx
    event = tracer.get_event(event_id)
    frame, stack = _frame_for(modelname, uid)

    # Tradeoff #2: parent/child application order is enforced by the wrapper.
    # A non-cascade create/update/delete must be applied while every ancestor frame
    # is still open, i.e. strictly within its parent's application window.
    if frame is not None and frame.action != DiffSyncActions.DELETE and not delegated:
        open_nodes = {open_frame.node for open_frame in stack}
        for ancestor in frame.ancestors:
            if ancestor not in open_nodes:
                raise TraceOrderError(
                    f"{modelname} {uid} ({frame.action}) applied out of order: "
                    f"ancestor {ancestor[0]} {ancestor[1]} is not currently being applied"
                )

    seq = len(event.writes) + 1
    backend = getattr(store, "backend", store.__class__.__name__)
    payload = None
    if obj is not None:
        payload = {"ids": obj.get_identifiers(), "attrs": obj.get_attrs()}

    if simulate_failure:
        record = WriteRecord(
            seq=seq,
            op=op,
            model=modelname,
            uid=uid,
            store=str(store),
            backend=backend,
            acknowledged=False,
            error=fail_message,
            ancestors=list(frame.ancestors) if frame else [],
            payload=payload,
        )
        event.writes.append(record)
        raise StoreWriteError(fail_message)

    record = WriteRecord(
        seq=seq,
        op=op,
        model=modelname,
        uid=uid,
        store=str(store),
        backend=backend,
        acknowledged=True,
        ancestors=list(frame.ancestors) if frame else [],
        payload=payload,
    )
    event.writes.append(record)
    return record


def _uid_for(element: "DiffElement", dst: "Adapter") -> str:
    model_class: "DiffSyncModel" = getattr(dst, element.type)
    return model_class.create_unique_id(**element.keys)


def _natural_delete(element: "DiffElement", dst: "Adapter") -> Tuple[bool, bool]:
    """Return ``(natural_delete, skip_children)`` for a delete element."""
    if element.action != DiffSyncActions.DELETE:
        return False, False
    model_class = getattr(dst, element.type)
    try:
        model = dst.get(model_class, element.keys)
    except ObjectNotFound:
        return False, False
    natural = bool(model.model_flags & DiffSyncModelFlags.NATURAL_DELETION_ORDER)
    skip_children = bool(model.model_flags & DiffSyncModelFlags.SKIP_CHILDREN_ON_DELETE)
    return natural and not skip_children, skip_children


def _expected_order(element: "DiffElement", dst: "Adapter") -> Iterator[Dict[str, str]]:
    """Yield the canonical application order entries (parent/child aware)."""
    action = element.action
    natural_delete, skip_children = _natural_delete(element, dst)
    entry = {"model": element.type, "uid": _uid_for(element, dst), "action": action or "no-change"}

    if action == DiffSyncActions.DELETE and natural_delete:
        for child in element.get_children():
            yield from _expected_order(child, dst)
        yield entry
        return

    yield entry
    if not (action == DiffSyncActions.DELETE and skip_children):
        for child in element.get_children():
            yield from _expected_order(child, dst)


def snapshot_adapter(adapter: "Adapter") -> Dict[str, Dict[str, Any]]:
    """Capture identifiers + attributes (no child uid lists) for every stored object."""
    snapshot: Dict[str, Dict[str, Any]] = {}
    for modelname in adapter.get_all_model_names():
        snapshot[modelname] = {}
        for obj in adapter.get_all(modelname):
            data = {}
            data.update(obj.get_identifiers())
            data.update(obj.get_attrs())
            snapshot[modelname][obj.get_unique_id()] = data
    return snapshot


def readback_compare(source: "Adapter", dst: "Adapter") -> Dict[str, Any]:
    """Compare the source snapshot against the destination after a sync."""
    source_snap = snapshot_adapter(source)
    dest_after = snapshot_adapter(dst)
    equal = source_snap == dest_after
    divergent: List[Dict[str, str]] = []
    for modelname in sorted(set(source_snap) | set(dest_after)):
        src_uids = set(source_snap.get(modelname, {}))
        dst_uids = set(dest_after.get(modelname, {}))
        for uid in sorted(src_uids - dst_uids):
            divergent.append({"model": modelname, "uid": uid, "reason": "missing-in-destination"})
        for uid in sorted(dst_uids - src_uids):
            divergent.append({"model": modelname, "uid": uid, "reason": "stale-in-destination"})
        for uid in sorted(src_uids & dst_uids):
            if source_snap[modelname][uid] != dest_after[modelname][uid]:
                divergent.append({"model": modelname, "uid": uid, "reason": "attrs-mismatch"})
    return {
        "equal": equal,
        "divergent": divergent,
        "source": source_snap,
        "destination_after": dest_after,
    }


class SyncTracer:
    """In-memory, thread-safe registry of structured sync events."""

    def __init__(self) -> None:
        """Initialize an empty event log."""
        self._events: List[SyncEvent] = []
        self._lock = threading.Lock()

    def reset(self) -> None:
        """Drop all recorded events."""
        with self._lock:
            self._events = []

    def add_event(self, event: SyncEvent) -> None:
        """Register a new event."""
        with self._lock:
            self._events.append(event)

    def get_event(self, event_id: str) -> SyncEvent:
        """Look up an event by id."""
        with self._lock:
            for event in self._events:
                if event.event_id == event_id:
                    return event
        raise KeyError(event_id)

    def events(self, backend: Optional[str] = None, sync_id: Optional[str] = None) -> List[SyncEvent]:
        """Return recorded events, newest first, optionally filtered by backend/sync_id."""
        with self._lock:
            events = list(self._events)
        if backend:
            events = [event for event in events if event.store_backend == backend]
        if sync_id:
            events = [event for event in events if event.sync_id == sync_id]
        events.reverse()
        return events


# Module-level default tracer used by the FastAPI app and traced_sync().
_default_tracer = SyncTracer()


def get_tracer() -> SyncTracer:
    """Return the module-level default tracer."""
    return _default_tracer


def set_tracer(tracer: SyncTracer) -> None:
    """Replace the module-level default tracer."""
    global _default_tracer  # pylint: disable=global-statement
    _default_tracer = tracer


@contextmanager
def _sync_scope(tracer: SyncTracer, event_id: str) -> Iterator[None]:
    token = _sync_ctx.set((tracer, event_id))
    _frame_stack.set([])
    try:
        yield
    finally:
        _frame_stack.set([])
        _sync_ctx.reset(token)


def _patch_syncer_frames(dst: "Adapter") -> Tuple[Any, Any]:
    """Monkeypatch DiffSyncSyncer.sync_diff_element to push/pop SyncFrames.

    Returns a ``(patch, undo)`` pair.
    """
    from diffsync.helpers import DiffSyncSyncer

    original = DiffSyncSyncer.sync_diff_element

    def instrumented(self, element, parent_model=None):  # type: ignore[no-untyped-def]
        uid = _uid_for(element, self.dst_diffsync)
        natural_delete, _skip = _natural_delete(element, self.dst_diffsync)
        ancestors: List[Tuple[str, str]] = []
        for open_frame in _frame_stack.get():
            ancestors.append(open_frame.node)
        frame = SyncFrame(
            model=element.type,
            uid=uid,
            action=element.action,
            ancestors=ancestors,
            natural_delete=natural_delete,
        )
        push_frame(frame)
        try:
            return original(self, element, parent_model)
        finally:
            pop_frame()

    DiffSyncSyncer.sync_diff_element = instrumented

    def undo() -> None:
        DiffSyncSyncer.sync_diff_element = original

    return instrumented, undo


def _patch_syncer_update() -> Tuple[Any, Any]:
    """Persist UPDATE outcomes through the store so they land in Redis and the trace.

    diffsync's default syncer mutates updated objects in memory and relies on a store
    whose objects share identity with it (LocalStore). For pickling stores (RedisStore)
    that means an update never reaches the backend. The tracing layer additionally
    funnels every successful update through ``store.update`` so it is persisted and
    recorded (this is a no-op for LocalStore, which early-returns for same identity).

    Returns a ``(patch, undo)`` pair.
    """
    from diffsync.helpers import DiffSyncSyncer

    original_sync_model = DiffSyncSyncer.sync_model

    def instrumented_sync_model(self, src_model, dst_model, ids, attrs):  # type: ignore[no-untyped-def]
        changed, model = original_sync_model(self, src_model, dst_model, ids, attrs)
        if changed and model is not None and self.action == DiffSyncActions.UPDATE and current_sync() is not None:
            self.dst_diffsync.store.update(obj=model)
        return changed, model

    DiffSyncSyncer.sync_model = instrumented_sync_model

    def undo() -> None:
        DiffSyncSyncer.sync_model = original_sync_model

    return instrumented_sync_model, undo


def traced_sync(
    *,
    source: "Adapter",
    destination: "Adapter",
    direction: str = "sync_from",
    flags: Any = None,
    tracer: Optional[SyncTracer] = None,
    sync_id: Optional[str] = None,
    raise_on_error: bool = False,
) -> SyncEvent:
    """Run a traced sync of ``source`` into ``destination`` and record an event.

    Args:
        source: Source adapter.
        destination: Destination adapter (its store should be a :class:`TracedStore`).
        direction: ``sync_from`` or ``sync_to`` (cosmetic; data always flows source -> destination).
        flags: Optional :class:`~diffsync.enum.DiffSyncFlags`.
        tracer: Tracer to record into; defaults to the module-level tracer.
        sync_id: Shared id grouping multiple backend events from one user action.
        raise_on_error: If True, re-raise after recording; otherwise the event carries the error.

    Returns:
        The recorded :class:`SyncEvent`.
    """
    from diffsync.enum import DiffSyncFlags

    tracer = tracer or get_tracer()
    flags = DiffSyncFlags.NONE if flags is None else flags
    sync_id = sync_id or uuid.uuid4().hex
    store = destination.store
    backend = getattr(store, "backend", store.__class__.__name__)

    event = SyncEvent(
        event_id=uuid.uuid4().hex,
        started_at=time.time(),
        sync_id=sync_id,
        direction=direction,
        source=source.name,
        destination=destination.name,
        store_backend=backend,
        store_name=str(store),
        fallback=bool(getattr(store, "fallback", False)),
        fallback_reason=getattr(store, "fallback_reason", None),
        flags=int(flags.value if hasattr(flags, "value") else flags),
    )
    tracer.add_event(event)

    started = time.time()
    _patch, undo = _patch_syncer_frames(destination)
    _update_patch, undo_update = _patch_syncer_update()
    try:
        with _sync_scope(tracer, event.event_id):
            diff = (
                source.diff_to(destination, flags=flags)
                if direction == "sync_to"
                else destination.diff_from(source, flags=flags)
            )
            event.diff_summary = diff.summary()
            event.diff_snapshot = _json_safe(diff.dict())
            event.expected_order = [entry for top in diff.get_children() for entry in _expected_order(top, destination)]

            try:
                if direction == "sync_to":
                    result_diff = source.sync_to(destination, flags=flags, diff=diff)
                else:
                    result_diff = destination.sync_from(source, flags=flags, diff=diff)
                event.changed = result_diff.has_diffs()
            except BaseException as sync_error:  # noqa: BLE001 - recorded on the event
                event.partial = True
                event.error = f"{sync_error.__class__.__name__}: {sync_error}"
                event.status = "partial"
                event.finished_at = time.time()
                event.duration_ms = (event.finished_at - started) * 1000
                event.readback = readback_compare(source, destination)
                if raise_on_error:
                    raise
                return event

            event.finished_at = time.time()
            event.duration_ms = (event.finished_at - started) * 1000
            event.readback = readback_compare(source, destination)
            event.status = "success"
            event.partial = not event.readback["equal"]
            return event
    finally:
        undo_update()
        undo()


def _json_safe(value: Any) -> Any:
    """Convert dict/list/scalar structures (and enum keys) into JSON-safe values."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


_WRITE_METHODS = ("add", "update", "remove", "remove_item")


class TracedStore(BaseStore):
    """Decorator around any :class:`~diffsync.store.BaseStore` that records sync writes.

    Delegates every read/write call to the wrapped store but intercepts the mutating
    protocol methods (``add``, ``update``, ``remove``, ``remove_item``) so that
    :func:`record_write` can capture the write sequence and enforce parent/child order.
    """

    def __init__(self, inner: "BaseStore", backend: Optional[str] = None) -> None:
        """Wrap ``inner``.

        Args:
            inner: The concrete store (LocalStore/RedisStore/...) to wrap.
            backend: Backend label; inferred from the inner class name if omitted.
        """
        self._inner = inner
        inferred = inner.__class__.__name__.replace("Store", "").lower()
        self.backend = backend or inferred
        # When positive, the Nth write raises StoreWriteError (1-indexed). Reset per sync.
        self.fail_on_write: Optional[int] = None

    # ------------------------------------------------------------------
    # Proxy plumbing
    # ------------------------------------------------------------------
    @property
    def adapter(self) -> Any:
        """Proxy the inner store's adapter."""
        return self._inner.adapter

    @adapter.setter
    def adapter(self, value: Any) -> None:
        self._inner.adapter = value

    @property
    def name(self) -> str:
        """Proxy the inner store's name."""
        return self._inner.name

    @name.setter
    def name(self, value: str) -> None:
        self._inner.name = value

    @property
    def fallback(self) -> bool:
        """Whether the wrapped store is a fallback (e.g. fakeredis)."""
        return bool(getattr(self._inner, "fallback", False))

    @property
    def fallback_reason(self) -> Optional[str]:
        """Why the wrapped store fell back, if any."""
        return getattr(self._inner, "fallback_reason", None)

    def __str__(self) -> str:
        """Render the wrapped store's label."""
        return f"traced:{self._inner}"

    def __getattr__(self, item: str) -> Any:
        """Proxy unknown attributes to the wrapped store."""
        # Only called for attributes not found on this wrapper.
        return getattr(self._inner, item)

    # ------------------------------------------------------------------
    # Read protocol (explicit delegation; BaseStore defines these so
    # __getattr__ would never be invoked for them).
    # ------------------------------------------------------------------
    def get_all_model_names(self) -> Any:
        """Delegate to the inner store."""
        return self._inner.get_all_model_names()

    def get(self, *, model: Any, identifier: Any) -> "DiffSyncModel":
        """Delegate to the inner store."""
        return self._inner.get(model=model, identifier=identifier)

    def get_all(self, *, model: Any) -> Any:
        """Delegate to the inner store."""
        return self._inner.get_all(model=model)

    def get_by_uids(self, *, uids: Any, model: Any) -> Any:
        """Delegate to the inner store."""
        return self._inner.get_by_uids(uids=uids, model=model)

    def count(self, *, model: Any = None) -> int:
        """Delegate to the inner store."""
        return self._inner.count(model=model)

    # ------------------------------------------------------------------
    # Failure injection
    # ------------------------------------------------------------------
    def _next_fails(self) -> bool:
        if self.fail_on_write is None:
            return False
        count = len(current_sync()[0].get_event(current_sync()[1]).writes) + 1 if current_sync() else 0
        return count == self.fail_on_write

    # ------------------------------------------------------------------
    # Traced write protocol
    # ------------------------------------------------------------------
    def add(self, *, obj: "DiffSyncModel") -> None:
        """Trace then delegate an ``add`` write."""
        record_write(store=self, op="add", obj=obj, simulate_failure=self._next_fails())
        return self._inner.add(obj=obj)

    def update(self, *, obj: "DiffSyncModel") -> None:
        """Trace then delegate an ``update`` write."""
        record_write(store=self, op="update", obj=obj, simulate_failure=self._next_fails())
        return self._inner.update(obj=obj)

    def remove_item(self, modelname: str, uid: str) -> None:
        """Trace then delegate a ``remove_item`` write."""
        record_write(
            store=self,
            op="remove",
            modelname=modelname,
            uid=uid,
            simulate_failure=self._next_fails(),
        )
        return self._inner.remove_item(modelname, uid)

    def remove(self, *, obj: "DiffSyncModel", remove_children: bool = False) -> None:
        """Trace ``remove``; the recursive cascade records each child remove_item too."""
        return BaseStore.remove(self, obj=obj, remove_children=remove_children)


def wrap_store(store: "BaseStore", backend: Optional[str] = None) -> TracedStore:
    """Wrap a store with tracing if it isn't already wrapped."""
    if isinstance(store, TracedStore):
        return store
    return TracedStore(store, backend=backend)

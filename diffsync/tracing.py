"""Persistent sync tracing layer.

Wraps the DiffSync Store protocol (see :class:`diffsync.store.BaseStore`) with a
decorator-based tracer so that every sync applied to a ``LocalStore`` or
``RedisStore`` is recorded as a sequence of structured events: the diff
sequence, the parent/child application order and the per-op persistence result.

Design decisions (see ``docs/tracing.md`` for the full rationale):

1. A sync whose incremental writes fail partway is **not** considered committed.
   The failed op event and the sync summary event are marked ``partial=True``
   and carry a ``replay_prefix``: the ordered list of ops that did land and can
   be replayed.
2. Parent/child order violations are raised by the **wrapper** (this layer),
   not the store, because only the wrapper sees both the diff sequence and the
   adapter's model tree. It raises :class:`TraceOrderViolation`.
3. Ops on the same path (create -> update -> delete) are **kept one by one**,
   never collapsed into a terminal state, so a read-back comparison of two
   backends' traces is a plain op-by-op stream equality check.
"""

import functools
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional

from diffsync.exceptions import ObjectStoreException
from diffsync.store import BaseStore


class TraceOrderViolation(ObjectStoreException):
    """Raised by the tracing wrapper when a child diff is applied before any parent."""


@dataclass
class TraceEvent:
    """One structured event in a sync trace."""

    seq: int
    sync_id: Optional[str]
    backend: str
    store_class: str
    action: str  # create | update | delete | sync_summary
    model: str
    uid: str
    path: str  # "<model>:<uid>"
    status: str  # ok | failed
    partial: bool = False
    fallback: bool = False
    order_violation: bool = False
    parent_model: Optional[str] = None
    detail: str = ""
    snapshot: Dict[str, Any] = field(default_factory=dict)
    replay_prefix: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Render this event as a plain JSON-serializable dict."""
        return asdict(self)


class TraceRecorder:
    """Thread-safe in-memory sink for :class:`TraceEvent` records."""

    def __init__(self) -> None:
        """Initialize an empty recorder."""
        self._events: List[TraceEvent] = []
        self._lock = threading.Lock()
        self._seq = 0

    def next_seq(self) -> int:
        """Return the next global sequence number."""
        with self._lock:
            self._seq += 1
            return self._seq

    def record(self, event: TraceEvent) -> TraceEvent:
        """Append an event to the trace."""
        with self._lock:
            self._events.append(event)
        return event

    def events(self) -> List[Dict[str, Any]]:
        """Return all recorded events as dicts, in record order."""
        with self._lock:
            return [event.to_dict() for event in self._events]

    def get(self, seq: int) -> Optional[Dict[str, Any]]:
        """Return a single event by sequence number, or None."""
        with self._lock:
            for event in self._events:
                if event.seq == seq:
                    return event.to_dict()
        return None

    def backends(self) -> List[str]:
        """Return the distinct backend names seen in the trace, in first-seen order."""
        seen: List[str] = []
        with self._lock:
            for event in self._events:
                if event.backend not in seen:
                    seen.append(event.backend)
        return seen

    def clear(self) -> None:
        """Drop all recorded events."""
        with self._lock:
            self._events.clear()
            self._seq = 0


class _SyncSession:
    """Mutable state for one traced sync run."""

    def __init__(self, sync_id: str) -> None:
        """Initialize session state."""
        self.sync_id = sync_id
        self.ops: List[Dict[str, Any]] = []  # successful ops, i.e. the replayable prefix
        self.failed = False
        self.seen_models: set = set()


def traced(action: str) -> Callable:
    """Decorate a Store write method so each call is recorded as a :class:`TraceEvent`.

    Args:
        action: The diff action name to record: "create", "update" or "delete".
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self: "TracedStore", *args: Any, **kwargs: Any) -> Any:
            return self._run_traced(action, func, *args, **kwargs)

        return wrapper

    return decorator


class TracedStore(BaseStore):
    """BaseStore wrapper that records every mutation as a structured trace event."""

    def __init__(
        self,
        store: BaseStore,
        recorder: TraceRecorder,
        *,
        backend: Optional[str] = None,
        fallback: bool = False,
    ) -> None:
        """Wrap an existing store instance.

        Args:
            store: The concrete store (LocalStore, RedisStore, ...) to wrap.
            recorder: The recorder receiving the structured events.
            backend: Human-readable backend name used in events; defaults to the store name.
            fallback: True if the wrapped store is a degraded fallback (e.g. fakeredis);
                every event is then flagged ``fallback=True`` -- never silently.
        """
        self._store = store
        self._recorder = recorder
        self._backend = backend or getattr(store, "name", "") or store.__class__.__name__
        self._fallback = fallback
        self._session: Optional[_SyncSession] = None
        super().__init__(name=self._backend)

    # The adapter belongs to the wrapped store; forward both reads and writes.
    @property
    def adapter(self) -> Any:  # noqa: D102
        return self._store.adapter

    @adapter.setter
    def adapter(self, value: Any) -> None:
        self._store.adapter = value

    def __str__(self) -> str:
        """Render store name."""
        return f"TracedStore[{self._backend}]({self._store})"

    # ------------------------------------------------------------------
    # Sync session handling
    # ------------------------------------------------------------------

    def begin_sync(self) -> _SyncSession:
        """Open a new traced sync session. All subsequent mutations join it."""
        if self._session is not None:
            raise RuntimeError(f"A sync session ({self._session.sync_id}) is already open on {self._backend}")
        self._session = _SyncSession(sync_id=str(uuid.uuid4()))
        return self._session

    def end_sync(self) -> None:
        """Close the current session and record a summary event.

        The summary is marked ``partial`` (with a replayable prefix) if any op
        in the session failed -- a half-written sync is never a commit.
        """
        session = self._session
        if session is None:
            raise RuntimeError(f"No sync session is open on {self._backend}")
        self._session = None
        self._recorder.record(
            TraceEvent(
                seq=self._recorder.next_seq(),
                sync_id=session.sync_id,
                backend=self._backend,
                store_class=self._store.__class__.__name__,
                action="sync_summary",
                model="",
                uid="",
                path="",
                status="failed" if session.failed else "ok",
                partial=session.failed,
                fallback=self._fallback,
                detail=f"{len(session.ops)} op(s) applied" + (", sync incomplete" if session.failed else ""),
                replay_prefix=list(session.ops) if session.failed else [],
            )
        )

    @contextmanager
    def sync_session(self) -> Iterator[_SyncSession]:
        """Context manager wrapping a full sync in one traced session."""
        session = self.begin_sync()
        try:
            yield session
        finally:
            self.end_sync()

    # ------------------------------------------------------------------
    # Event recording
    # ------------------------------------------------------------------

    def _parent_models(self) -> Dict[str, str]:
        """Map each child model name to its parent model name, from the adapter's model tree."""
        mapping: Dict[str, str] = {}
        adapter = self.adapter
        if adapter is None:
            return mapping

        def walk(modelname: str) -> None:
            model_class = getattr(adapter, modelname, None)
            if model_class is None or not hasattr(model_class, "get_children_mapping"):
                return
            for child_type in model_class.get_children_mapping():
                mapping.setdefault(child_type, modelname)
                walk(child_type)

        for top in getattr(adapter, "top_level", []):
            walk(top)
        return mapping

    def _check_parent_order(self, model: str, session: _SyncSession) -> Optional[str]:
        """Return the parent model name if creating ``model`` now would violate parent-first order."""
        parent = self._parent_models().get(model)
        if parent is None:
            return None
        if parent in session.seen_models:
            return None
        if self._store.count(model=parent) > 0:
            return None
        return parent

    def _record(self, session: Optional[_SyncSession], **kwargs: Any) -> TraceEvent:
        event = TraceEvent(
            seq=self._recorder.next_seq(),
            sync_id=session.sync_id if session else None,
            backend=self._backend,
            store_class=self._store.__class__.__name__,
            fallback=self._fallback,
            **kwargs,
        )
        self._recorder.record(event)
        return event

    def _run_traced(self, action: str, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run one wrapped store mutation and record the outcome."""
        obj = kwargs.get("obj")
        if obj is not None:
            model = obj.get_type()
            uid = obj.get_unique_id()
            snapshot = {"identifiers": obj.get_identifiers(), "attrs": obj.get_attrs()}
        else:
            model = kwargs.get("modelname", "")
            uid = kwargs.get("uid", "")
            snapshot = {"identifiers": {}, "attrs": {}}
        path = f"{model}:{uid}"
        session = self._session
        parent_model = self._parent_models().get(model)

        if action == "create" and session is not None:
            violating_parent = self._check_parent_order(model, session)
            if violating_parent is not None:
                session.failed = True
                self._record(
                    session,
                    action=action,
                    model=model,
                    uid=uid,
                    path=path,
                    status="failed",
                    order_violation=True,
                    parent_model=parent_model,
                    detail=f"child '{model}' applied before any parent '{violating_parent}'",
                    snapshot=snapshot,
                    replay_prefix=list(session.ops),
                )
                raise TraceOrderViolation(
                    f"Parent/child order violation on {self._backend}: "
                    f"'{path}' created before any '{violating_parent}' in sync {session.sync_id}"
                )

        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            if session is not None:
                session.failed = True
            self._record(
                session,
                action=action,
                model=model,
                uid=uid,
                path=path,
                status="failed",
                partial=True,
                parent_model=parent_model,
                detail=f"{exc.__class__.__name__}: {exc}",
                snapshot=snapshot,
                replay_prefix=list(session.ops) if session else [],
            )
            raise

        event = self._record(
            session,
            action=action,
            model=model,
            uid=uid,
            path=path,
            status="ok",
            parent_model=parent_model,
            snapshot=snapshot,
        )
        if session is not None:
            session.seen_models.add(model)
            session.ops.append(
                {"seq": event.seq, "action": action, "model": model, "uid": uid, "path": path, "snapshot": snapshot}
            )
        return result

    # ------------------------------------------------------------------
    # Store protocol: writes are traced, reads are delegated
    # ------------------------------------------------------------------

    @traced("create")
    def add(self, *, obj: Any) -> None:
        """Add a DiffSyncModel object to the store (traced)."""
        return self._store.add(obj=obj)

    @traced("update")
    def update(self, *, obj: Any) -> None:
        """Update a DiffSyncModel object in the store (traced)."""
        return self._store.update(obj=obj)

    @traced("delete")
    def remove_item(self, modelname: str, uid: str) -> None:
        """Remove one item from the store (traced)."""
        return self._store.remove_item(modelname, uid)

    def get_all_model_names(self) -> set:
        """Get all the model names stored."""
        return self._store.get_all_model_names()

    def get(self, *, model: Any, identifier: Any) -> Any:
        """Get one object from the data store based on its unique id."""
        return self._store.get(model=model, identifier=identifier)

    def get_all(self, *, model: Any) -> list:
        """Get all objects of a given type."""
        return self._store.get_all(model=model)

    def get_by_uids(self, *, uids: list, model: Any) -> list:
        """Get multiple objects from the store by their unique IDs/Keys and type."""
        return self._store.get_by_uids(uids=uids, model=model)

    def count(self, *, model: Any = None) -> int:
        """Return the number of elements of a specific model, or all elements if unspecified."""
        return self._store.count(model=model)


def trace_store(
    store: BaseStore,
    *,
    recorder: Optional[TraceRecorder] = None,
    backend: Optional[str] = None,
    fallback: bool = False,
) -> TracedStore:
    """Wrap a BaseStore instance with the tracing layer.

    Usable as a plain factory or as a decorator-style wrapper around the Store
    protocol. Returns a :class:`TracedStore` that can be handed to an Adapter
    as its ``internal_storage_engine``.
    """
    return TracedStore(store, recorder or TraceRecorder(), backend=backend, fallback=fallback)

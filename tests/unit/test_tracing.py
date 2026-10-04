"""Tests for the persistent sync tracing layer (diffsync.tracing)."""

from typing import ClassVar, List

import pytest

from diffsync import Adapter, DiffSyncModel
from diffsync.store.local import LocalStore
from diffsync.store.redis import RedisStore
from diffsync.tracing import (
    SyncFrame,
    SyncTracer,
    TracedStore,
    TraceOrderError,
    _sync_scope,
    pop_frame,
    push_frame,
    traced_sync,
)


class Site(DiffSyncModel):
    """A site containing devices."""

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


class Source(Adapter):
    """Source adapter with one site and two devices."""

    site = Site
    device = Device
    top_level = ["site"]

    def load_with(self, roles):
        """Populate one site ``nyc`` with devices ``{name: role}``."""
        site = self.site(name="nyc")
        self.add(site)
        for name, role in roles.items():
            device = self.device(name=name, role=role)
            self.add(device)
            site.add_child(device)


class LocalDestination(Adapter):
    """Destination adapter backed by a traced LocalStore."""

    site = Site
    device = Device
    top_level = ["site"]

    def __init__(self):
        super().__init__(internal_storage_engine=TracedStore(LocalStore(), backend="local"))


class RedisDestination(Adapter):
    """Destination adapter backed by a traced RedisStore (fakeredis fallback)."""

    site = Site
    device = Device
    top_level = ["site"]

    def __init__(self):
        inner = RedisStore(store_id="tracing-test", url="redis://127.0.0.1:0")
        super().__init__(internal_storage_engine=TracedStore(inner, backend="redis"))


def _source(roles):
    source = Source(name="src")
    source.load_with(roles)
    return source


@pytest.mark.parametrize("destination_cls", [LocalDestination, RedisDestination])
def test_traced_sync_records_parent_child_order_and_readback(destination_cls):
    """A clean sync records ordered writes, a diff snapshot and an equal readback."""
    tracer = SyncTracer()
    source = _source({"r1": "spine", "r2": "leaf"})
    destination = destination_cls()

    event = traced_sync(source=source, destination=destination, sync_id="s1", tracer=tracer)

    assert event.status == "success"
    assert event.partial is False
    assert event.readback["equal"] is True
    assert [entry["model"] for entry in event.expected_order] == ["site", "device", "device"]
    assert [(w.op, w.model, w.acknowledged) for w in event.writes] == [
        ("add", "site", True),
        ("add", "device", True),
        ("add", "device", True),
    ]
    # The child device write must carry the parent site as an ancestor.
    assert event.writes[1].ancestors == [("site", "nyc")]
    assert "site" in event.diff_snapshot


@pytest.mark.parametrize("destination_cls", [LocalDestination, RedisDestination])
def test_partial_write_marks_event_and_exposes_replay_prefix(destination_cls):
    """Failing the 2nd incremental write yields partial + a replayable prefix."""
    tracer = SyncTracer()
    source = _source({"r1": "spine", "r2": "leaf"})
    destination = destination_cls()
    destination.store.fail_on_write = 2

    event = traced_sync(source=source, destination=destination, sync_id="s2", tracer=tracer)

    assert event.status == "partial"
    assert event.partial is True
    assert event.error.startswith("StoreWriteError")
    # Only the first (site) write was acknowledged -> the replayable prefix.
    assert [(w.model, w.acknowledged) for w in event.replay_prefix] == [("site", True)]
    # The failed write is retained but unacknowledged.
    failed = [w for w in event.writes if not w.acknowledged]
    assert len(failed) == 1
    assert failed[0].error is not None
    # Destination readback diverges (devices never landed).
    assert event.readback["equal"] is False


def test_update_is_persisted_and_recorded_across_rounds():
    """An update in a second round is applied through the store and traced per-write."""
    tracer = SyncTracer()
    destination = LocalDestination()

    traced_sync(source=_source({"r1": "spine"}), destination=destination, sync_id="r1", tracer=tracer)
    event2 = traced_sync(source=_source({"r1": "super-spine"}), destination=destination, sync_id="r2", tracer=tracer)

    assert event2.status == "success"
    assert event2.partial is False
    assert [(w.op, w.model, w.uid) for w in event2.writes] == [("update", "device", "r1")]
    # Per-write traces are kept, not folded: both rounds exist independently.
    assert [event.sync_id for event in tracer.events()][::-1] == ["r1", "r2"]
    assert destination.get(Device, "r1").role == "super-spine"


def test_redis_fallback_is_flagged_not_silent():
    """An unreachable Redis degrades to fakeredis and records the fallback reason."""
    destination = RedisDestination()
    assert destination.store.fallback is True
    assert destination.store.fallback_reason
    tracer = SyncTracer()
    event = traced_sync(source=_source({"r1": "spine"}), destination=destination, sync_id="fb", tracer=tracer)
    assert event.fallback is True
    assert event.fallback_reason


def test_out_of_order_child_raises_in_wrapper():
    """A child applied with its parent frame closed raises TraceOrderError."""
    tracer = SyncTracer()
    destination = LocalDestination()
    source = _source({"r1": "spine"})
    event = traced_sync(source=source, destination=destination, sync_id="ord", tracer=tracer)
    assert event.status == "success"

    # Now simulate a reordering store: within an active sync, the device's frame is open
    # while its ancestor site frame is NOT. The wrapper (not the store) must reject it.
    device = destination.device(name="r1", role="x")
    orphan = SyncFrame(model="device", uid="r1", action="create", ancestors=[("site", "nyc")])
    with _sync_scope(tracer, event.event_id):
        push_frame(orphan)
        try:
            with pytest.raises(TraceOrderError):
                destination.store.add(obj=device)
        finally:
            pop_frame()

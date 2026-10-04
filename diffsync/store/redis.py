"""RedisStore module."""

import copy
import uuid
from pickle import dumps, loads  # nosec
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Type, Union

try:
    from redis import Redis
    from redis.backoff import NoBackoff
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError
    from redis.retry import Retry
except ImportError as ierr:
    print("Redis is not installed. Have you installed diffsync with redis extra? `pip install diffsync[redis]`")
    raise ierr

from diffsync.exceptions import ObjectAlreadyExists, ObjectNotFound, ObjectStoreException
from diffsync.store import BaseStore

if TYPE_CHECKING:
    from diffsync import DiffSyncModel

REDIS_DIFFSYNC_ROOT_LABEL = "diffsync"


class RedisStore(BaseStore):
    """RedisStore class."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *args: Any,
        store_id: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 6379,
        url: Optional[str] = None,
        db: int = 0,
        **kwargs: Any,
    ):
        """Init method for RedisStore."""
        super().__init__(*args, **kwargs)

        if url and host and port:
            raise ValueError("'url' and 'host' arguments can't be specified together.")

        self.fallback = False
        self.fallback_reason: Optional[str] = None
        # Keep connection probing short so that the fakeredis fallback engages
        # promptly instead of waiting through redis-py's default retry backoff.
        connect_options = {
            "socket_connect_timeout": 1,
            "socket_timeout": 1,
            "retry": Retry(NoBackoff(), 1),
        }
        try:
            if url:
                self._store = Redis.from_url(url, db=db, **connect_options)
            elif host:
                self._store = Redis(host=host, port=port, db=db, **connect_options)
            else:
                raise RedisConnectionError("Neither 'host' nor 'url' were specified.")

            if not self._store.ping():
                raise RedisConnectionError("Redis ping returned False")
        except (RedisConnectionError, RedisTimeoutError, OSError, AttributeError) as connection_error:
            # Degrade to an in-process fakeredis instance rather than failing hard.
            # The fallback is surfaced explicitly (self.fallback) so callers never
            # silently run against a different backend than they asked for.
            try:
                import fakeredis  # pylint: disable=import-outside-toplevel

                self._store = fakeredis.FakeStrictRedis(decode_responses=False)
                self.fallback = True
                self.fallback_reason = f"{connection_error.__class__.__name__}: {connection_error}"
            except ImportError:
                raise ObjectStoreException(
                    "Redis store is unavailable and fakeredis is not installed "
                    "(install the 'fakeredis' package to enable fallback)."
                ) from connection_error

        self._store_id = store_id if store_id else str(uuid.uuid4())

        self._store_label = f"{REDIS_DIFFSYNC_ROOT_LABEL}:{self._store_id}"

    def __str__(self) -> str:
        """Render store name."""
        return f"{self.name} ({self._store_id})"

    def _get_object_from_redis_key(self, key: str) -> "DiffSyncModel":
        """Get the object from Redis key."""
        pickled_object = self._store.get(key)
        if pickled_object:
            obj_result = loads(pickled_object)  # noqa: S301
            obj_result.adapter = self.adapter
            return obj_result
        raise ObjectNotFound(f"{key} not present in Cache")

    def _identity_get(self, modelname: str, uid: str) -> Optional["DiffSyncModel"]:
        """Return the process-live instance for a uid, if one is cached."""
        cache = getattr(self, "_identity_cache", None)
        if cache is None:
            return None
        return cache.get(modelname, {}).get(uid)

    def _identity_set(self, modelname: str, uid: str, obj: "DiffSyncModel") -> None:
        """Cache a deserialized object as the process-live instance for a uid."""
        if not hasattr(self, "_identity_cache"):
            self._identity_cache: Dict[str, Dict[str, "DiffSyncModel"]] = {}
        obj.adapter = self.adapter
        self._identity_cache.setdefault(modelname, {})[uid] = obj

    def _identity_drop(self, modelname: str, uid: str) -> None:
        """Drop a cached live instance."""
        cache = getattr(self, "_identity_cache", None)
        if cache is not None:
            cache.get(modelname, {}).pop(uid, None)

    def get_all_model_names(self) -> Set[str]:
        """Get all the model names stored.

        Return:
            Set of all the model names.
        """
        # TODO: optimize it
        all_model_names = set()
        for item in self._store.scan_iter(f"{self._store_label}:*"):
            # Model Name is the third item in the Redis key
            # b'diffsync:123:device:device1' -> Model name b'device'
            model_name = item.split(b":")[2].decode()
            all_model_names.add(model_name)

        return all_model_names

    def _get_key_for_object(self, modelname: str, uid: str) -> str:
        return f"{self._store_label}:{modelname}:{uid}"

    def get(
        self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]], identifier: Union[str, Dict]
    ) -> "DiffSyncModel":
        """Get one object from the data store based on its unique id.

        Args:
            model: DiffSyncModel class or instance, or modelname string, that defines the type of the object to retrieve
            identifier: Unique ID of the object to retrieve, or dict of unique identifier keys/values

        Raises:
            ValueError: if obj is a str and identifier is a dict (can't convert dict into a uid str without a model class)
            ObjectNotFound: if the requested object is not present
        """
        object_class, modelname = self._get_object_class_and_model(model)

        uid = self._get_uid(model, object_class, identifier)

        # Return the process-live instance when available so in-flight updates mutate the
        # same object the syncer holds (mirrors LocalStore identity semantics). Otherwise
        # deserialize from Redis and register it, preventing the next sync from seeing a
        # fresh adapter-less copy and trying to re-create an object that exists in Redis.
        live = self._identity_get(modelname, uid)
        if live is not None:
            return live

        obj_result = self._get_object_from_redis_key(self._get_key_for_object(modelname, uid))
        self._identity_set(modelname, uid, obj_result)
        return obj_result

    def get_all(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]) -> List["DiffSyncModel"]:
        """Get all objects of a given type.

        Args:
            model: DiffSyncModel class or instance, or modelname string, that defines the type of the objects to retrieve

        Returns:
            List of Object
        """
        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()

        results: List["DiffSyncModel"] = []
        for key in self._store.scan_iter(f"{self._store_label}:{modelname}:*"):
            # Key layout: b"<store_label>:<modelname>:<uid>"; uid itself may contain ':'.
            prefix = f"{self._store_label}:{modelname}:".encode()
            uid = key[len(prefix) :].decode()
            results.append(self.get(model=modelname, identifier=uid))

        return results

    def get_by_uids(
        self, *, uids: List[str], model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]
    ) -> List["DiffSyncModel"]:
        """Get multiple objects from the store by their unique IDs/Keys and type.

        Args:
            uids: List of unique id / key identifying object in the database.
            model: DiffSyncModel class or instance, or modelname string, that defines the type of the objects to retrieve

        Raises:
            ObjectNotFound: if any of the requested UIDs are not found in the store
        """
        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()

        results = []
        for uid in uids:
            results.append(self.get(model=modelname, identifier=uid))

        return results

    def add(self, *, obj: "DiffSyncModel") -> None:
        """Add a DiffSyncModel object to the store.

        Args:
            obj: Object to store

        Raises:
            ObjectAlreadyExists: if a different object with the same uid is already present.
        """
        modelname = obj.get_type()
        uid = obj.get_unique_id()

        # Get existing Object
        object_key = self._get_key_for_object(modelname, uid)

        existing_obj_binary = self._store.get(object_key)
        if existing_obj_binary:
            existing_obj = loads(existing_obj_binary)  # noqa: S301
            existing_obj_dict = existing_obj.dict()

            if existing_obj_dict != obj.dict():
                raise ObjectAlreadyExists(f"Object {uid} already present", obj)

            # Return so we don't have to change anything on the existing object and underlying data
            return

        # Remove the diffsync object before sending to Redis
        obj_copy = copy.copy(obj)
        obj_copy.adapter = None

        self._store.set(object_key, dumps(obj_copy))
        self._identity_set(modelname, uid, obj)

    def update(self, *, obj: "DiffSyncModel") -> None:
        """Update a DiffSyncModel object to the store.

        Args:
            obj: Object to update
        """
        modelname = obj.get_type()
        uid = obj.get_unique_id()

        object_key = self._get_key_for_object(modelname, uid)
        existing_obj_binary = self._store.get(object_key)
        if not existing_obj_binary:
            raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")
        obj_copy = copy.copy(obj)
        obj_copy.adapter = None

        self._store.set(object_key, dumps(obj_copy))
        self._identity_set(modelname, uid, obj)

    def remove_item(self, modelname: str, uid: str) -> None:
        """Remove one item from store."""
        object_key = self._get_key_for_object(modelname, uid)

        if not self._store.exists(object_key):
            raise ObjectNotFound(f"{modelname} {uid} not present in Cache")

        self._store.delete(object_key)
        self._identity_drop(modelname, uid)

    def count(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"], None] = None) -> int:
        """Returns the number of elements of a specific model, or all elements in the store if unspecified."""
        search_pattern = f"{self._store_label}:*"
        if model is not None:
            if isinstance(model, str):
                modelname = model
            else:
                modelname = model.get_type()
            search_pattern = f"{self._store_label}:{modelname.lower()}:*"

        return len(list(self._store.scan_iter(search_pattern)))

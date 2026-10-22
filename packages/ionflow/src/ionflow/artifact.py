"""Typed files and bundles with content or declared identity (0023 and 0032)."""

import copy
import functools
import hashlib
import json
import operator
import os
import re
import shutil
import stat
import tempfile
import types
import typing
from pathlib import Path, PurePosixPath

from pydantic import ConfigDict, Field, ValidationError, create_model

from .space import utc_now
from .store import Store, payload_lock

CHUNK = 8 * 2**20
_KINDS: dict[str, type["Artifact"]] = {}
_ID = re.compile(r"([a-z][a-z0-9-]*):([0-9a-f]{64})\Z")
_DDL = {
    "artifacts": "id VARCHAR, kind VARCHAR, record VARCHAR",
    "artifact_files": "artifact_id VARCHAR, path VARCHAR, digest VARCHAR, size BIGINT",
    "artifact_links": "parent VARCHAR, child VARCHAR, relation VARCHAR, source VARCHAR",
    "artifact_provenance": "activity_id VARCHAR, record VARCHAR",
    "artifact_presence": "id VARCHAR, present BOOLEAN",
    "artifact_peers": "peer VARCHAR, id VARCHAR, present BOOLEAN",
}


def _json(value: typing.Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def metadata_token(record: dict) -> str:
    """Identity of an editable metadata revision, separate from payload identity."""
    body = {k: record.get(k) for k in ("id", "name", "metadata", "revision", "history")}
    return hashlib.sha256(_json(body).encode()).hexdigest()


def _member(name: str) -> str:
    """Validate an exact, portable, relative bundle member name.

    >>> _member("nested/coefficients.arrow")
    'nested/coefficients.arrow'
    >>> _member("../escape")
    Traceback (most recent call last):
        ...
    ValueError: unsafe artifact member: '../escape'
    """
    p = PurePosixPath(name)
    if not p.parts or p.is_absolute() or str(p) != name or ".." in p.parts or "\\" in name or ":" in name:
        raise ValueError(f"unsafe artifact member: {name!r}")
    return name


def _spec(hint: typing.Any) -> typing.Any:
    """Portable JSON metadata types, also usable by a CLI without the schema module."""
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if origin is typing.Annotated:
        return _spec(args[0])
    if isinstance(hint, type) and issubclass(hint, Artifact):
        return ["artifact", hint._kind]
    if hint is typing.Any:
        return "any"
    if origin in (types.UnionType, typing.Union):
        return ["union", *[_spec(a) for a in args]]
    if origin in (list, dict):
        if origin is dict and args[0] is not str:
            raise TypeError("artifact metadata dictionaries require string keys")
        return [origin.__name__, *[_spec(a) for a in args]]
    if hint not in (str, int, float, bool, type(None)):
        raise TypeError(f"unsupported artifact metadata type: {hint}")
    return hint.__name__


def _portable_type(spec: typing.Any) -> typing.Any:
    """Reconstruct validation types from portable metadata, without importing application code."""
    if isinstance(spec, str):
        return {"any": typing.Any, "str": str, "int": int, "float": float, "bool": bool, "NoneType": type(None)}[spec]
    kind, *args = spec
    if kind == "artifact":
        return typing.Annotated[str, Field(pattern=rf"^{args[0]}:[0-9a-f]{{64}}$")]
    if kind == "union":
        return functools.reduce(operator.or_, (_portable_type(a) for a in args))
    if kind in ("list", "dict"):
        return types.GenericAlias(list if kind == "list" else dict, tuple(_portable_type(a) for a in args))
    raise TypeError("invalid portable metadata type")


@functools.lru_cache(maxsize=256)
def _portable_model(schema_json: str) -> typing.Any:
    return typing.cast(typing.Any, create_model)(
        "ArtifactMetadata",
        __config__=ConfigDict(strict=True, extra="forbid", allow_inf_nan=False),
        **{k: (_portable_type(spec), ...) for k, spec in json.loads(schema_json).items()},
    )


def _metadata(record: dict, values: dict) -> dict:
    try:
        out = _portable_model(_json(record["schema"])).model_validate(values).model_dump(mode="json")
    except ValidationError as error:
        raise TypeError("metadata does not match the persisted artifact schema") from error
    _json(out)
    return out


def _check_record(record: dict) -> None:
    sync = record["sync"]
    if not isinstance(sync, dict) or set(sync) != {"up", "down"} or any(type(v) is not bool for v in sync.values()):
        raise ValueError("invalid artifact sync defaults")
    match = _ID.fullmatch(record["id"])
    content = record["content"]
    if match is None or match[1] != record["kind"] or match[2] != record["digest"]:
        raise ValueError("artifact identity mismatch")
    if (
        content["version"] != 1
        or content["payload"] != record["payload"]
        or record["payload"] not in ("file", "bundle")
    ):
        raise ValueError("invalid artifact content descriptor")
    names = []
    for f in content["files"]:
        names.append(_member(f["path"]))
        if not re.fullmatch(r"[0-9a-f]{64}", f["sha256"]) or type(f["size"]) is not int or f["size"] < 0:
            raise ValueError("invalid artifact file descriptor")
    if names != sorted(set(names)):
        raise ValueError("artifact members must be unique and sorted")
    if any(str(p) in names for n in names for p in PurePosixPath(n).parents if str(p) != "."):
        raise ValueError("artifact members overlap")
    if record["payload"] == "file":
        if names != ["data"]:
            raise ValueError("file artifacts require one data member")
        digest = content["files"][0]["sha256"]
    else:
        digest = hashlib.sha256(_json(content).encode()).hexdigest()
    identity = record.get("identity")
    if identity is not None:
        if not isinstance(identity, dict) or not identity or any(k not in record["schema"] for k in identity):
            raise ValueError("invalid artifact identity fields")
        if any(record["metadata"][k] != value for k, value in identity.items()):
            raise ValueError("artifact identity fields are immutable")
        digest = hashlib.sha256(_json(identity).encode()).hexdigest()
    if digest != record["digest"]:
        raise ValueError("artifact descriptor checksum mismatch")
    if not isinstance(record["name"], str) or type(record["revision"]) is not int or record["revision"] < 1:
        raise ValueError("invalid artifact metadata revision or name")
    history = record.get("history", [])
    if not isinstance(history, list) or any(
        not isinstance(t, str) or not re.fullmatch(r"[0-9a-f]{64}", t) for t in history
    ):
        raise ValueError("invalid artifact revision ancestry")
    _member(record["filename"])
    if len(PurePosixPath(record["filename"]).parts) != 1:
        raise ValueError("artifact filename must be a basename")
    _metadata(record, record["metadata"])


def content_token(record: dict) -> str:
    """Checksum of a concrete file manifest, never another artifact ID."""
    return hashlib.sha256(_json(record["content"]).encode()).hexdigest()


def _equivalent_copy(old: dict | None, record: dict) -> bool:
    return bool(
        old
        and record.get("identity")
        and old.get("identity") == record["identity"]
        and old["schema"] == record["schema"]
        and old["payload"] == record["payload"]
        and old["content"] != record["content"]
    )


def _value(value: typing.Any) -> typing.Any:
    if isinstance(value, Artifact):
        return value.id
    if isinstance(value, list):
        return [_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _value(v) for k, v in value.items()}
    return value


def _validation_type(hint: typing.Any) -> typing.Any:
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if origin is typing.Annotated:
        return typing.cast(typing.Any, typing.Annotated)[(_validation_type(args[0]), *args[1:])]
    if origin in (types.UnionType, typing.Union):
        return functools.reduce(operator.or_, (_validation_type(a) for a in args))
    if isinstance(hint, type) and issubclass(hint, Artifact):
        return typing.Annotated[str, Field(pattern=rf"^{hint._kind}:[0-9a-f]{{64}}$")]
    if origin is list:
        return types.GenericAlias(list, (_validation_type(args[0]),))
    if origin is dict:
        return types.GenericAlias(dict, (str, _validation_type(args[1])))
    return hint


def _stage(source: Path, target: Path) -> tuple[str, int]:
    """Copy and hash exactly the regular-file bytes that will be uploaded."""
    digest, size = hashlib.sha256(), 0
    if not source.is_file():
        raise ValueError(f"not a regular file: {source}")
    with source.open("rb") as src, target.open("wb") as dst:
        before = os.fstat(src.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"not a regular file: {source}")
        while chunk := src.read(CHUNK):
            digest.update(chunk)
            size += len(chunk)
            dst.write(chunk)
        after = os.fstat(src.fileno())
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError(f"artifact changed during ingestion: {source}")
    return digest.hexdigest(), size


class Artifact:
    """Payload schema, candidate, or persisted reference.

    >>> class Fasta(Artifact):
    ...     organism: str | None
    >>> Fasta.metadata_values({})
    {'organism': None}
    >>> Fasta._kind
    'fasta'
    """

    _kind: typing.ClassVar[str]
    _payload: typing.ClassVar[str]
    _fields: typing.ClassVar[dict]
    _identity: typing.ClassVar[tuple[str, ...]] = ()
    _model: typing.ClassVar[typing.Any]
    _identity_model: typing.ClassVar[typing.Any]
    _sync: typing.ClassVar[dict[str, bool]] = {"up": True, "down": True}
    _path: Path
    _name: str | None
    _members: typing.Mapping[str, str | Path] | None
    _metadata: dict
    _record: dict | None = None
    _registry: typing.Any = None

    def __init_subclass__(
        cls,
        *,
        kind: str | None = None,
        payload: str = "file",
        identity: tuple[str, ...] | None = None,
        sync_up: bool | None = None,
        sync_down: bool | None = None,
        **kw: typing.Any,
    ) -> None:
        super().__init_subclass__(**kw)
        if kind is None:
            kind = cls.__name__.replace("_", "-").lower()
        if any(value is not None and type(value) is not bool for value in (sync_up, sync_down)):
            raise TypeError("sync_up and sync_down must be booleans")
        cls._sync = {
            "up": cls._sync["up"] if sync_up is None else sync_up,
            "down": cls._sync["down"] if sync_down is None else sync_down,
        }
        if not re.fullmatch(r"[a-z][a-z0-9-]*", kind) or payload not in ("file", "bundle"):
            raise ValueError("artifact kind must be a lowercase slug; payload must be file or bundle")
        cls._kind, cls._payload = kind, payload
        cls._fields = {
            k: v
            for k, v in typing.get_type_hints(cls, include_extras=True).items()
            if not k.startswith("_") and typing.get_origin(v) is not typing.ClassVar
        }
        for hint in cls._fields.values():
            _spec(hint)
        cls._identity = cls._identity if identity is None else identity
        if (
            not isinstance(cls._identity, tuple)
            or len(set(cls._identity)) != len(cls._identity)
            or any(not isinstance(k, str) or k not in cls._fields for k in cls._identity)
        ):
            raise TypeError("identity must be a tuple of distinct artifact metadata field names")
        cls._model = typing.cast(typing.Any, create_model)(
            cls.__name__ + "Metadata",
            __config__=ConfigDict(strict=True, extra="forbid", validate_default=True, allow_inf_nan=False),
            **{k: (_validation_type(hint), getattr(cls, k, None)) for k, hint in cls._fields.items()},
        )
        cls._identity_model = typing.cast(typing.Any, create_model)(
            cls.__name__ + "Identity",
            __config__=cls._model.model_config,
            **{k: (f.annotation, f) for k in cls._identity for f in [cls._model.model_fields[k]]},
        )
        if collisions := set(cls._fields) & (set(dir(Artifact)) | {"run_id", "item_key", "local", "s3"}):
            raise ValueError(f"reserved artifact metadata fields: {sorted(collisions)}")
        _KINDS[kind] = cls

    @classmethod
    def metadata_values(cls, given: dict) -> dict:
        """Resolve defaults and validate JSON metadata at the publication boundary."""
        if unknown := set(given) - set(cls._fields):
            raise ValueError(f"unknown {cls._kind} metadata: {sorted(unknown)}")
        try:
            out = cls._model.model_validate(_value(given)).model_dump(mode="json")
        except ValidationError as error:
            raise TypeError(str(error)) from error
        _json(out)
        return out

    @classmethod
    def identity(cls, **values: typing.Any) -> str:
        """Calculate a declared identity without constructing or reading any payload."""
        if not cls._identity:
            raise ValueError(f"{cls.__name__} uses content identity; its files must exist first")
        if set(values) != set(cls._identity):
            raise ValueError(f"identity requires exactly {list(cls._identity)}")
        normalized = cls._identity_model.model_validate(_value(values)).model_dump(mode="json")
        return f"{cls._kind}:" + hashlib.sha256(_json(normalized).encode()).hexdigest()

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        *,
        name: str | None = None,
        members: typing.Mapping[str, str | Path] | None = None,
        **metadata: typing.Any,
    ) -> typing.Self:
        """Describe a candidate; publication happens through Artifacts.put or the runner."""
        obj = cls.__new__(cls)
        obj._path, obj._name, obj._members = Path(path), name, members
        obj._metadata = cls.metadata_values(metadata)
        obj._record, obj._registry = None, None
        return obj

    @property
    def id(self) -> str:
        if self._record is None:
            raise ValueError("artifact has not been published")
        return self._record["id"]

    @property
    def name(self) -> str:
        return self._record["name"] if self._record else self._name or self._path.name

    @property
    def metadata(self) -> dict:
        return copy.deepcopy(self._metadata)

    @property
    def revision(self) -> int:
        if self._record is None:
            raise ValueError("artifact has not been published")
        return self._record["revision"]

    @property
    def uri(self) -> str:
        if self._record is not None and self._record["payload"] == "file":
            return self._registry.store.uri(
                f"{self._registry.root}/_blobs/sha256/{self._record['content']['files'][0]['sha256']}"
            )
        return self._registry.store.uri(self._registry.directory(self.id) + "/content.json")

    def local_path(self) -> Path:
        """Resolve a complete, verified local file or bundle; candidates use their source."""
        return self._registry.materialize(self) if self._registry else self._path

    def snapshot(self) -> dict:
        """Store-independent binding, including pinned metadata."""
        if self._record is None:
            raise ValueError("artifact has not been published")
        return copy.deepcopy(self._record)


class ArtifactInputs:
    """Named run inputs: artifact schemas or ordered lists of artifact schemas."""

    @classmethod
    def declarations(cls) -> dict:
        hints = typing.get_type_hints(cls)
        for name, hint in hints.items():
            schema = typing.get_args(hint)[0] if typing.get_origin(hint) is list else hint
            if not isinstance(schema, type) or not issubclass(schema, Artifact):
                raise TypeError(f"artifact input {name!r} must be an Artifact or list[Artifact]")
        return hints


class MetadataConflict(ValueError):
    """An edit or sync would overwrite a different metadata revision."""


class MissingPayload(RuntimeError):
    """Known metadata needs an explicit payload download before local use."""


class Artifacts:
    """One store's shared inventory; catalogs own metadata, descriptors own content."""

    def __init__(self, config: typing.Any) -> None:
        self.store = config if isinstance(config, Store) else Store.of(config)
        self.root = f"{self.store.prefix}/artifacts"
        self.lake = self.store.lake(self.root, data_subdir="_lake")

    def directory(self, artifact_id: str) -> str:
        match = _ID.fullmatch(artifact_id)
        if match is None:
            raise ValueError(f"invalid artifact ID: {artifact_id!r}")
        return f"{self.root}/{match[1]}/{match[2]}"

    def _genesis(self, con: typing.Any) -> None:
        for name, ddl in _DDL.items():
            con.execute(f"CREATE TABLE IF NOT EXISTS lake.{name} ({ddl})")

    def records(self, kind: str | None = None) -> list[dict]:
        if self.lake.register.read()[0] is None:
            if self._revision_keys():
                raise RuntimeError("artifact registry is missing; run `ionflow artifacts recover`")
            return []
        with self.lake.read() as con:
            rows = con.execute(
                "SELECT record FROM lake.artifacts WHERE (? IS NULL OR kind = ?) ORDER BY id", [kind, kind]
            )
            return [json.loads(r[0]) for r in rows.fetchall()]

    def get(self, artifact_id: str, schema: type[Artifact] | None = None, snapshot: dict | None = None) -> Artifact:
        self.directory(artifact_id)
        if snapshot is None:
            if self.lake.register.read()[0] is None:
                raise KeyError(artifact_id)
            with self.lake.read() as con:
                row = con.execute("SELECT record FROM lake.artifacts WHERE id = ?", [artifact_id]).fetchone()
            if row is None:
                raise KeyError(artifact_id)
            snapshot = json.loads(row[0])
        if snapshot["id"] != artifact_id:
            raise ValueError("artifact snapshot ID mismatch")
        _check_record(snapshot)
        resolved = schema or _KINDS.get(snapshot["kind"], Artifact)
        if resolved is not Artifact and (
            resolved._kind != snapshot["kind"] or resolved._payload != snapshot["payload"]
        ):
            raise TypeError(f"{artifact_id} is not a {resolved._kind}")
        if resolved is not Artifact and {k: _spec(v) for k, v in resolved._fields.items()} != snapshot["schema"]:
            raise TypeError(f"{artifact_id}: metadata schema differs from {resolved.__name__}")
        if resolved is not Artifact and set(resolved._identity) != set(snapshot.get("identity") or {}):
            raise TypeError(f"{artifact_id}: declared identity differs from {resolved.__name__}")
        if resolved is not Artifact:
            if resolved.metadata_values(snapshot["metadata"]) != snapshot["metadata"]:
                raise TypeError(f"{artifact_id}: metadata is not normalized")
            if resolved._identity and resolved.identity(**snapshot["identity"]) != artifact_id:
                raise ValueError(f"{artifact_id}: identity is not normalized")
        obj = resolved.__new__(resolved)
        obj._record, obj._registry = copy.deepcopy(snapshot), self
        obj._metadata = copy.deepcopy(snapshot["metadata"])
        return obj

    def _export(self, record: dict) -> None:
        root = self.directory(record["id"])
        body = _json(record).encode()
        self.store.put(f"{root}/metadata/{record['revision']}.json", body)
        self.store.put(f"{root}/_meta.json", body)

    def _export_payload(self, record: dict) -> None:
        if record.get("identity") is not None:
            self.store.put(
                f"{self.directory(record['id'])}/payloads/{content_token(record)}.json", _json(record).encode()
            )

    def payload_records(self, artifact_id: str, *, snapshot: dict | None = None) -> list[dict]:
        """Retained concrete manifests for an artifact, including forced local builds."""
        preferred = self.get(artifact_id).snapshot() if snapshot is None else snapshot
        records: dict[str, dict] = {content_token(preferred): preferred}
        for key in self.store.list(self.directory(artifact_id) + "/payloads/"):
            record = json.loads(self.store.get(key))
            _check_record(record)
            if (
                record["id"] != artifact_id
                or key != f"{self.directory(artifact_id)}/payloads/{content_token(record)}.json"
            ):
                raise ValueError("invalid artifact payload export")
            records.setdefault(content_token(record), record)
        return list(records.values())

    def _revision_keys(self) -> list[str]:
        keys = []
        for key in self.store.list(self.root + "/"):
            parts = key.removeprefix(self.root + "/").split("/")
            if len(parts) == 4 and parts[2] == "metadata" and parts[3].endswith(".json"):
                keys.append(key)
        return keys

    def _insert(self, con: typing.Any, record: dict) -> None:
        same_kind = con.execute("SELECT record FROM lake.artifacts WHERE kind = ? LIMIT 1", [record["kind"]]).fetchone()
        if same_kind:
            declared = json.loads(same_kind[0])
            if (declared["schema"], declared["payload"], set(declared.get("identity") or {})) != (
                record["schema"],
                record["payload"],
                set(record.get("identity") or {}),
            ):
                raise TypeError(f"artifact type {record['kind']!r} already has a different schema or payload kind")
        con.execute("INSERT INTO lake.artifacts VALUES (?, ?, ?)", [record["id"], record["kind"], _json(record)])
        self._insert_files(con, record)

    @staticmethod
    def _insert_files(con: typing.Any, record: dict) -> None:
        for f in record["content"]["files"]:
            values = [record["id"], f["path"], f["sha256"], f["size"]]
            con.execute(
                "INSERT INTO lake.artifact_files SELECT ?, ?, ?, ? WHERE NOT EXISTS ("
                "SELECT 1 FROM lake.artifact_files WHERE artifact_id = ? AND path = ? AND digest = ?)",
                values + values[:3],
            )

    def put[A: Artifact](self, candidate: A) -> A:
        """Publish staged content before making an artifact visible in the registry."""
        with payload_lock(self.store):
            return self._put(candidate)

    def _put[A: Artifact](self, candidate: A) -> A:
        if candidate._record is not None:
            if candidate._registry is self:
                return candidate
            self.get(candidate.id, type(candidate))
            return typing.cast(A, self.get(candidate.id, type(candidate), snapshot=candidate.snapshot()))
        path, payload = candidate._path, candidate._payload
        if payload == "file":
            if candidate._members is not None:
                raise ValueError("file artifacts do not accept bundle members")
            sources = {"data": path}
        elif candidate._members is not None:
            sources = {k: Path(v) for k, v in candidate._members.items()}
        else:
            if not path.is_dir() or path.is_symlink():
                raise ValueError(f"not a bundle directory: {path}")
            sources = {}
            for p in sorted(path.rglob("*")):
                if p.is_symlink() and p.is_dir():
                    raise ValueError(f"directory symlinks are unsupported: {p}")
                if not p.is_dir():
                    sources[p.relative_to(path).as_posix()] = p
        for name in sources:
            _member(name)
            if any(str(parent) in sources for parent in PurePosixPath(name).parents if str(parent) != "."):
                raise ValueError(f"artifact member overlaps a directory: {name}")
        files = []
        with tempfile.TemporaryDirectory(prefix="ionflow-artifact-") as tmp:
            staged = []
            for i, (name, source) in enumerate(sorted(sources.items())):
                dest = Path(tmp) / str(i)
                digest, size = _stage(source, dest)
                files.append({"path": name, "sha256": digest, "size": size})
                staged.append((digest, dest))
            content: dict = {"version": 1, "payload": payload, "files": files}
            digest = files[0]["sha256"] if payload == "file" else hashlib.sha256(_json(content).encode()).hexdigest()
            artifact_id = f"{candidate._kind}:{digest}"
            for file_digest, dest in staged:
                key = f"{self.root}/_blobs/sha256/{file_digest}"
                if not self.store.exists(key):
                    self.store.upload(key, str(dest))
            identity = {k: candidate.metadata[k] for k in candidate._identity} or None
            if identity is not None:
                digest = candidate.identity(**identity).split(":")[1]
                artifact_id = f"{candidate._kind}:{digest}"
        record = {
            "id": artifact_id,
            "kind": candidate._kind,
            "payload": payload,
            "digest": digest,
            "name": candidate.name,
            "filename": _member(path.name),
            "metadata": candidate.metadata,
            "revision": 1,
            "created": utc_now().isoformat(),
            "content": content,
            "schema": {k: _spec(v) for k, v in candidate._fields.items()},
            "sync": dict(candidate._sync),
            "history": [],
            "identity": identity,
        }
        published = record

        def commit(con: typing.Any) -> None:
            nonlocal published
            self._genesis(con)
            row = con.execute("SELECT record FROM lake.artifacts WHERE id = ?", [artifact_id]).fetchone()
            if row is not None:
                old = json.loads(row[0])
                if old["schema"] != record["schema"]:
                    raise TypeError(f"artifact type {record['kind']!r} already has a different schema")
                if old.get("identity") != identity or old["payload"] != payload:
                    raise MetadataConflict(f"{artifact_id}: existing identity or payload declaration differs")
                if identity is None and old["metadata"] != record["metadata"]:
                    raise MetadataConflict(f"{artifact_id}: existing metadata differs; use update explicitly")
                published = old
                if (
                    identity is not None
                    and self.store.local_dir
                    and (old["content"] != record["content"] or old["metadata"] != record["metadata"])
                ):
                    published = old | {
                        "content": content,
                        "metadata": record["metadata"],
                        "filename": record["filename"],
                        "revision": old["revision"] + 1,
                        "history": [*old.get("history", []), metadata_token(old)],
                    }
                    con.execute("UPDATE lake.artifacts SET record = ? WHERE id = ?", [_json(published), artifact_id])
                    self._insert_files(con, published)
                return
            self._insert(con, record)

        self.lake.commit(commit)
        self.store.put(self.directory(artifact_id) + "/content.json", _json(published["content"]).encode())
        self._export_payload(published)
        self.mark_present(artifact_id)
        ref = self.get(artifact_id, type(candidate), snapshot=published)
        self._export(ref.snapshot())
        from .storage import request_refresh

        request_refresh(self.store.cfg)
        return typing.cast(A, ref)

    def update(
        self, artifact_id: str, *, revision: int, name: str | None = None, metadata: dict | None = None
    ) -> Artifact:
        """Edit metadata using the revision that the caller actually read."""
        old = self.get(artifact_id)
        values = _metadata(old.snapshot(), old.metadata if metadata is None else metadata)
        if type(old) is not Artifact:
            values = type(old).metadata_values(values)
        if any(values[k] != v for k, v in (old.snapshot().get("identity") or {}).items()):
            raise ValueError("artifact identity fields are immutable")
        if name is not None and not isinstance(name, str):
            raise TypeError("artifact name must be a string")
        record = old.snapshot() | {
            "name": old.name if name is None else name,
            "metadata": values,
            "revision": revision + 1,
            "history": [*old.snapshot().get("history", []), metadata_token(old.snapshot())],
        }

        def commit(con: typing.Any) -> None:
            row = con.execute("SELECT record FROM lake.artifacts WHERE id = ?", [artifact_id]).fetchone()
            if row is None or json.loads(row[0])["revision"] != revision:
                raise MetadataConflict(f"{artifact_id}: metadata revision changed")
            con.execute("UPDATE lake.artifacts SET record = ? WHERE id = ?", [_json(record), artifact_id])

        self.lake.commit(commit)
        self._export(record)
        from .storage import request_refresh

        request_refresh(self.store.cfg)
        return self.get(artifact_id)

    def availability(self, peer: str | None = None, *, artifact_id: str | None = None) -> dict[str, dict[str, bool]]:
        """Payload presence here and at the last observed peer; metadata alone is absent."""
        if self.lake.register.read()[0] is None:
            return {}
        with self.lake.read() as con:
            return {
                aid: {"local": bool(present), "s3": bool(remote)}
                for aid, present, remote in con.execute(
                    "SELECT a.id, p.present, r.present FROM lake.artifacts a "
                    "LEFT JOIN lake.artifact_presence p ON p.id = a.id "
                    "LEFT JOIN lake.artifact_peers r ON r.id = a.id AND r.peer = ? "
                    "WHERE (? IS NULL OR a.id = ?)",
                    [peer, artifact_id, artifact_id],
                ).fetchall()
            }

    def mark_present(self, artifact_id: str) -> None:
        """Record payload presence only after all members have been verified."""

        def commit(con: typing.Any) -> None:
            self._genesis(con)
            con.execute("DELETE FROM lake.artifact_presence WHERE id = ?", [artifact_id])
            con.execute("INSERT INTO lake.artifact_presence VALUES (?, true)", [artifact_id])

        with payload_lock(self.store):
            record = self.get(artifact_id, Artifact).snapshot()
            for member in record["content"]["files"]:
                info = self.store.stat(f"{self.root}/_blobs/sha256/{member['sha256']}")
                if info is None or info["size"] != member["size"]:
                    raise MissingPayload(f"{artifact_id}: payload unavailable; review a new transfer plan")
            self.lake.commit(commit)

    def merge_metadata(
        self, records: list[dict], peer: str | None = None, presence: dict | None = None, *, received: bool = False
    ) -> list[str]:
        """Import descriptors without blobs; return divergent metadata IDs."""
        conflicts = []
        for record in records:
            _check_record(record)
            if received and not self.has_payload(self.get(record["id"], snapshot=record)):
                raise MissingPayload(f"{record['id']}: receiving files are incomplete; review a new transfer plan")
        existing = {r["id"]: r for r in self.records()}
        observed = self.availability(peer)
        records = [
            r
            for r in records
            if received
            or (existing.get(r["id"]) != r and not _equivalent_copy(existing.get(r["id"]), r))
            or (
                peer is not None
                and observed.get(r["id"], {}).get("s3", False) != bool((presence or {}).get(r["id"], {}).get("local"))
            )
        ]
        if not records:
            return []

        def commit(con: typing.Any) -> None:
            conflicts.clear()
            self._genesis(con)
            for record in records:
                row = con.execute("SELECT record FROM lake.artifacts WHERE id = ?", [record["id"]]).fetchone()
                old = json.loads(row[0]) if row else None
                if old is None:
                    self._insert(con, record)
                elif (old["schema"], old["payload"], old.get("identity")) != (
                    record["schema"],
                    record["payload"],
                    record.get("identity"),
                ) or (old["content"] != record["content"] and record.get("identity") is None):
                    raise TypeError(f"{record['id']}: artifact schema or content differs")
                elif old["content"] != record["content"]:
                    pass  # Equivalent builds retain their own payload metadata.
                elif metadata_token(old) in record.get("history", []):
                    updated = record | {"content": old["content"], "filename": old["filename"]}
                    con.execute("UPDATE lake.artifacts SET record = ? WHERE id = ?", [_json(updated), record["id"]])
                elif (old["name"], old["metadata"]) != (record["name"], record["metadata"]) and (
                    metadata_token(record) not in old.get("history", [])
                ):
                    conflicts.append(record["id"])
                if received and record["id"] not in conflicts:
                    present = con.execute(
                        "SELECT present FROM lake.artifact_presence WHERE id = ?", [record["id"]]
                    ).fetchone()
                    if not present or not present[0]:
                        preferred = json.loads(
                            con.execute("SELECT record FROM lake.artifacts WHERE id = ?", [record["id"]]).fetchone()[0]
                        )
                        if preferred["content"] != record["content"]:
                            preferred = record
                        con.execute(
                            "UPDATE lake.artifacts SET record = ? WHERE id = ?", [_json(preferred), record["id"]]
                        )
                    con.execute("DELETE FROM lake.artifact_presence WHERE id = ?", [record["id"]])
                    con.execute("INSERT INTO lake.artifact_presence VALUES (?, true)", [record["id"]])
                self._insert_files(con, record)
                if peer is not None:
                    con.execute("DELETE FROM lake.artifact_peers WHERE peer = ? AND id = ?", [peer, record["id"]])
                    con.execute(
                        "INSERT INTO lake.artifact_peers VALUES (?, ?, ?)",
                        [peer, record["id"], bool((presence or {}).get(record["id"], {}).get("local"))],
                    )

        if records:
            self.lake.commit(commit)
        for record in records:
            if record["id"] not in conflicts:
                self._export_payload(record)
                preferred = self.get(record["id"]).snapshot()
                self.store.put(self.directory(record["id"]) + "/content.json", _json(preferred["content"]).encode())
                self._export(preferred)
        return conflicts

    def _cache_root(self) -> Path:
        namespace = hashlib.sha256(self.store.uri(self.root).encode()).hexdigest()
        return Path(tempfile.gettempdir()) / "ionflow-artifacts" / namespace

    def _file_alias(self, record: dict, payload: Path) -> Path:
        """Names are outside content identity; aliases share verified cache bytes."""
        directory = self._cache_root() / "_files" / (record["id"].replace(":", "-") + "-" + content_token(record))
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / record["filename"]
        try:
            os.link(payload, path)
        except FileExistsError:
            if not path.samefile(payload):
                raise ValueError(f"artifact cache alias conflicts: {path}") from None
        return path

    def materialize(self, ref: Artifact) -> Path:
        """Download, verify, and atomically expose a complete shared cache entry."""
        record = ref.snapshot()
        if not self.availability().get(ref.id, {}).get("local"):
            raise MissingPayload(f"{ref.id}: payload unavailable here; download this artifact first")
        if any(not self.store.exists(f"{self.root}/_blobs/sha256/{f['sha256']}") for f in record["content"]["files"]):
            raise MissingPayload(f"{ref.id}: payload unavailable here; download this artifact first")
        cache = self._cache_root()
        cache.mkdir(parents=True, exist_ok=True)
        dest = cache / (record["id"].replace(":", "-") + "-" + content_token(record))
        if dest.is_dir():
            return self._file_alias(record, dest / "data") if record["payload"] == "file" else dest
        with tempfile.TemporaryDirectory(prefix=".partial-", dir=cache) as tmp:
            staging = Path(tmp) / "content"
            staging.mkdir()
            for member in record["content"]["files"]:
                p = staging / _member(member["path"])
                p.parent.mkdir(parents=True, exist_ok=True)
                self.store.download(f"{self.root}/_blobs/sha256/{member['sha256']}", str(p))
                with p.open("rb") as f:
                    digest = hashlib.file_digest(f, "sha256").hexdigest()
                if digest != member["sha256"] or p.stat().st_size != member["size"]:
                    raise ValueError(f"artifact checksum mismatch: {ref.id}/{member['path']}")
                p.chmod(0o444)
            for directory in sorted(staging.rglob("*"), reverse=True):
                if directory.is_dir():
                    directory.chmod(0o555)
            try:
                staging.rename(dest)
                dest.chmod(0o555)
            except OSError:
                if not dest.is_dir():
                    raise
        return self._file_alias(record, dest / "data") if record["payload"] == "file" else dest

    def has_payload(self, ref: Artifact) -> bool:
        """Check the pinned files, rather than a different build of the same artifact."""
        return all(
            (info := self.store.stat(f"{self.root}/_blobs/sha256/{m['sha256']}")) is not None
            and info["size"] == m["size"]
            for m in ref.snapshot()["content"]["files"]
        )

    def evict(self, artifact_id: str) -> bool:
        """Remove only this store's disposable local cache entry, never canonical data."""
        self.directory(artifact_id)
        name = artifact_id.replace(":", "-")
        paths = [*self._cache_root().glob(name + "-*"), *(self._cache_root() / "_files").glob(name + "-*")]
        evicted = False
        for path in paths:
            if path.exists():
                for directory in [path, *path.rglob("*")]:
                    if directory.is_dir() and not directory.is_symlink():
                        directory.chmod(0o755)
                shutil.rmtree(path)
                evicted = True
        return evicted

    def bind(self, declarations: dict, given: dict | None) -> dict:
        """Validate named bindings once, before a run starts; retain ordered snapshots."""
        given = given or {}
        if set(given) != set(declarations):
            raise ValueError(f"artifact inputs must be {sorted(declarations)}, got {sorted(given)}")
        out = {}
        for name, hint in declarations.items():
            many = typing.get_origin(hint) is list
            schema = typing.get_args(hint)[0] if many else hint
            values = given[name]
            if many and not isinstance(values, list):
                raise TypeError(f"artifact input {name!r} must be a list")
            refs = [
                self.get(v.id, schema, snapshot=v.snapshot()) if isinstance(v, Artifact) else self.get(v, schema)
                for v in (values if many else [values])
            ]
            out[name] = [r.snapshot() for r in refs] if many else refs[0].snapshot()
        return out

    def link(self, parent: str, child: str, *, relation: str = "derived_from", source: str = "import") -> None:
        """Assert an evidenced relationship without inventing a producing run."""
        self.get(parent)
        self.get(child)
        values = [parent, child, relation, source]

        def commit(con: typing.Any) -> None:
            row = con.execute(
                "SELECT 1 FROM lake.artifact_links WHERE parent=? AND child=? AND relation=? AND source=?", values
            )
            if row.fetchone() is None:
                con.execute("INSERT INTO lake.artifact_links VALUES (?, ?, ?, ?)", values)

        self.lake.commit(commit)
        key = hashlib.sha256(_json(values).encode()).hexdigest()
        self.store.put(f"{self.root}/_links/{key}.json", _json(values).encode())

    def links(self) -> list[list]:
        if self.lake.register.read()[0] is None:
            return []
        with self.lake.read() as con:
            return [list(r) for r in con.sql("SELECT * FROM lake.artifact_links").fetchall()]

    def activities(self) -> list[dict]:
        """Read execution lineage across flow catalogs without importing user code."""
        records = {self._activity_id(r): r for r in self.provenance()}
        for flow_id in self.store.flow_ids():
            with self.store.space(flow_id).lake().read() as con:
                for run_id, body in con.sql("SELECT run_id, record FROM lake.artifact_activity").fetchall():
                    record = {"flow_id": flow_id, "run_id": run_id, **json.loads(body)}
                    records[self._activity_id(record)] = record
        return list(records.values())

    def provenance(self) -> list[dict]:
        """Portable producer evidence, including artifacts transferred without runs."""
        if self.lake.register.read()[0] is None:
            return []
        with self.lake.read() as con:
            return [json.loads(v) for (v,) in con.sql("SELECT record FROM lake.artifact_provenance").fetchall()]

    @staticmethod
    def _activity_id(record: dict) -> str:
        return _json([record["flow_id"], record["run_id"], record["step"], record["key"]])

    def _import_activity(self, record: dict) -> None:
        """Carry evidenced producer provenance when the original flow is not transferred."""
        key = self._activity_id(record)
        merged = record

        def commit(con: typing.Any) -> None:
            nonlocal merged
            self._genesis(con)
            row = con.execute("SELECT record FROM lake.artifact_provenance WHERE activity_id = ?", [key]).fetchone()
            old = json.loads(row[0]) if row else record
            merged = (
                record
                | {
                    field: list({_json(v): v for v in [*old[field], *record[field]]}.values())
                    for field in ("uses", "outputs")
                }
                | {"ancestors": sorted(set(old["ancestors"]) | set(record["ancestors"]))}
            )
            con.execute("DELETE FROM lake.artifact_provenance WHERE activity_id = ?", [key])
            con.execute("INSERT INTO lake.artifact_provenance VALUES (?, ?)", [key, _json(merged)])

        self.lake.commit(commit)
        slug = hashlib.sha256(key.encode()).hexdigest()
        body = _json(merged).encode()
        revision = hashlib.sha256(body).hexdigest()
        self.store.put(f"{self.root}/_provenance/{slug}/{revision}.json", body)

    def consumers(self, artifact_id: str) -> list[dict]:
        """Step-scoped declared consumers, including failed attempts."""
        self.directory(artifact_id)
        return [r for r in self.activities() if any(u["artifact"]["id"] == artifact_id for u in r["uses"])]

    def producers(self, artifact_id: str) -> list[dict]:
        """Distinct original producers, retained even when only a reused run was synced."""
        self.directory(artifact_id)
        records = {}
        for activity in self.activities():
            for output in activity["outputs"]:
                if output["artifact"]["id"] == artifact_id and output["event"] != "referenced":
                    origin = output["origin"]
                    records[_json(origin)] = origin
        return list(records.values())

    def input_bindings(self) -> list[dict]:
        """Selected run inputs, including runs that never reached a consuming step."""
        from .query import artifact_binding_rows

        records = []
        for flow_id in self.store.flow_ids():
            with self.store.space(flow_id).lake().read() as con:
                records.extend(
                    artifact_binding_rows(flow_id, con.sql("SELECT run_id, artifacts FROM lake.runs").fetchall())
                )
        return records

    def parents(self, artifact_id: str) -> list[str]:
        """Direct parents from execution activities and explicitly imported assertions."""
        self.directory(artifact_id)
        parents = {p for p, c, _, _ in self.links() if c == artifact_id}
        for activity in self.activities():
            for output in activity["outputs"]:
                if output["artifact"]["id"] == artifact_id:
                    parents.update(output["parents"])
        return sorted(parents)

    def duck(self) -> typing.Any:
        """Inventory and cross-flow lineage SQL, including a never-run store."""
        from .query import artifact_views

        con = self.store.connect()
        artifact_views(con, self, self.activities(), self.input_bindings())
        return con

    def recover(self) -> int:
        """Restore inventory and edits from immutable revision exports; no payload deletion."""
        with payload_lock(self.store):
            return self._recover()

    def _recover(self) -> int:
        records = {}
        for key in self._revision_keys():
            record = json.loads(self.store.get(key))
            _check_record(record)
            if key != f"{self.directory(record['id'])}/metadata/{record['revision']}.json":
                raise ValueError("artifact revision export has an inconsistent path")
            old = records.get(record["id"])
            if old is None or record["revision"] > old["revision"]:
                records[record["id"]] = record
        for record in records.values():
            content = json.loads(self.store.get(self.directory(record["id"]) + "/content.json") or "null")
            if content != record["content"] and record.get("identity") is None:
                raise ValueError(f"missing or conflicting descriptor: {record['id']}")
        payloads = [
            payload for record in records.values() for payload in self.payload_records(record["id"], snapshot=record)
        ]
        links = [json.loads(self.store.get(k)) for k in self.store.list(self.root + "/_links/")]

        def commit(con: typing.Any) -> None:
            self._genesis(con)
            for aid, body in con.sql("SELECT id, record FROM lake.artifacts").fetchall():
                current = json.loads(body)
                if aid not in records or current["revision"] > records[aid]["revision"]:
                    raise MetadataConflict(f"{aid}: recovery exports are behind the live registry")
            for table in _DDL:
                con.execute(f"DELETE FROM lake.{table}")
            for record in records.values():
                self._insert(con, record)
                present = all(
                    self.store.exists(f"{self.root}/_blobs/sha256/{m['sha256']}") for m in record["content"]["files"]
                )
                con.execute("INSERT INTO lake.artifact_presence VALUES (?, ?)", [record["id"], present])
            for payload in payloads:
                self._insert_files(con, payload)
            for values in links:
                if not all(v in records for v in values[:2]):
                    raise ValueError("artifact relationship references missing content")
                con.execute("INSERT INTO lake.artifact_links VALUES (?, ?, ?, ?)", values)

        self.lake.commit(commit)
        for key in self.store.list(self.root + "/_provenance/"):
            self._import_activity(json.loads(self.store.get(key)))
        return len(records)

    def sync_to(
        self,
        other: "Artifacts",
        ids: list[str] | None = None,
        *,
        include_artifacts: list[str] | None = None,
        direction: str | None = None,
    ) -> int:
        """Sync closure metadata and the payloads selected by directional defaults."""
        from .storage import execute_transfer, plan_transfer

        with payload_lock(self.store, other.store):
            plan = plan_transfer(
                self.store,
                other.store,
                artifact_ids=ids if ids is not None else [r["id"] for r in self.records()],
                include_artifacts=include_artifacts,
                direction=direction,
            )
            execute_transfer(plan)
            return len(plan.artifact_records)

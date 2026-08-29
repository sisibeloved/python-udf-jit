from __future__ import annotations

import importlib
import json
import os
import secrets
import threading
import time
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from python_udf_jit.runtime.guards import (
    DescriptorGuardError,
    DescriptorRejectCode,
    guard_arrow_batch_descriptor,
)
from python_udf_jit.runtime.layout import (
    ARROW_BATCH_ABI_VERSION,
    ArrowBatchDescriptor,
    ProcessIdentity,
)


COLUMNAR_WRAPPER_SERIALIZATION_VERSION = 1
_PROCESS_PID = 0
_PROCESS_GENERATION = ""
_NEXT_BORROW_ID = 0
_PROCESS_IDENTITY_LOCK = threading.Lock()
_NATIVE_BATCH_BUILD_LOCK = threading.Lock()


def _reset_process_locks_after_fork(_ignored: Any = None) -> None:
    """Drop inherited lock ownership before any child-process JIT work."""

    global _PROCESS_IDENTITY_LOCK, _NATIVE_BATCH_BUILD_LOCK
    _PROCESS_IDENTITY_LOCK = threading.Lock()
    _NATIVE_BATCH_BUILD_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_process_locks_after_fork)


@dataclass
class ColumnarRuntimeCounters:
    """Value-free, process-local counters; mutation relies on the CPython GIL."""

    batches: int = 0
    rows: int = 0
    arrow_borrows: int = 0
    batch_boundary_hits: int = 0
    materializations: int = 0
    vector_batches: int = 0
    vector_unavailable_batches: int = 0
    native_jit_rows: int = 0
    native_batch_batches: int = 0
    native_batch_rows: int = 0
    native_batch_unavailable_batches: int = 0
    dictionary_batches: int = 0
    dictionary_rows: int = 0
    dictionary_unique_values: int = 0
    dictionary_python_unique_rows: int = 0
    dictionary_python_output_rows: int = 0
    dictionary_unavailable_batches: int = 0
    dictionary_disabled_batches: int = 0
    full_python_materializations: int = 0
    full_python_materialized_rows: int = 0
    dictionary_encode_ns: int = 0
    dictionary_unique_materialize_ns: int = 0
    dictionary_target_ns: int = 0
    dictionary_reconstruct_ns: int = 0
    python_scalar_fallback_rows: int = 0
    precommit_failures: int = 0
    published_batches: int = 0
    postcommit_replays: int = 0
    native_expression_translation_plans: int = 0
    native_expression_whitespace_plans: int = 0
    native_expression_regex_plans: int = 0
    native_expression_language_id_plans: int = 0
    native_expression_length_plans: int = 0
    native_expression_guard_checks: int = 0
    native_expression_guard_misses: int = 0
    native_expression_fallback_rebuilds: int = 0
    native_expression_null_guard_batches: int = 0
    native_expression_null_guard_rows: int = 0
    native_expression_null_guard_misses: int = 0


_COUNTERS = ColumnarRuntimeCounters()


def columnar_boundary_proven(func: Any, original_callable: Any) -> bool:
    """Name-free proof that a one-string scalar call is safe to batch-lift.

    Admission requires both safe semantics and an executable batch backend.
    Typed capture alone is intentionally insufficient: it proves effects and
    guards, but it does not prove that CinderX can compile the target bytecode
    safely or profitably.
    """

    try:
        from python_udf_jit.compiler.invariant_calls import analyze_value_cache
        from python_udf_jit.integration.daft_ray.schema import (
            canonicalize_logical_type,
        )
        from python_udf_jit.integration.daft_ray.typed_loop_worker import (
            resolve_typed_loop_callable,
        )

        if canonicalize_logical_type(func.return_dtype) != "string":
            return False
        resolved = resolve_typed_loop_callable(original_callable)
        # Value-cache analysis supplies a guarded exact-value executable
        # backend. It is generic structural analysis, never a function-name
        # allowlist.
        if analyze_value_cache(resolved.function) is not None:
            return True
        # A local elementwise proof is not enough to change Daft's execution
        # boundary: the batch expression can alter concurrency with downstream
        # native/model UDFs. Until the framework graph supplies an isolation
        # proof, only the value-cache backend is admitted in production.
        return False
    except Exception:
        return False


def _process_identity() -> ProcessIdentity:
    global _PROCESS_PID, _PROCESS_GENERATION, _NEXT_BORROW_ID, _COUNTERS
    pid = os.getpid()
    if pid != _PROCESS_PID:
        with _PROCESS_IDENTITY_LOCK:
            if pid != _PROCESS_PID:
                _PROCESS_GENERATION = (
                    os.environ.get("UDFJIT_PROCESS_GENERATION", "").strip()
                    or secrets.token_hex(16)
                )
                _NEXT_BORROW_ID = 0
                _COUNTERS = ColumnarRuntimeCounters()
                # Publish the PID last so another thread can never observe the
                # new process paired with the inherited generation/counters.
                _PROCESS_PID = pid
    return ProcessIdentity(pid, _PROCESS_GENERATION)


def _next_borrow_id() -> str:
    global _NEXT_BORROW_ID
    process = _process_identity()
    _NEXT_BORROW_ID += 1
    return f"{process.generation}:{_NEXT_BORROW_ID}"


def snapshot_columnar_counters() -> dict[str, int]:
    _process_identity()
    return asdict(_COUNTERS)


def reset_columnar_counters_for_testing() -> None:
    global _COUNTERS
    _process_identity()
    _COUNTERS = ColumnarRuntimeCounters()


def record_native_expression_lowering(kind: str) -> None:
    """Record a value-free Driver-side plan lowering for attribution."""

    _process_identity()
    field = f"native_expression_{kind}_plans"
    if field not in {
        "native_expression_translation_plans",
        "native_expression_whitespace_plans",
        "native_expression_regex_plans",
        "native_expression_language_id_plans",
        "native_expression_length_plans",
    }:
        raise ValueError("native_expression_kind_unsupported")
    setattr(_COUNTERS, field, getattr(_COUNTERS, field) + 1)
    _flush_diagnostic_snapshot()


def record_native_expression_guard(
    *,
    checks: int,
    misses: int,
    rebuilt: bool,
) -> None:
    """Record one Driver execution-boundary dependency revalidation."""

    if checks < 0 or misses < 0 or misses > checks:
        raise ValueError("native_expression_guard_count_invalid")
    _process_identity()
    _COUNTERS.native_expression_guard_checks += checks
    _COUNTERS.native_expression_guard_misses += misses
    if rebuilt:
        _COUNTERS.native_expression_fallback_rebuilds += 1
    _flush_diagnostic_snapshot()


def record_native_expression_null_guard(*, rows: int, null_miss: bool) -> None:
    """Record one Arrow-batch nullability guard before a native expression."""

    if rows < 0:
        raise ValueError("native_expression_null_guard_rows_invalid")
    _process_identity()
    _COUNTERS.native_expression_null_guard_batches += 1
    _COUNTERS.native_expression_null_guard_rows += rows
    if null_miss:
        _COUNTERS.native_expression_null_guard_misses += 1
    _flush_diagnostic_snapshot()


def _flush_diagnostic_snapshot() -> None:
    directory = os.environ.get("UDFJIT_COLUMNAR_DIAGNOSTIC_DIR", "").strip()
    if not directory:
        return
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    process = _process_identity()
    target = root / f"columnar-{process.pid}-{process.generation}.json"
    temporary = root / f".{target.name}.{secrets.token_hex(8)}.tmp"
    document = {
        "format_version": 1,
        "process": process.to_document(),
        "counters": snapshot_columnar_counters(),
    }
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(document, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _chunks(array: Any) -> tuple[Any, ...]:
    chunks = getattr(array, "chunks", None)
    if chunks is None:
        return (array,)
    normalized = tuple(chunks)
    return normalized or (array,)


def _physical_type(array: Any) -> str:
    physical_type = str(getattr(array, "type", "")).strip().lower()
    if not physical_type or len(physical_type) > 128:
        raise ValueError("arrow_physical_type_invalid")
    return physical_type


def _offset_width_bits(physical_type: str) -> int:
    if physical_type in {"large_string", "large_binary"}:
        return 64
    if physical_type in {"string", "binary"}:
        return 32
    return 0


def _validity_buffer_count(chunks: tuple[Any, ...]) -> int:
    count = 0
    for chunk in chunks:
        buffers = getattr(chunk, "buffers", None)
        if not callable(buffers):
            continue
        values = buffers()
        if values and values[0] is not None:
            count += 1
    return count


class ArrowBorrowScope:
    """Process-local keepalive and guard authority for one Arrow input."""

    def __init__(self, array: Any, *, epoch: str) -> None:
        self._array = array
        self._epoch = epoch
        self._process = _process_identity()
        self._borrow_id = _next_borrow_id()
        self._active = False
        self._guarded = False
        self.descriptor: ArrowBatchDescriptor | None = None

    def __getstate__(self) -> object:
        raise TypeError("Arrow borrow scope is process-local")

    def __enter__(self) -> "ArrowBorrowScope":
        if self._active:
            raise RuntimeError("arrow_borrow_already_active")
        chunks = _chunks(self._array)
        physical_type = _physical_type(self._array)
        length = len(self._array)
        self.descriptor = ArrowBatchDescriptor(
            abi_version=ARROW_BATCH_ABI_VERSION,
            physical_type=physical_type,
            length=length,
            offset=int(getattr(self._array, "offset", 0)),
            chunk_lengths=tuple(len(chunk) for chunk in chunks),
            chunk_offsets=tuple(int(getattr(chunk, "offset", 0)) for chunk in chunks),
            null_count=int(getattr(self._array, "null_count", 0)),
            validity_buffer_count=_validity_buffer_count(chunks),
            offset_width_bits=_offset_width_bits(physical_type),
            epoch=self._epoch,
            borrow_id=self._borrow_id,
            process=self._process,
        )
        self._active = True
        guard_arrow_batch_descriptor(
            self.descriptor,
            expected_epoch=self._epoch,
            expected_borrow_id=self._borrow_id,
            expected_process=self._process,
            expected_physical_type=physical_type,
            expected_length=length,
        )
        self._guarded = True
        return self

    def __exit__(self, *_exc: object) -> None:
        self._guarded = False
        self._active = False
        self._array = None

    def load_pylist(self) -> list[Any]:
        if not self._active or not self._guarded or self._array is None:
            raise DescriptorGuardError(DescriptorRejectCode.BORROW_EXPIRED)
        if _process_identity() != self._process:
            raise DescriptorGuardError(DescriptorRejectCode.PROCESS_MISMATCH)
        loader = getattr(self._array, "to_pylist", None)
        if not callable(loader):
            raise TypeError("arrow_pylist_loader_unavailable")
        return list(loader())


def _physical_types_for_logical(logical_type: str) -> frozenset[str]:
    return {
        "string": frozenset({"string", "large_string"}),
        "bool": frozenset({"bool"}),
        "int32": frozenset({"int32"}),
        "int64": frozenset({"int64"}),
        "float32": frozenset({"float"}),
        "float64": frozenset({"double"}),
    }.get(logical_type, frozenset())


def _physical_type_matches_logical(
    physical_type: str,
    logical_type: str,
    *,
    allow_dictionary: bool,
) -> bool:
    if physical_type in _physical_types_for_logical(logical_type):
        return True
    if (
        not allow_dictionary
        or logical_type != "string"
        or not physical_type.startswith("dictionary<")
    ):
        return False
    # Do not admit dictionary values of arbitrary Python/object types. The
    # descriptor is address-free and the Arrow-domain helper performs the
    # corresponding runtime structural checks before semantic entry.
    return physical_type.startswith("dictionary<values=string,") or (
        physical_type.startswith("dictionary<values=large_string,")
    )


def _arrow_output_type(pa: Any, logical_type: str) -> Any:
    factory = {
        "string": "large_string",
        "bool": "bool_",
        "int32": "int32",
        "int64": "int64",
        "float32": "float32",
        "float64": "float64",
    }.get(logical_type)
    if factory is None:
        raise TypeError("columnar_output_type_unsupported")
    return getattr(pa, factory)()


@dataclass(frozen=True)
class _ArrowDictionaryDomain:
    dictionary: Any
    indices: Any
    row_count: int
    unique_count: int


class _DictionaryDomainRefusal(ValueError):
    pass


def _arrow_dictionary_domain(array: Any, compute: Any) -> _ArrowDictionaryDomain:
    current = array
    if _physical_type(current).startswith("dictionary<"):
        current = compute.dictionary_decode(current)
    combiner = getattr(current, "combine_chunks", None)
    if callable(combiner):
        current = combiner()
    if _physical_type(current) not in {"string", "large_string"}:
        raise _DictionaryDomainRefusal("dictionary_value_type_unsupported")
    if int(getattr(current, "null_count", 0)) != 0:
        raise _DictionaryDomainRefusal("dictionary_input_nullable")
    encoded = compute.dictionary_encode(current)
    combiner = getattr(encoded, "combine_chunks", None)
    if callable(combiner):
        encoded = combiner()
    dictionary = getattr(encoded, "dictionary", None)
    indices = getattr(encoded, "indices", None)
    if dictionary is None or indices is None:
        raise _DictionaryDomainRefusal("dictionary_encoding_invalid")
    if _physical_type(dictionary) not in {"string", "large_string"}:
        raise _DictionaryDomainRefusal("dictionary_value_type_unsupported")
    row_count = len(array)
    unique_count = len(dictionary)
    if len(indices) != row_count:
        raise _DictionaryDomainRefusal("dictionary_index_length_mismatch")
    if int(getattr(dictionary, "null_count", 0)) != 0 or int(
        getattr(indices, "null_count", 0)
    ) != 0:
        raise _DictionaryDomainRefusal("dictionary_null_unsupported")
    return _ArrowDictionaryDomain(
        dictionary,
        indices,
        row_count,
        unique_count,
    )


def _dictionary_domain_profitable(
    domain: _ArrowDictionaryDomain,
    *,
    capacity: int,
) -> bool:
    return (
        type(capacity) is int
        and 0 < domain.unique_count <= capacity
        and domain.unique_count * 2 <= domain.row_count
    )


def _diagnostic_phase_start(enabled: bool) -> int:
    return time.perf_counter_ns() if enabled else 0


def _diagnostic_phase_finish(
    enabled: bool,
    started: int,
    counter_name: str,
) -> None:
    if enabled:
        elapsed = time.perf_counter_ns() - started
        setattr(_COUNTERS, counter_name, getattr(_COUNTERS, counter_name) + elapsed)


@dataclass
class ColumnarBatchWrapper:
    """Serializable Daft batch method preserving scalar API semantics."""

    scalar_wrapper: Any
    _native_batch_executor: Any = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _native_batch_unavailable_process: tuple[int, str] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _serialization_version: int = field(
        default=COLUMNAR_WRAPPER_SERIALIZATION_VERSION,
        init=False,
        repr=False,
        compare=False,
    )

    def __getstate__(self) -> dict[str, Any]:
        return {
            "scalar_wrapper": self.scalar_wrapper,
            "_serialization_version": COLUMNAR_WRAPPER_SERIALIZATION_VERSION,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        if (
            type(state) is not dict
            or state.get("_serialization_version")
            != COLUMNAR_WRAPPER_SERIALIZATION_VERSION
            or set(state) != {"scalar_wrapper", "_serialization_version"}
        ):
            raise ValueError("columnar_wrapper_serialization_version_unsupported")
        self.scalar_wrapper = state["scalar_wrapper"]
        self._serialization_version = COLUMNAR_WRAPPER_SERIALIZATION_VERSION
        self._native_batch_executor = None
        self._native_batch_unavailable_process = None

    def _resolve_native_batch_executor(self) -> tuple[Any | None, ProcessIdentity]:
        process = _process_identity()
        executor = self._native_batch_executor
        if executor is not None and executor.matches_process(process):
            if not executor.guards_match(process):
                return None, process
            return executor, process

        process_key = (process.pid, process.generation)
        if (
            executor is None
            and self._native_batch_unavailable_process == process_key
        ):
            return None, process

        # CinderX initialization and force_compile mutate process-global JIT
        # state. Daft may invoke distinct batch wrappers concurrently from its
        # DAFTCPU threads, so a per-wrapper lock is insufficient. Serialize all
        # lazy native builds process-wide, then double-check this wrapper after
        # acquiring the lock. Steady-state executor hits never take this lock.
        with _NATIVE_BATCH_BUILD_LOCK:
            executor = self._native_batch_executor
            if executor is not None and not executor.matches_process(process):
                executor = None
                self._native_batch_executor = None
                self._native_batch_unavailable_process = None
            if executor is None:
                if self._native_batch_unavailable_process == process_key:
                    return None, process
                try:
                    from python_udf_jit.integration.daft_ray.native_batch import (
                        build_native_batch_executor,
                    )

                    executor = build_native_batch_executor(
                        self.scalar_wrapper,
                        process=process,
                    )
                except Exception:
                    executor = None
                if executor is None:
                    self._native_batch_unavailable_process = process_key
                    return None, process
                self._native_batch_executor = executor
                self._native_batch_unavailable_process = None
        if not executor.guards_match(process):
            # A live binding/code/dependency drift is a pre-semantics whole-batch
            # fallback. Do not silently compile a new target in the same process.
            return None, process
        return executor, process

    def _invoke_scalar_rows(
        self,
        daft_instance: Any,
        columns: tuple[list[Any], ...],
    ) -> list[Any]:
        outputs: list[Any] = []
        for lane in zip(*columns, strict=True):
            adapter_before = getattr(self.scalar_wrapper, "_typed_loop_adapter", None)
            # A snapshot would acquire the typed adapter's lock twice per row.
            # The GIL-protected integer is sufficient for value-free attribution
            # and adds no diagnostics-only lock to timing runs.
            hits_before = getattr(adapter_before, "_hits", 0)
            try:
                value = self.scalar_wrapper(daft_instance, *lane)
            except Exception:
                _COUNTERS.python_scalar_fallback_rows += 1
                raise
            adapter_after = getattr(self.scalar_wrapper, "_typed_loop_adapter", None)
            hits_after = getattr(adapter_after, "_hits", 0)
            if adapter_after is not None and hits_after > hits_before:
                # This is a native scalar hit inside one batch boundary.  It is
                # intentionally not reported as a vector batch.
                _COUNTERS.native_jit_rows += 1
            else:
                _COUNTERS.python_scalar_fallback_rows += 1
            outputs.append(value)
        return outputs

    def __call__(self, daft_instance: Any, *series: Any) -> Any:
        _process_identity()
        _COUNTERS.batches += 1
        _COUNTERS.batch_boundary_hits += 1
        # Exact-Unicode currently has no semantics-preserving whole-column
        # executable. Keep the transparent boundary, but attribute every batch
        # to the scalar providers instead of inflating the vector hit count.
        _COUNTERS.vector_unavailable_batches += 1
        semantic_entry = False
        counted_rows = False
        native_accounted = False
        dictionary_accounted = False
        dictionary_enabled = (
            os.environ.get("UDFJIT_COLUMNAR_DICTIONARY", "1") == "1"
        )
        diagnostic_timing = bool(
            os.environ.get("UDFJIT_COLUMNAR_DIAGNOSTIC_DIR", "").strip()
        )
        if not dictionary_enabled:
            _COUNTERS.dictionary_disabled_batches += 1
            dictionary_accounted = True
        try:
            layout = getattr(self.scalar_wrapper, "invocation_layout", None)
            epoch = getattr(layout, "epoch", "")
            if not epoch or not series:
                raise ValueError("columnar_invocation_layout_unavailable")
            arrows = tuple(value.to_arrow() for value in series)
            row_count = len(arrows[0])
            if any(len(value) != row_count for value in arrows[1:]):
                raise ValueError("columnar_input_length_mismatch")
            if len(getattr(layout, "input_types", ())) != len(arrows):
                raise ValueError("columnar_input_arity_mismatch")
            with ExitStack() as stack:
                scopes = tuple(
                    stack.enter_context(ArrowBorrowScope(value, epoch=epoch))
                    for value in arrows
                )
                for scope, logical_type in zip(
                    scopes, layout.input_types, strict=True
                ):
                    descriptor = scope.descriptor
                    assert descriptor is not None
                    if not _physical_type_matches_logical(
                        descriptor.physical_type,
                        logical_type,
                        allow_dictionary=dictionary_enabled,
                    ):
                        raise TypeError("columnar_input_physical_type_mismatch")
                native_executor, native_process = (
                    self._resolve_native_batch_executor()
                )
                if native_executor is None:
                    _COUNTERS.native_batch_unavailable_batches += 1
                    native_accounted = True
                _COUNTERS.arrow_borrows += len(scopes)
                _COUNTERS.rows += row_count
                counted_rows = True

                dictionary_capacity = (
                    getattr(native_executor, "dictionary_capacity", None)
                    if native_executor is not None
                    else None
                )
                dictionary_layout_eligible = (
                    dictionary_enabled
                    and native_executor is not None
                    and type(dictionary_capacity) is int
                    and len(arrows) == 1
                    and tuple(layout.input_types) == ("string",)
                    and layout.output_type == "string"
                    and tuple(layout.input_nullability) == (False,)
                    and not layout.output_nullable
                    and scopes[0].descriptor is not None
                    and scopes[0].descriptor.null_count == 0
                )
                if dictionary_layout_eligible:
                    try:
                        import pyarrow as pa

                        compute = importlib.import_module("pyarrow.compute")

                        started = _diagnostic_phase_start(diagnostic_timing)
                        try:
                            domain = _arrow_dictionary_domain(
                                arrows[0],
                                compute,
                            )
                        finally:
                            _diagnostic_phase_finish(
                                diagnostic_timing,
                                started,
                                "dictionary_encode_ns",
                            )
                        if not _dictionary_domain_profitable(
                            domain,
                            capacity=dictionary_capacity,
                        ):
                            raise _DictionaryDomainRefusal(
                                "dictionary_domain_unprofitable"
                            )
                        loader = getattr(domain.dictionary, "to_pylist", None)
                        if not callable(loader):
                            raise _DictionaryDomainRefusal(
                                "dictionary_pylist_loader_unavailable"
                            )
                        started = _diagnostic_phase_start(diagnostic_timing)
                        try:
                            unique_values = list(loader())
                            if (
                                len(unique_values) != domain.unique_count
                                or any(
                                    type(value) is not str
                                    for value in unique_values
                                )
                            ):
                                raise _DictionaryDomainRefusal(
                                    "dictionary_unique_values_invalid"
                                )
                        finally:
                            _diagnostic_phase_finish(
                                diagnostic_timing,
                                started,
                                "dictionary_unique_materialize_ns",
                            )
                    except Exception:
                        # No target has been called: the existing guarded full
                        # batch loop remains a legal whole-batch fallback.
                        domain = None
                        _COUNTERS.dictionary_unavailable_batches += 1
                        dictionary_accounted = True
                    else:
                        _COUNTERS.materializations += 1
                        _COUNTERS.dictionary_python_unique_rows += len(
                            unique_values
                        )
                        if not native_executor.guards_match(native_process):
                            domain = None
                            unique_values = []
                            _COUNTERS.dictionary_unavailable_batches += 1
                            dictionary_accounted = True
                        else:
                            semantic_entry = True
                            dictionary_accounted = True
                            native_accounted = True
                            _COUNTERS.dictionary_batches += 1
                            _COUNTERS.dictionary_rows += row_count
                            _COUNTERS.dictionary_unique_values += (
                                domain.unique_count
                            )
                            _COUNTERS.native_batch_batches += 1
                            _COUNTERS.native_batch_rows += row_count
                            started = _diagnostic_phase_start(diagnostic_timing)
                            try:
                                unique_outputs = native_executor.invoke(
                                    (unique_values,)
                                )
                            finally:
                                _diagnostic_phase_finish(
                                    diagnostic_timing,
                                    started,
                                    "dictionary_target_ns",
                                )
                            if (
                                len(unique_outputs) != domain.unique_count
                                or any(
                                    type(value) is not str
                                    for value in unique_outputs
                                )
                            ):
                                raise TypeError(
                                    "dictionary_unique_outputs_invalid"
                                )
                            _COUNTERS.dictionary_python_output_rows += len(
                                unique_outputs
                            )
                            started = _diagnostic_phase_start(diagnostic_timing)
                            try:
                                unique_output_array = pa.array(
                                    unique_outputs,
                                    type=_arrow_output_type(
                                        pa,
                                        layout.output_type,
                                    ),
                                )
                                result = compute.take(
                                    unique_output_array,
                                    domain.indices,
                                )
                                if len(result) != row_count:
                                    raise ValueError(
                                        "columnar_output_length_mismatch"
                                    )
                            finally:
                                _diagnostic_phase_finish(
                                    diagnostic_timing,
                                    started,
                                    "dictionary_reconstruct_ns",
                                )
                            _COUNTERS.published_batches += 1
                            return result

                if dictionary_enabled and not dictionary_accounted:
                    _COUNTERS.dictionary_unavailable_batches += 1
                    dictionary_accounted = True
                columns = tuple(scope.load_pylist() for scope in scopes)
                _COUNTERS.materializations += len(columns)
                _COUNTERS.full_python_materializations += len(columns)
                _COUNTERS.full_python_materialized_rows += (
                    row_count * len(columns)
                )
                if (
                    native_executor is not None
                    and native_executor.guards_match(native_process)
                ):
                    semantic_entry = True
                    _COUNTERS.native_batch_batches += 1
                    _COUNTERS.native_batch_rows += row_count
                    native_accounted = True
                    outputs = native_executor.invoke(columns)
                else:
                    if not native_accounted:
                        _COUNTERS.native_batch_unavailable_batches += 1
                        native_accounted = True
                    semantic_entry = True
                    outputs = self._invoke_scalar_rows(daft_instance, columns)
                _COUNTERS.full_python_materializations += 1
                _COUNTERS.full_python_materialized_rows += len(outputs)
            import pyarrow as pa

            result = pa.array(
                outputs,
                type=_arrow_output_type(pa, layout.output_type),
            )
            if len(result) != row_count:
                raise ValueError("columnar_output_length_mismatch")
            _COUNTERS.published_batches += 1
            return result
        except (DescriptorGuardError, TypeError, ValueError):
            if semantic_entry:
                # No replay is legal after the first scalar semantic call.
                raise
            if not native_accounted:
                _COUNTERS.native_batch_unavailable_batches += 1
                native_accounted = True
            if dictionary_enabled and not dictionary_accounted:
                _COUNTERS.dictionary_unavailable_batches += 1
                dictionary_accounted = True
            _COUNTERS.precommit_failures += 1
            # A framework-compatible Arrow value is still required.  Recover
            # only before semantic entry and evaluate the original callable in
            # row order; publication remains whole-batch.
            arrows = tuple(value.to_arrow() for value in series)
            row_count = len(arrows[0]) if arrows else 0
            if any(len(value) != row_count for value in arrows[1:]):
                raise ValueError("columnar_fallback_input_length_mismatch")
            columns = tuple(list(value.to_pylist()) for value in arrows)
            if columns:
                if not counted_rows:
                    _COUNTERS.rows += row_count
                _COUNTERS.materializations += len(columns)
                _COUNTERS.full_python_materializations += len(columns)
                _COUNTERS.full_python_materialized_rows += (
                    row_count * len(columns)
                )
            semantic_entry = True
            outputs = self._invoke_scalar_rows(daft_instance, columns)
            _COUNTERS.full_python_materializations += 1
            _COUNTERS.full_python_materialized_rows += len(outputs)
            import pyarrow as pa

            layout = getattr(self.scalar_wrapper, "invocation_layout", None)
            output_type = getattr(layout, "output_type", "")
            result = pa.array(
                outputs,
                type=(
                    _arrow_output_type(pa, output_type)
                    if output_type
                    else None
                ),
            )
            _COUNTERS.published_batches += 1
            return result
        finally:
            _flush_diagnostic_snapshot()

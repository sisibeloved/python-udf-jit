from __future__ import annotations

import functools
import hashlib
import importlib
import os
import threading
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from python_udf_jit.diagnostics import events
from python_udf_jit.diagnostics.config import (
    DiagnosticRuntimeContext,
    OFF_DIAGNOSTIC_POLICY,
    resolve_diagnostic_policy,
)
from python_udf_jit.diagnostics.events import DecisionEvent
from python_udf_jit.integration.daft_ray.compatibility import (
    DAFT_V0_7_2_TARGET,
    CompatibilityTarget,
    validate_daft_compatibility,
    validate_func_instance,
)
from python_udf_jit.integration.daft_ray.objectref_bridge import (
    install_daft_objectref_bridge,
)
from python_udf_jit.integration.daft_ray.native_expression import (
    NativeExpressionCandidate,
    NativeExpressionLineageRegistry,
)
from python_udf_jit.integration.daft_ray.registry import CandidateRegistry


_HOOK_MARKER = "__python_udf_jit_u2_hook__"
_ORIGINAL_METHOD = "__python_udf_jit_original__"
_INSTALL_LOCK = threading.RLock()
_CALL_LOCK = threading.RLock()
_CALL_STATE = threading.local()
_DEFAULT_REGISTRY: CandidateRegistry | None = None
_MISSING = object()
_NO_NATIVE_EXPRESSION = object()

_LINEAGE_PRESERVING_METHODS = {
    "__getitem__",
    "agg",
    "agg_concat",
    "agg_list",
    "agg_set",
    "any_value",
    "count",
    "describe",
    "distinct",
    "drop_duplicates",
    "drop_nan",
    "drop_null",
    "exclude",
    "explode",
    "filter",
    "into_batches",
    "into_partitions",
    "limit",
    "max",
    "mean",
    "melt",
    "min",
    "offset",
    "pivot",
    "repartition",
    "sample",
    "sort",
    "stddev",
    "sum",
    "summarize",
    "unique",
    "unpivot",
    "with_column",
    "with_column_renamed",
    "with_columns_renamed",
}
_LINEAGE_TERMINAL_METHODS = {
    "__len__",
    "collect",
    "count_rows",
    "show",
    "to_arrow",
    "to_dask_dataframe",
    "to_pandas",
    "to_pydict",
    "to_pylist",
    "to_ray_dataset",
    "to_torch_iter_dataset",
    "to_torch_map_dataset",
}
_LINEAGE_FORCE_FALLBACK_METHODS = {
    "__iter__",
    "concat",
    "except_all",
    "except_distinct",
    "groupby",
    "intersect",
    "intersect_all",
    "iter_partitions",
    "iter_rows",
    "join",
    "pipe",
    "transform",
    "to_arrow_iter",
    "union",
    "union_all",
    "union_all_by_name",
    "union_by_name",
}


class HookStatus(StrEnum):
    INSTALLED = "installed"
    ALREADY_INSTALLED = "already_installed"
    DISABLED = "disabled"
    INCOMPATIBLE = "incompatible"
    ERROR = "error"


@dataclass(frozen=True)
class HookResult:
    status: HookStatus
    reason: str


@dataclass(frozen=True)
class _NativeExpressionProof:
    expression: Any
    wrapper_guard: Any
    semantic_guard: Any
    kind: str


def _emit_fail_open(reason_code: str) -> None:
    try:
        events.try_emit(
            DecisionEvent(
                stage="adapter",
                decision="fail_open",
                reason_code=reason_code,
            )
        )
    except Exception:
        pass


def _contains_expression(value: Any, expression_class: type[Any]) -> bool:
    pending = [value]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        if isinstance(current, expression_class):
            return True
        identity = id(current)
        if identity in visited:
            continue
        visited.add(identity)
        if isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, (list, tuple)):
            pending.extend(current)
    return False


def _columnar_scalar_call_eligible(
    func: Any,
    original_callable: Any,
    kwargs: dict[str, Any],
) -> bool:
    """Conservative, name-free gate for transparent scalar-to-batch lifting."""

    columnar_mode = os.environ.get("UDFJIT_COLUMNAR", "0")
    if columnar_mode == "native-expr":
        return False
    if columnar_mode != "1" or kwargs:
        return False
    try:
        option_proof = bool(
            not object.__getattribute__(func, "is_batch")
            and not object.__getattribute__(func, "is_async")
            and not object.__getattribute__(func, "is_generator")
            and object.__getattribute__(func, "on_error") in {None, "raise"}
        )
    except (AttributeError, TypeError):
        return False
    if not option_proof:
        return False
    try:
        from python_udf_jit.integration.daft_ray.columnar import (
            columnar_boundary_proven,
        )

        return columnar_boundary_proven(func, original_callable)
    except Exception:
        return False


_FINEWEB_LINKS_PATTERN = r"https?://\S+|www\.\S+"
_FINEWEB_COPYRIGHT_PATTERN = (
    r"(?i)(copyright\s*\(?c\)?|©|\(c\)|all rights reserved)[^\n.]*\.?"
)


def _fineweb_experimental_plan_enabled(plan: Any) -> bool:
    """Fail closed for the three independently validated FineWeb lowerings."""

    kind = getattr(plan, "kind", "")
    if kind == "language_id":
        return os.environ.get(
            "UDFJIT_FINEWEB_LANGUAGE_ID_LOWERING",
            "0",
        ) == "1"
    if kind != "regex":
        return True
    pattern = getattr(plan, "pattern", "")
    if pattern == _FINEWEB_LINKS_PATTERN:
        return os.environ.get(
            "UDFJIT_FINEWEB_LINKS_LOWERING",
            "0",
        ) == "1"
    if pattern == _FINEWEB_COPYRIGHT_PATTERN:
        return os.environ.get(
            "UDFJIT_FINEWEB_COPYRIGHT_LOWERING",
            "0",
        ) == "1"
    return True


def _native_expression_lowering(
    daft_module: Any,
    original_func_call: Any,
    func: Any,
    original_callable: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """Lower a structurally proven unary transform into Daft's expression IR."""

    if os.environ.get("UDFJIT_COLUMNAR", "0") not in {"1", "native-expr"}:
        return _NO_NATIVE_EXPRESSION
    if len(args) != 1 or kwargs:
        return _NO_NATIVE_EXPRESSION
    try:
        if (
            object.__getattribute__(func, "is_batch")
            or object.__getattribute__(func, "is_async")
            or object.__getattribute__(func, "is_generator")
            or object.__getattribute__(func, "on_error") not in {None, "raise"}
        ):
            return _NO_NATIVE_EXPRESSION
        from python_udf_jit.compiler.vector_predicate import (
            VectorPredicateCaptureError,
            capture_language_id_score_predicate,
            capture_string_length_predicate,
            capture_string_transform,
        )
        from python_udf_jit.integration.daft_ray.schema import (
            canonicalize_logical_type,
        )
        from python_udf_jit.integration.daft_ray.typed_loop_worker import (
            resolve_typed_loop_callable,
        )

        resolved = resolve_typed_loop_callable(original_callable)
        output_type = canonicalize_logical_type(func.return_dtype)
        if output_type == "string":
            plan = capture_string_transform(resolved.function)
        elif output_type == "bool":
            try:
                plan = capture_string_length_predicate(
                    resolved.function,
                    bound_arguments=resolved.bound_arguments,
                )
            except VectorPredicateCaptureError:
                plan = capture_language_id_score_predicate(
                    resolved.function,
                    bound_arguments=resolved.bound_arguments,
                )
        else:
            return _NO_NATIVE_EXPRESSION
        if not _fineweb_experimental_plan_enabled(plan):
            return _NO_NATIVE_EXPRESSION

        # Constructing a Daft batch UDF is observable framework work even when
        # its expression is later discarded. Prove a supported native shape
        # first so unsupported UDFs retain the exact feature-off plan.
        expression = _native_expression_nonnull_guard(
            daft_module,
            original_func_call,
            func,
            args[0],
            resolved.function,
            resolved.bound_arguments,
        )
        if output_type == "string":
            if plan.kind == "translation":
                for source, replacement in plan.replacements:
                    expression = expression.replace(source, replacement)
                return _NativeExpressionProof(
                    expression,
                    resolved.wrapper_guard,
                    plan,
                    "translation",
                )
            if plan.kind == "whitespace":
                result = expression.regexp_replace(
                    plan.arrow_pattern,
                    " ",
                ).lstrip().rstrip()
                return _NativeExpressionProof(
                    result,
                    resolved.wrapper_guard,
                    plan,
                    "whitespace",
                )
            if plan.kind == "regex":
                result = expression.regexp_replace(
                    plan.arrow_pattern,
                    plan.replacement,
                )
                return _NativeExpressionProof(
                    result,
                    resolved.wrapper_guard,
                    plan,
                    "regex",
                )
        elif output_type == "bool":
            if getattr(plan, "kind", "length") == "language_id":
                patterns = dict(plan.arrow_patterns)
                counts = {
                    name: expression.regexp_count(pattern)
                    for name, pattern in patterns.items()
                }
                english = counts["en"]
                best_is_english = (
                    (english >= counts["zh"])
                    & (english >= counts["ja"])
                    & (english >= counts["ko"])
                    & (english >= counts["ar"])
                    & (english >= counts["ru"])
                )
                nonblank = expression.regexp_count(r"\S") > 0
                threshold = (english * 10) >= expression.length()
                return _NativeExpressionProof(
                    nonblank & best_is_english & threshold,
                    resolved.wrapper_guard,
                    plan,
                    "language_id",
                )
            length = expression.length()
            lower = (
                length >= plan.lower
                if plan.lower_inclusive
                else length > plan.lower
            )
            upper = (
                length <= plan.upper
                if plan.upper_inclusive
                else length < plan.upper
            )
            result = lower & upper
            return _NativeExpressionProof(
                result,
                resolved.wrapper_guard,
                plan,
                "length",
            )
    except Exception:
        pass
    return _NO_NATIVE_EXPRESSION


def _validate_native_expression_nonnull_series(
    series: Any,
    target: Any,
    bound_arguments: dict[str, object],
) -> Any:
    """Borrow a non-null Arrow batch or reproduce the first null-lane error."""

    array = series.to_arrow()
    null_count = int(getattr(array, "null_count", 0))
    if os.environ.get("UDFJIT_COLUMNAR_DIAGNOSTIC_DIR", "").strip():
        from python_udf_jit.integration.daft_ray.columnar import (
            record_native_expression_null_guard,
        )

        record_native_expression_null_guard(
            rows=len(array),
            null_miss=bool(null_count),
        )
    if null_count:
        # Captured native-expression regions are pure exact-string operations;
        # null is their only value-dependent exceptional lane. Invoke the live
        # scalar authority at the first null to preserve its exception.
        for index, value in enumerate(array.to_pylist()):
            if value is None:
                try:
                    target(value, **bound_arguments)
                except Exception as error:
                    # Daft 0.7.2 row UDFs report one indexed PyO3 error inside
                    # a ComputeError. Batch UDF exceptions otherwise escape as
                    # the raw Python type, so reconstruct that public boundary.
                    from daft.exceptions import DaftCoreException

                    raise DaftCoreException(
                        "DaftError::ComputeError Error processing some rows:\n"
                        f"{index}: DaftError::PyO3Error "
                        f"{type(error).__name__}: {error}"
                    ) from None
                raise TypeError("native_expression_null_contract_failed")
    return array


def _native_expression_nonnull_guard(
    daft_module: Any,
    original_func_call: Any,
    func: Any,
    expression: Any,
    target: Any,
    bound_arguments: Any,
) -> Any:
    """Insert one transparent Arrow-batch null guard before native lowering."""

    func_factory = getattr(daft_module, "func", None)
    batch = getattr(func_factory, "batch", None)
    if not callable(batch):
        # Unit-test framework doubles have no batch decorator. Real Daft 0.7.2
        # compatibility validation requires it before this path is installed.
        return expression
    frozen_arguments = dict(bound_arguments)

    def validate_nonnull(series):
        return _validate_native_expression_nonnull_series(
            series,
            target,
            frozen_arguments,
        )

    validate_nonnull.__name__ = getattr(target, "__name__", "validate_nonnull")
    validate_nonnull.__qualname__ = getattr(
        target,
        "__qualname__",
        validate_nonnull.__name__,
    )

    batch_options = {
        "return_dtype": daft_module.DataType.string(),
        "on_error": "raise",
    }
    try:
        process_policy = object.__getattribute__(func, "use_process")
    except (AttributeError, TypeError):
        process_policy = None
    if process_policy is not None:
        if type(process_policy) is not bool:
            raise TypeError("native_expression_process_policy_invalid")
        batch_options["use_process"] = process_policy
    validator = batch(**batch_options)(validate_nonnull)
    return original_func_call(validator, expression)


def _record_native_expression_lowering(kind: str) -> None:
    if not os.environ.get("UDFJIT_COLUMNAR_DIAGNOSTIC_DIR", "").strip():
        return
    from python_udf_jit.integration.daft_ray.columnar import (
        record_native_expression_lowering,
    )

    record_native_expression_lowering(kind)


def _record_native_expression_guard(
    *,
    checks: int,
    misses: int,
    rebuilt: bool,
) -> None:
    if not os.environ.get("UDFJIT_COLUMNAR_DIAGNOSTIC_DIR", "").strip():
        return
    from python_udf_jit.integration.daft_ray.columnar import (
        record_native_expression_guard,
    )

    record_native_expression_guard(
        checks=checks,
        misses=misses,
        rebuilt=rebuilt,
    )


def _lineage_bypass_enabled() -> bool:
    return bool(getattr(_CALL_STATE, "native_lineage_bypass", False))


def _with_lineage_bypass(callable_object, *args, **kwargs):
    previous = _lineage_bypass_enabled()
    _CALL_STATE.native_lineage_bypass = True
    try:
        return callable_object(*args, **kwargs)
    finally:
        _CALL_STATE.native_lineage_bypass = previous


def _install_native_expression_lineage_hooks(
    dataframe_class: type[Any],
    lineage: NativeExpressionLineageRegistry,
) -> None:
    wrapped_names = {"where", "select", "with_columns"}

    def install_preserving(name: str) -> None:
        if name in wrapped_names or not hasattr(dataframe_class, name):
            return
        original = getattr(dataframe_class, name)
        if not callable(original) or getattr(original, _HOOK_MARKER, False):
            return

        @functools.wraps(original)
        def wrapped(self, *args, **kwargs):
            if _lineage_bypass_enabled() or lineage.bypass_enabled():
                return original(self, *args, **kwargs)
            candidates = lineage.candidates_for(
                (*args, kwargs),
                dataframe=self,
            )
            native_args = lineage.native_arguments(args, candidates)
            native_kwargs = lineage.native_arguments(kwargs, candidates)
            result = original(self, *native_args, **native_kwargs)
            lineage.bind_operation(
                result,
                parent=self,
                operation=original,
                args=args,
                kwargs=kwargs,
                candidates=candidates,
            )
            return result

        setattr(wrapped, _HOOK_MARKER, True)
        setattr(wrapped, _ORIGINAL_METHOD, original)
        setattr(dataframe_class, name, wrapped)

    def install_terminal(name: str) -> None:
        if not hasattr(dataframe_class, name):
            return
        original = getattr(dataframe_class, name)
        if not callable(original) or getattr(original, _HOOK_MARKER, False):
            return

        @functools.wraps(original)
        def wrapped(self, *args, **kwargs):
            if _lineage_bypass_enabled() or not lineage.has_lineage(self):
                return original(self, *args, **kwargs)
            try:
                if object.__getattribute__(self, "_result") is not None:
                    return original(self, *args, **kwargs)
            except (AttributeError, TypeError):
                pass
            resolution = lineage.resolve(self)
            _record_native_expression_guard(
                checks=resolution.guard_checks,
                misses=resolution.guard_misses,
                rebuilt=resolution.rebuilt,
            )
            return _with_lineage_bypass(
                original,
                resolution.dataframe,
                *args,
                **kwargs,
            )

        setattr(wrapped, _HOOK_MARKER, True)
        setattr(wrapped, _ORIGINAL_METHOD, original)
        setattr(dataframe_class, name, wrapped)

    def install_force_fallback(name: str) -> None:
        if not hasattr(dataframe_class, name):
            return
        original = getattr(dataframe_class, name)
        if not callable(original) or getattr(original, _HOOK_MARKER, False):
            return

        @functools.wraps(original)
        def wrapped(self, *args, **kwargs):
            if _lineage_bypass_enabled() or not (
                lineage.has_lineage(self)
                or lineage.has_lineage_in(args)
                or lineage.has_lineage_in(kwargs)
            ):
                return original(self, *args, **kwargs)
            parent = lineage.resolve(self, force_fallback=True)
            resolved_args = lineage.resolve_value(
                args,
                force_fallback=True,
            )
            resolved_kwargs = lineage.resolve_value(
                kwargs,
                force_fallback=True,
            )
            _record_native_expression_guard(
                checks=(
                    parent.guard_checks
                    + resolved_args.guard_checks
                    + resolved_kwargs.guard_checks
                ),
                misses=(
                    parent.guard_misses
                    + resolved_args.guard_misses
                    + resolved_kwargs.guard_misses
                ),
                rebuilt=bool(
                    parent.rebuilt
                    or resolved_args.rebuilt
                    or resolved_kwargs.rebuilt
                ),
            )
            return _with_lineage_bypass(
                original,
                parent.dataframe,
                *resolved_args.value,
                **resolved_kwargs.value,
            )

        setattr(wrapped, _HOOK_MARKER, True)
        setattr(wrapped, _ORIGINAL_METHOD, original)
        setattr(dataframe_class, name, wrapped)

    for method_name in _LINEAGE_PRESERVING_METHODS:
        install_preserving(method_name)
    terminal_methods = {
        *_LINEAGE_TERMINAL_METHODS,
        *(
            name
            for name in dir(dataframe_class)
            if name.startswith("write_")
        ),
    }
    for method_name in terminal_methods:
        install_terminal(method_name)
    for method_name in _LINEAGE_FORCE_FALLBACK_METHODS:
        install_force_fallback(method_name)


def install_daft_control_hooks(
    *,
    daft_module: Any,
    func_class: type[Any],
    dataframe_class: type[Any],
    expression_class: type[Any],
    mode: str,
    registry: CandidateRegistry,
    target: CompatibilityTarget = DAFT_V0_7_2_TARGET,
) -> HookResult:
    if mode == "off":
        return HookResult(HookStatus.DISABLED, "mode_off")
    if mode not in {"observe", "auto"}:
        return HookResult(HookStatus.DISABLED, "invalid_mode")

    with _INSTALL_LOCK:
        native_lineage = NativeExpressionLineageRegistry(
            expression_class,
            dataframe_class,
        )
        native_lineage_active = False
        func_method = func_class.__call__
        dataframe_methods = {
            name: getattr(dataframe_class, name)
            for name in ("where", "select", "with_columns")
        }
        func_installed = bool(getattr(func_method, _HOOK_MARKER, False))
        operation_installed = {
            name: bool(getattr(method, _HOOK_MARKER, False))
            for name, method in dataframe_methods.items()
        }
        if func_installed and all(operation_installed.values()):
            return HookResult(HookStatus.ALREADY_INSTALLED, "hooks_already_installed")
        if func_installed or any(operation_installed.values()):
            _emit_fail_open("partial_hook_state")
            return HookResult(HookStatus.ERROR, "partial_hook_state")

        compatibility = validate_daft_compatibility(
            daft_module, func_class, dataframe_class, target
        )
        if not compatibility.compatible:
            _emit_fail_open(compatibility.reason)
            return HookResult(HookStatus.INCOMPATIBLE, compatibility.reason)

        original_func_call = func_method

        @functools.wraps(original_func_call)
        def wrapped_func_call(self, *args, **kwargs):
            nonlocal native_lineage_active
            expression_values = (*args, *kwargs.values())
            if not any(
                _contains_expression(value, expression_class)
                for value in expression_values
            ):
                return original_func_call(self, *args, **kwargs)

            instance_report = validate_func_instance(self, target)
            if not instance_report.compatible:
                _emit_fail_open(instance_report.reason)
                return original_func_call(self, *args, **kwargs)

            with _CALL_LOCK:
                active = getattr(_CALL_STATE, "active_func_ids", set())
                if id(self) in active:
                    return original_func_call(self, *args, **kwargs)
                active = set(active)
                active.add(id(self))
                _CALL_STATE.active_func_ids = active
                original_callable = self._method
                native_proof = _native_expression_lowering(
                    daft_module,
                    original_func_call,
                    self,
                    original_callable,
                    args,
                    kwargs,
                )
                if native_proof is not _NO_NATIVE_EXPRESSION:
                    try:
                        if not native_lineage_active:
                            _install_native_expression_lineage_hooks(
                                dataframe_class,
                                native_lineage,
                            )
                            native_lineage_active = True
                        fallback_expression = original_func_call(
                            self,
                            *args,
                            **kwargs,
                        )
                        native_lineage.bind_candidate(
                            fallback_expression,
                            NativeExpressionCandidate(
                                native_expression=native_proof.expression,
                                input_expression=args[0],
                                wrapper_guard=native_proof.wrapper_guard,
                                semantic_guard=native_proof.semantic_guard,
                                kind=native_proof.kind,
                            ),
                        )
                        _record_native_expression_lowering(native_proof.kind)
                    except Exception:
                        active.remove(id(self))
                        _CALL_STATE.active_func_ids = active
                        return original_func_call(self, *args, **kwargs)
                    active.remove(id(self))
                    _CALL_STATE.active_func_ids = active
                    return fallback_expression
                use_columnar_batch = _columnar_scalar_call_eligible(
                    self,
                    original_callable,
                    kwargs,
                )
                original_is_batch = getattr(self, "is_batch", _MISSING)
                try:
                    record = registry.register(self, original_callable)
                    if use_columnar_batch:
                        from python_udf_jit.integration.daft_ray.columnar import (
                            ColumnarBatchWrapper,
                        )

                        record.batch_wrapper = ColumnarBatchWrapper(record.wrapper)
                        self._method = record.batch_wrapper
                        self.is_batch = True
                    else:
                        self._method = record.wrapper
                except Exception:
                    self._method = original_callable
                    if original_is_batch is not _MISSING:
                        self.is_batch = original_is_batch
                    active.remove(id(self))
                    _CALL_STATE.active_func_ids = active
                    _emit_fail_open("candidate_registration_failed")
                    return original_func_call(self, *args, **kwargs)

                try:
                    expression = original_func_call(self, *args, **kwargs)
                finally:
                    self._method = original_callable
                    if use_columnar_batch:
                        if original_is_batch is _MISSING:
                            del self.is_batch
                        else:
                            self.is_batch = original_is_batch
                    active.remove(id(self))
                    _CALL_STATE.active_func_ids = active

            try:
                registry.bind_expression(
                    expression,
                    record,
                    invocation_args=args,
                    invocation_kwargs=kwargs,
                )
            except Exception:
                _emit_fail_open("expression_binding_failed")
            return expression

        setattr(wrapped_func_call, _HOOK_MARKER, True)
        setattr(wrapped_func_call, _ORIGINAL_METHOD, original_func_call)
        func_class.__call__ = wrapped_func_call
        for operation_name, original_operation in dataframe_methods.items():
            def make_operation_wrapper(name, original):
                @functools.wraps(original)
                def wrapped(self, *args, **kwargs):
                    if native_lineage.bypass_enabled():
                        return original(self, *args, **kwargs)
                    try:
                        registry.finalize_operation(
                            self,
                            name,
                            args,
                            kwargs,
                        )
                    except Exception:
                        _emit_fail_open("operation_finalization_failed")
                    if not native_lineage_active:
                        return original(self, *args, **kwargs)
                    candidates = native_lineage.candidates_for(
                        (*args, kwargs),
                        dataframe=self,
                    )
                    native_args = native_lineage.native_arguments(args, candidates)
                    native_kwargs = native_lineage.native_arguments(
                        kwargs,
                        candidates,
                    )
                    result = original(self, *native_args, **native_kwargs)
                    native_lineage.bind_operation(
                        result,
                        parent=self,
                        operation=original,
                        args=args,
                        kwargs=kwargs,
                        candidates=candidates,
                    )
                    return result

                setattr(wrapped, _HOOK_MARKER, True)
                setattr(wrapped, _ORIGINAL_METHOD, original)
                return wrapped

            setattr(
                dataframe_class,
                operation_name,
                make_operation_wrapper(operation_name, original_operation),
            )
        return HookResult(HookStatus.INSTALLED, "compatible_hooks_installed")


def uninstall_daft_control_hooks(
    func_class: type[Any], dataframe_class: type[Any]
) -> None:
    """Test/diagnostic rollback; production normally leaves hooks for process life."""

    with _INSTALL_LOCK:
        func_method = func_class.__call__
        if getattr(func_method, _HOOK_MARKER, False):
            func_class.__call__ = getattr(func_method, _ORIGINAL_METHOD)
        method_names = {
            "where",
            "select",
            "with_columns",
            *_LINEAGE_PRESERVING_METHODS,
            *_LINEAGE_TERMINAL_METHODS,
            *_LINEAGE_FORCE_FALLBACK_METHODS,
            *(
                name
                for name in dir(dataframe_class)
                if name.startswith("write_")
            ),
        }
        for operation_name in method_names:
            if not hasattr(dataframe_class, operation_name):
                continue
            dataframe_method = getattr(dataframe_class, operation_name)
            if getattr(dataframe_method, _HOOK_MARKER, False):
                setattr(
                    dataframe_class,
                    operation_name,
                    getattr(dataframe_method, _ORIGINAL_METHOD),
                )


def _manifest_sha256_from_environment() -> str | None:
    explicit = os.environ.get("UDFJIT_MANIFEST_SHA256", "")
    if explicit:
        return explicit
    path = os.environ.get("UDFJIT_MANIFEST_PATH", "")
    if not path:
        return None
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def install_default_daft_hooks(daft_module: Any) -> HookResult:
    """Resolve Daft-private objects only after the top-level Daft import completed."""

    global _DEFAULT_REGISTRY
    mode = os.environ.get("UDFJIT_MODE", "off")
    if mode == "off":
        return HookResult(HookStatus.DISABLED, "mode_off")
    manifest_sha256 = _manifest_sha256_from_environment()
    if manifest_sha256 is None:
        _emit_fail_open("manifest_missing")
        return HookResult(HookStatus.ERROR, "manifest_missing")
    if os.environ.get("UDFJIT_DIAGNOSTICS", "off") == "off":
        diagnostic_policy = OFF_DIAGNOSTIC_POLICY
    else:
        diagnostic_policy = resolve_diagnostic_policy(
            os.environ,
            DiagnosticRuntimeContext(
                dedicated_worker=(
                    os.environ.get("PYTHONJITUDFDIAGNOSTICS") == "1"
                ),
                workspace_root=Path.cwd(),
            ),
        )
    try:
        flotilla_module = importlib.import_module(
            "daft.runners.flotilla"
        )
        bridge = install_daft_objectref_bridge(
            flotilla_module
        )
        if not bridge.installed:
            _emit_fail_open(bridge.reason)
            return HookResult(
                HookStatus.ERROR,
                bridge.reason,
            )
        udf_module = importlib.import_module("daft.udf.udf_v2")
        dataframe_module = importlib.import_module("daft.dataframe.dataframe")
        expressions_module = importlib.import_module("daft.expressions.expressions")
        if _DEFAULT_REGISTRY is None:
            _DEFAULT_REGISTRY = CandidateRegistry(
                manifest_sha256,
                job_namespace=os.environ.get(
                    "UDFJIT_JOB_NAMESPACE",
                    "default-ray-job",
                ),
                diagnostic_policy=diagnostic_policy,
                diagnostic_run_id=os.environ.get(
                    "UDFJIT_RUN_ID",
                    "driver-diagnostic",
                ),
                diagnostic_runtime_mode=mode,
                diagnostic_process_key=f"driver-{os.getpid()}",
            )
        return install_daft_control_hooks(
            daft_module=daft_module,
            func_class=udf_module.Func,
            dataframe_class=dataframe_module.DataFrame,
            expression_class=expressions_module.Expression,
            mode=mode,
            registry=_DEFAULT_REGISTRY,
        )
    except Exception as error:
        _emit_fail_open(f"hook_install_failed:{type(error).__name__}")
        return HookResult(HookStatus.ERROR, "hook_install_failed")

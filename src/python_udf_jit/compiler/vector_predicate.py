from __future__ import annotations

import ast
import builtins
import functools
import hashlib
import inspect
import re
import textwrap
import types
from dataclasses import dataclass
from typing import Mapping


class VectorPredicateCaptureError(ValueError):
    pass


@dataclass(frozen=True)
class StringLengthPredicatePlan:
    """Exact ``lower <[=] len(text) <[=] upper`` semantics."""

    function: types.FunctionType
    code: types.CodeType
    lower: int
    upper: int
    lower_inclusive: bool
    upper_inclusive: bool
    globals_dict: dict[str, object]
    global_len_present: bool
    len_callable: object
    default_arguments: tuple[tuple[str, int], ...]

    def matches(self) -> bool:
        if self.function.__code__ is not self.code:
            return False
        present = "len" in self.globals_dict
        if present != self.global_len_present:
            return False
        observed = self.globals_dict["len"] if present else builtins.len
        if observed is not self.len_callable:
            return False
        try:
            parameters = inspect.signature(self.function).parameters
            return all(
                name in parameters
                and type(parameters[name].default) is int
                and parameters[name].default == expected
                for name, expected in self.default_arguments
            )
        except (TypeError, ValueError):
            return False


@dataclass(frozen=True)
class LanguageIdScorePlan:
    """Exact six-profile language score specialized for target ``en``/0.1."""

    function: types.FunctionType
    code: types.CodeType
    profiles_dict: dict[str, object]
    regex_items: tuple[tuple[str, object], ...]
    arrow_patterns: tuple[tuple[str, str], ...]

    kind = "language_id"

    def matches(self) -> bool:
        if self.function.__code__ is not self.code:
            return False
        if self.function.__globals__.get("_LANG_CHAR_RE", _ABSENT) is not self.profiles_dict:
            return False
        return all(self.profiles_dict.get(name, _ABSENT) is regex for name, regex in self.regex_items)


@dataclass(frozen=True)
class StringTranslationPlan:
    """One-pass ``str.translate`` mapping safe to express as Arrow kernels."""

    function: types.FunctionType
    code: types.CodeType
    replacements: tuple[tuple[str, str], ...]
    globals_dict: dict[str, object]
    global_str_present: bool
    str_callable: object

    kind = "translation"

    def matches(self) -> bool:
        if self.function.__code__ is not self.code:
            return False
        present = "str" in self.globals_dict
        if present != self.global_str_present:
            return False
        observed = self.globals_dict["str"] if present else builtins.str
        return observed is self.str_callable


@dataclass(frozen=True)
class WhitespaceNormalizationPlan:
    r"""Exact ``re.compile(r"\s+").sub(" ", text).strip()`` semantics."""

    function: types.FunctionType
    code: types.CodeType
    globals_dict: dict[str, object]
    regex_name: str
    regex_object: object
    arrow_pattern: str

    kind = "whitespace"

    def matches(self) -> bool:
        return (
            self.function.__code__ is self.code
            and self.globals_dict.get(self.regex_name, _ABSENT)
            is self.regex_object
        )


@dataclass(frozen=True)
class RegexSubstitutionPlan:
    """A cross-engine-safe compiled regex substitution."""

    function: types.FunctionType
    code: types.CodeType
    globals_dict: dict[str, object]
    regex_name: str
    regex_object: object
    pattern: str
    arrow_pattern: str
    replacement: str

    kind = "regex"

    def matches(self) -> bool:
        return (
            self.function.__code__ is self.code
            and self.globals_dict.get(self.regex_name, _ABSENT)
            is self.regex_object
        )


@dataclass(frozen=True)
class JoinTranslationPlan:
    r"""Exact ``"".join(c if c not in SET else " " for c in text)`` semantics.

    ``SET`` must resolve to a constant frozenset of single characters: either
    a module-level name, or a zero-argument ``functools.lru_cache`` provider.
    The optional ``text = s.strip()`` prefix lowers to a strip *before* the
    per-character replacement, preserving boundary behavior for set members
    that are not whitespace. Replacement never collapses runs.
    """

    function: types.FunctionType
    code: types.CodeType
    globals_dict: dict[str, object]
    set_name: str
    set_source: object
    codepoints: tuple[int, ...]
    arrow_pattern: str
    strip_input: bool

    kind = "join_translate"

    def matches(self) -> bool:
        return (
            self.function.__code__ is self.code
            and self.globals_dict.get(self.set_name, _ABSENT)
            is self.set_source
        )


_ABSENT = object()
_RE_PATTERN_TYPE = type(re.compile(""))
_KNOWN_CROSS_ENGINE_REGEX_SUBSTITUTIONS = {
    (
        r"https?://\S+|www\.\S+",
        re.IGNORECASE | re.UNICODE,
        "",
    ): r"(?i)https?://\S+|www\.\S+",
    (
        r"(?i)(copyright\s*\(?c\)?|©|\(c\)|all rights reserved)[^\n.]*\.?",
        re.IGNORECASE | re.UNICODE,
        "",
    ): r"(?i)(copyright\s*\(?c\)?|©|\(c\)|all rights reserved)[^\n.]*\.?",
}
_LANGUAGE_ID_HELPER_AST_SHA256 = (
    "1288702ffba59fab1c1f59680eb3706ffc23a5b60401991725d03f4587595a36"
)
_LANGUAGE_REGEX_PATTERNS = (
    ("en", r"[a-zA-Z]", r"[a-zA-Z]"),
    ("zh", r"[\u4e00-\u9fff]", r"[\x{4E00}-\x{9FFF}]"),
    ("ja", r"[\u3040-\u309f\u30a0-\u30ff]", r"[\x{3040}-\x{309F}\x{30A0}-\x{30FF}]"),
    ("ko", r"[\uac00-\ud7af]", r"[\x{AC00}-\x{D7AF}]"),
    ("ar", r"[\u0600-\u06ff]", r"[\x{0600}-\x{06FF}]"),
    ("ru", r"[\u0400-\u04ff]", r"[\x{0400}-\x{04FF}]"),
)


def _function_node(
    function: types.FunctionType,
) -> tuple[ast.FunctionDef, int]:
    try:
        lines, first_line = inspect.getsourcelines(function)
    except (OSError, TypeError) as error:
        raise VectorPredicateCaptureError("source_unavailable") from error
    source = textwrap.dedent("".join(lines))
    # The proof is derived from source, so prove that source still describes
    # the live code object before authorizing a different execution engine.
    from python_udf_jit.compiler.typed_frontend import (
        TypedCaptureError,
        _validate_source_identity,
    )

    try:
        _validate_source_identity(source, function)
    except TypedCaptureError as error:
        raise VectorPredicateCaptureError(error.reason_code) from error
    module = ast.parse(source)
    nodes = [node for node in module.body if isinstance(node, ast.FunctionDef)]
    if len(nodes) != 1:
        raise VectorPredicateCaptureError("function_source_shape_unsupported")
    return nodes[0], first_line


def _function_ast_sha256(function: types.FunctionType) -> str:
    node, _first_line = _function_node(function)
    document = ast.dump(node, include_attributes=False)
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def capture_language_id_score_predicate(
    function: types.FunctionType,
    *,
    bound_arguments: Mapping[str, object],
) -> LanguageIdScorePlan:
    if type(function) is not types.FunctionType:
        raise VectorPredicateCaptureError("function_required")
    if set(bound_arguments) != {"lang", "min_score"}:
        raise VectorPredicateCaptureError("language_bound_arguments_unsupported")
    if bound_arguments["lang"] != "en" or bound_arguments["min_score"] != 0.1:
        raise VectorPredicateCaptureError("language_bound_arguments_unsupported")
    if _function_ast_sha256(function) != _LANGUAGE_ID_HELPER_AST_SHA256:
        raise VectorPredicateCaptureError("language_helper_shape_unsupported")
    profiles = function.__globals__.get("_LANG_CHAR_RE", _ABSENT)
    if type(profiles) is not dict or tuple(profiles) != tuple(
        name for name, _python, _arrow in _LANGUAGE_REGEX_PATTERNS
    ):
        raise VectorPredicateCaptureError("language_profiles_unsupported")
    regex_items: list[tuple[str, object]] = []
    for name, python_pattern, _arrow_pattern in _LANGUAGE_REGEX_PATTERNS:
        regex = profiles.get(name, _ABSENT)
        if (
            type(regex) is not _RE_PATTERN_TYPE
            or regex.pattern != python_pattern
            or regex.flags != re.UNICODE
        ):
            raise VectorPredicateCaptureError("language_profile_regex_unsupported")
        regex_items.append((name, regex))
    return LanguageIdScorePlan(
        function=function,
        code=function.__code__,
        profiles_dict=profiles,
        regex_items=tuple(regex_items),
        arrow_patterns=tuple((name, arrow) for name, _python, arrow in _LANGUAGE_REGEX_PATTERNS),
    )


def _constant_bindings(
    function: types.FunctionType,
    bound_arguments: Mapping[str, object],
) -> dict[str, object]:
    signature = inspect.signature(function)
    values: dict[str, object] = {}
    for name, parameter in signature.parameters.items():
        if name in bound_arguments:
            values[name] = bound_arguments[name]
        elif parameter.default is not inspect.Parameter.empty:
            values[name] = parameter.default
    if set(bound_arguments) - set(signature.parameters):
        raise VectorPredicateCaptureError("bound_argument_unknown")
    return values


def _integer_name(expression: ast.expr, constants: Mapping[str, object]) -> int:
    if not isinstance(expression, ast.Name) or expression.id not in constants:
        raise VectorPredicateCaptureError("length_bound_not_constant")
    value = constants[expression.id]
    if type(value) is not int:
        raise VectorPredicateCaptureError("length_bound_not_int")
    return value


def _input_name(node: ast.FunctionDef) -> str:
    positional = [*node.args.posonlyargs, *node.args.args]
    if not positional:
        raise VectorPredicateCaptureError("function_input_missing")
    return positional[0].arg


def _statements(node: ast.FunctionDef) -> list[ast.stmt]:
    statements = list(node.body)
    if (
        statements
        and isinstance(statements[0], ast.Expr)
        and isinstance(statements[0].value, ast.Constant)
        and isinstance(statements[0].value.value, str)
    ):
        statements = statements[1:]
    return statements


def capture_string_length_predicate(
    function: types.FunctionType,
    *,
    bound_arguments: Mapping[str, object],
) -> StringLengthPredicatePlan:
    """Capture a name-free, side-effect-free exact-string length interval."""

    if type(function) is not types.FunctionType:
        raise VectorPredicateCaptureError("function_required")
    node, _first_line = _function_node(function)
    statements = _statements(node)
    input_name = _input_name(node)
    if (
        len(statements) != 2
        or not isinstance(statements[0], ast.Assign)
        or len(statements[0].targets) != 1
        or not isinstance(statements[0].targets[0], ast.Name)
        or not isinstance(statements[0].value, ast.Call)
        or not isinstance(statements[0].value.func, ast.Name)
        or statements[0].value.func.id != "len"
        or len(statements[0].value.args) != 1
        or statements[0].value.keywords
        or not isinstance(statements[0].value.args[0], ast.Name)
        or statements[0].value.args[0].id != input_name
        or not isinstance(statements[1], ast.Return)
        or not isinstance(statements[1].value, ast.Compare)
    ):
        raise VectorPredicateCaptureError("length_predicate_shape_unsupported")
    length_name = statements[0].targets[0].id
    comparison = statements[1].value
    if (
        len(comparison.ops) != 2
        or len(comparison.comparators) != 2
        or type(comparison.ops[0]) not in {ast.Lt, ast.LtE}
        or type(comparison.ops[1]) not in {ast.Lt, ast.LtE}
        or not isinstance(comparison.comparators[0], ast.Name)
        or comparison.comparators[0].id != length_name
    ):
        raise VectorPredicateCaptureError("length_interval_shape_unsupported")
    constants = _constant_bindings(function, bound_arguments)
    lower = _integer_name(comparison.left, constants)
    upper = _integer_name(comparison.comparators[1], constants)
    if lower > upper:
        raise VectorPredicateCaptureError("length_interval_empty")
    globals_dict = function.__globals__
    global_len_present = "len" in globals_dict
    len_callable = globals_dict["len"] if global_len_present else builtins.len
    if len_callable is not builtins.len:
        raise VectorPredicateCaptureError("builtin_len_not_exact")
    parameters = inspect.signature(function).parameters
    default_arguments = tuple(
        (name, value)
        for name, value in constants.items()
        if name not in bound_arguments
        and name in parameters
        and parameters[name].default is not inspect.Parameter.empty
        and type(value) is int
    )
    return StringLengthPredicatePlan(
        function=function,
        code=function.__code__,
        lower=lower,
        upper=upper,
        lower_inclusive=type(comparison.ops[0]) is ast.LtE,
        upper_inclusive=type(comparison.ops[1]) is ast.LtE,
        globals_dict=globals_dict,
        global_len_present=global_len_present,
        len_callable=len_callable,
        default_arguments=default_arguments,
    )


def _translation_call(
    statements: list[ast.stmt],
    input_name: str,
) -> ast.Call:
    if len(statements) == 1 and isinstance(statements[0], ast.Return):
        returned = statements[0].value
        if (
            isinstance(returned, ast.Call)
            and isinstance(returned.func, ast.Attribute)
            and returned.func.attr == "translate"
            and isinstance(returned.func.value, ast.Name)
            and returned.func.value.id == input_name
            and len(returned.args) == 1
            and not returned.keywords
            and isinstance(returned.args[0], ast.Call)
        ):
            return returned.args[0]
    if (
        len(statements) == 2
        and isinstance(statements[0], ast.Assign)
        and len(statements[0].targets) == 1
        and isinstance(statements[0].targets[0], ast.Name)
        and isinstance(statements[0].value, ast.Call)
        and isinstance(statements[1], ast.Return)
        and isinstance(statements[1].value, ast.Call)
    ):
        table_name = statements[0].targets[0].id
        returned = statements[1].value
        if (
            isinstance(returned.func, ast.Attribute)
            and returned.func.attr == "translate"
            and isinstance(returned.func.value, ast.Name)
            and returned.func.value.id == input_name
            and len(returned.args) == 1
            and isinstance(returned.args[0], ast.Name)
            and returned.args[0].id == table_name
            and not returned.keywords
        ):
            return statements[0].value
    raise VectorPredicateCaptureError("translation_shape_unsupported")


def capture_string_translation(
    function: types.FunctionType,
) -> StringTranslationPlan:
    """Capture a non-recursive literal ``str.translate`` mapping."""

    if type(function) is not types.FunctionType:
        raise VectorPredicateCaptureError("function_required")
    node, _first_line = _function_node(function)
    maketrans = _translation_call(_statements(node), _input_name(node))
    if (
        not isinstance(maketrans.func, ast.Attribute)
        or maketrans.func.attr != "maketrans"
        or not isinstance(maketrans.func.value, ast.Name)
        or maketrans.func.value.id != "str"
        or len(maketrans.args) != 1
        or maketrans.keywords
        or not isinstance(maketrans.args[0], ast.Dict)
    ):
        raise VectorPredicateCaptureError("translation_table_unsupported")
    mapping: dict[str, str] = {}
    for key_node, value_node in zip(
        maketrans.args[0].keys,
        maketrans.args[0].values,
        strict=True,
    ):
        if (
            not isinstance(key_node, ast.Constant)
            or type(key_node.value) is not str
            or len(key_node.value) != 1
            or not isinstance(value_node, ast.Constant)
            or type(value_node.value) is not str
        ):
            raise VectorPredicateCaptureError("translation_entry_unsupported")
        mapping[key_node.value] = value_node.value
    if not mapping:
        raise VectorPredicateCaptureError("translation_table_empty")
    sources = frozenset(mapping)
    if any(sources.intersection(replacement) for replacement in mapping.values()):
        raise VectorPredicateCaptureError("translation_requires_one_pass")
    globals_dict = function.__globals__
    global_str_present = "str" in globals_dict
    str_callable = globals_dict["str"] if global_str_present else builtins.str
    if str_callable is not builtins.str:
        raise VectorPredicateCaptureError("builtin_str_not_exact")
    return StringTranslationPlan(
        function=function,
        code=function.__code__,
        replacements=tuple(mapping.items()),
        globals_dict=globals_dict,
        global_str_present=global_str_present,
        str_callable=str_callable,
    )


@functools.cache
def _arrow_whitespace_pattern() -> str:
    codepoints = [value for value in range(0x110000) if chr(value).isspace()]
    if not codepoints:
        raise VectorPredicateCaptureError("unicode_whitespace_table_empty")
    return "[" + "".join(f"\\x{{{value:X}}}" for value in codepoints) + "]+"


def capture_whitespace_normalization(
    function: types.FunctionType,
) -> WhitespaceNormalizationPlan:
    """Capture regex whitespace collapse followed by argument-free strip."""

    if type(function) is not types.FunctionType:
        raise VectorPredicateCaptureError("function_required")
    node, _first_line = _function_node(function)
    statements = _statements(node)
    input_name = _input_name(node)
    if len(statements) != 1 or not isinstance(statements[0], ast.Return):
        raise VectorPredicateCaptureError("whitespace_shape_unsupported")
    strip_call = statements[0].value
    if (
        not isinstance(strip_call, ast.Call)
        or strip_call.args
        or strip_call.keywords
        or not isinstance(strip_call.func, ast.Attribute)
        or strip_call.func.attr != "strip"
        or not isinstance(strip_call.func.value, ast.Call)
    ):
        raise VectorPredicateCaptureError("whitespace_strip_unsupported")
    sub_call = strip_call.func.value
    if (
        not isinstance(sub_call.func, ast.Attribute)
        or sub_call.func.attr != "sub"
        or not isinstance(sub_call.func.value, ast.Name)
        or len(sub_call.args) != 2
        or sub_call.keywords
        or not isinstance(sub_call.args[0], ast.Constant)
        or sub_call.args[0].value != " "
        or not isinstance(sub_call.args[1], ast.Name)
        or sub_call.args[1].id != input_name
    ):
        raise VectorPredicateCaptureError("whitespace_sub_unsupported")
    regex_name = sub_call.func.value.id
    globals_dict = function.__globals__
    regex_object = globals_dict.get(regex_name, _ABSENT)
    if (
        type(regex_object) is not _RE_PATTERN_TYPE
        or regex_object.pattern != r"\s+"
        or regex_object.flags != re.UNICODE
    ):
        raise VectorPredicateCaptureError("unicode_whitespace_regex_not_exact")
    return WhitespaceNormalizationPlan(
        function=function,
        code=function.__code__,
        globals_dict=globals_dict,
        regex_name=regex_name,
        regex_object=regex_object,
        arrow_pattern=_arrow_whitespace_pattern(),
    )


def _single_character_members(value: object) -> frozenset[str]:
    if type(value) is not frozenset and type(value) is not set:
        raise VectorPredicateCaptureError("join_translate_set_type_unsupported")
    members = frozenset(value)
    if not members or any(
        type(member) is not str or len(member) != 1 for member in members
    ):
        raise VectorPredicateCaptureError("join_translate_set_member_invalid")
    return members


def capture_join_translation(
    function: types.FunctionType,
) -> JoinTranslationPlan:
    """Capture a per-character membership translate inside ``str.join``.

    Recognizes ``"".join(c if c not in SET else " " for c in text)`` where
    ``text`` is the input or its argument-free ``strip()``. The membership
    direction may be mirrored. ``SET`` is proven constant by resolving it to
    a module-level frozenset/set, or to a zero-argument provider guarded by
    ``functools.lru_cache`` whose evaluation yields single characters.
    """

    if type(function) is not types.FunctionType:
        raise VectorPredicateCaptureError("function_required")
    node, _first_line = _function_node(function)
    statements = _statements(node)
    input_name = _input_name(node)
    if not 1 <= len(statements) <= 3 or not isinstance(
        statements[-1], ast.Return
    ):
        raise VectorPredicateCaptureError("join_translate_shape_unsupported")
    strip_input = False
    iter_name = input_name
    set_binding: tuple[str, ast.expr] | None = None
    for statement in statements[:-1]:
        if (
            not isinstance(statement, ast.Assign)
            or len(statement.targets) != 1
            or not isinstance(statement.targets[0], ast.Name)
        ):
            raise VectorPredicateCaptureError("join_translate_statement_unsupported")
        target = statement.targets[0].id
        value = statement.value
        if (
            isinstance(value, ast.Call)
            and not value.args
            and not value.keywords
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "strip"
            and isinstance(value.func.value, ast.Name)
            and value.func.value.id == input_name
        ):
            if strip_input or target == input_name:
                raise VectorPredicateCaptureError("join_translate_strip_unsupported")
            strip_input = True
            iter_name = target
            continue
        if set_binding is not None or target == input_name or target == iter_name:
            raise VectorPredicateCaptureError("join_translate_binding_unsupported")
        set_binding = (target, value)
    set_local: str | None = None
    set_expr: ast.expr | None = None
    if set_binding is not None:
        set_local, set_expr = set_binding

    join_call = statements[-1].value
    if (
        not isinstance(join_call, ast.Call)
        or len(join_call.args) != 1
        or join_call.keywords
        or not isinstance(join_call.func, ast.Attribute)
        or join_call.func.attr != "join"
        or not isinstance(join_call.func.value, ast.Constant)
        or join_call.func.value.value != ""
    ):
        raise VectorPredicateCaptureError("join_translate_join_unsupported")
    generator = join_call.args[0]
    if not isinstance(generator, ast.GeneratorExp) or len(generator.generators) != 1:
        raise VectorPredicateCaptureError("join_translate_generator_unsupported")
    comprehension = generator.generators[0]
    if (
        comprehension.ifs
        or not isinstance(comprehension.target, ast.Name)
        or not isinstance(comprehension.iter, ast.Name)
        or comprehension.iter.id != iter_name
    ):
        raise VectorPredicateCaptureError("join_translate_generator_shape_unsupported")
    element_name = comprehension.target.id

    def _is_element(expression: ast.expr) -> bool:
        return isinstance(expression, ast.Name) and expression.id == element_name

    def _is_space(expression: ast.expr) -> bool:
        return (
            isinstance(expression, ast.Constant)
            and type(expression.value) is str
            and expression.value == " "
        )

    def _membership(test: ast.expr, operator: type[ast.cmpop]) -> str | None:
        if (
            isinstance(test, ast.Compare)
            and len(test.ops) == 1
            and isinstance(test.ops[0], operator)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Name)
            and isinstance(test.left, ast.Name)
            and test.left.id == element_name
        ):
            return test.comparators[0].id
        return None

    conditional = generator.elt
    if not isinstance(conditional, ast.IfExp):
        raise VectorPredicateCaptureError("join_translate_element_unsupported")
    kept_set = _membership(conditional.test, ast.NotIn)
    swapped_set = _membership(conditional.test, ast.In)
    if kept_set is not None and _is_element(conditional.body) and _is_space(
        conditional.orelse
    ):
        set_reference = kept_set
    elif swapped_set is not None and _is_space(
        conditional.body
    ) and _is_element(conditional.orelse):
        set_reference = swapped_set
    else:
        raise VectorPredicateCaptureError("join_translate_element_shape_unsupported")
    if set_reference in {input_name, iter_name, element_name}:
        raise VectorPredicateCaptureError("join_translate_set_reference_invalid")
    if set_local is not None and set_reference != set_local:
        raise VectorPredicateCaptureError("join_translate_set_reference_invalid")

    globals_dict = function.__globals__
    if set_expr is None:
        set_name = set_reference
        set_source = globals_dict.get(set_name, _ABSENT)
        if set_source is _ABSENT:
            raise VectorPredicateCaptureError("join_translate_set_unresolved")
        members = _single_character_members(set_source)
    elif isinstance(set_expr, ast.Name):
        set_name = set_expr.id
        set_source = globals_dict.get(set_name, _ABSENT)
        if set_source is _ABSENT:
            raise VectorPredicateCaptureError("join_translate_set_unresolved")
        members = _single_character_members(set_source)
    elif (
        isinstance(set_expr, ast.Call)
        and not set_expr.args
        and not set_expr.keywords
        and isinstance(set_expr.func, ast.Name)
    ):
        set_name = set_expr.func.id
        set_source = globals_dict.get(set_name, _ABSENT)
        # Only a memoized zero-argument provider may be evaluated during
        # capture; any other callable is rejected without running it.
        if (
            set_source is _ABSENT
            or not callable(set_source)
            or not callable(getattr(set_source, "cache_info", None))
            or not hasattr(set_source, "__wrapped__")
        ):
            raise VectorPredicateCaptureError("join_translate_set_provider_unsupported")
        try:
            members = _single_character_members(set_source())
        except VectorPredicateCaptureError:
            raise
        except Exception as error:
            raise VectorPredicateCaptureError(
                "join_translate_set_evaluation_failed"
            ) from error
    else:
        raise VectorPredicateCaptureError("join_translate_set_source_unsupported")
    codepoints = tuple(sorted(ord(member) for member in members))
    arrow_pattern = (
        "[" + "".join(f"\\x{{{value:X}}}" for value in codepoints) + "]"
    )
    return JoinTranslationPlan(
        function=function,
        code=function.__code__,
        globals_dict=globals_dict,
        set_name=set_name,
        set_source=set_source,
        codepoints=codepoints,
        arrow_pattern=arrow_pattern,
        strip_input=strip_input,
    )


def _cross_engine_regex_safe(pattern: str) -> bool:
    """Accept a deliberately small Python-re/RE2 common language subset."""

    escaped = False
    in_class = False
    for character in pattern:
        if escaped:
            # Escaped ASCII punctuation is literal in both engines. Character
            # classes, anchors, backreferences, and Unicode escapes are not.
            if character.isalnum():
                return False
            escaped = False
            continue
        if character == "\\":
            escaped = True
        elif character == "[":
            if in_class:
                return False
            in_class = True
        elif character == "]":
            if not in_class:
                return False
            in_class = False
        elif ord(character) > 0x7F:
            return False
        elif not in_class and character in {
            "(",
            ")",
            "|",
            "{",
            "}",
            ".",
            "^",
            "$",
            "*",
            "?",
        }:
            return False
    return not escaped and not in_class


def _known_cross_engine_regex_substitution(
    regex_object: object,
    replacement: str,
) -> str | None:
    """Accept only patterns qualified against frozen real and negative data."""

    if type(regex_object) is not _RE_PATTERN_TYPE:
        return None
    return _KNOWN_CROSS_ENGINE_REGEX_SUBSTITUTIONS.get(
        (
            regex_object.pattern,
            regex_object.flags,
            replacement,
        )
    )


def capture_regex_substitution(
    function: types.FunctionType,
) -> RegexSubstitutionPlan:
    """Capture ``COMPILED.sub(literal, exact_string)`` for a safe subset."""

    if type(function) is not types.FunctionType:
        raise VectorPredicateCaptureError("function_required")
    node, _first_line = _function_node(function)
    statements = _statements(node)
    input_name = _input_name(node)
    if len(statements) != 1 or not isinstance(statements[0], ast.Return):
        raise VectorPredicateCaptureError("regex_substitution_shape_unsupported")
    call = statements[0].value
    if (
        not isinstance(call, ast.Call)
        or not isinstance(call.func, ast.Attribute)
        or call.func.attr != "sub"
        or not isinstance(call.func.value, ast.Name)
        or len(call.args) != 2
        or call.keywords
        or not isinstance(call.args[0], ast.Constant)
        or type(call.args[0].value) is not str
        or "\\" in call.args[0].value
        or not isinstance(call.args[1], ast.Name)
        or call.args[1].id != input_name
    ):
        raise VectorPredicateCaptureError("regex_substitution_call_unsupported")
    regex_name = call.func.value.id
    globals_dict = function.__globals__
    regex_object = globals_dict.get(regex_name, _ABSENT)
    if type(regex_object) is not _RE_PATTERN_TYPE or type(
        regex_object.pattern
    ) is not str:
        raise VectorPredicateCaptureError("regex_cross_engine_proof_failed")
    if regex_object.flags == re.UNICODE and _cross_engine_regex_safe(
        regex_object.pattern
    ):
        arrow_pattern = regex_object.pattern
    else:
        arrow_pattern = _known_cross_engine_regex_substitution(
            regex_object,
            call.args[0].value,
        )
    if arrow_pattern is None:
        raise VectorPredicateCaptureError("regex_cross_engine_proof_failed")
    return RegexSubstitutionPlan(
        function=function,
        code=function.__code__,
        globals_dict=globals_dict,
        regex_name=regex_name,
        regex_object=regex_object,
        pattern=regex_object.pattern,
        arrow_pattern=arrow_pattern,
        replacement=call.args[0].value,
    )


def capture_string_transform(
    function: types.FunctionType,
) -> (
    StringTranslationPlan
    | WhitespaceNormalizationPlan
    | JoinTranslationPlan
    | RegexSubstitutionPlan
):
    errors: list[VectorPredicateCaptureError] = []
    for capture in (
        capture_string_translation,
        capture_whitespace_normalization,
        capture_join_translation,
        capture_regex_substitution,
    ):
        try:
            return capture(function)
        except VectorPredicateCaptureError as error:
            errors.append(error)
    raise VectorPredicateCaptureError(
        "string_transform_unsupported:" + ",".join(str(error) for error in errors)
    )

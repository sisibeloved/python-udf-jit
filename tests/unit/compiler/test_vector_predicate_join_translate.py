from __future__ import annotations

import functools
import unittest

from python_udf_jit.compiler.vector_predicate import (
    JoinTranslationPlan,
    VectorPredicateCaptureError,
    capture_join_translation,
    capture_string_transform,
)

_OFFICIAL_STYLE_MEMBERS = frozenset({"\t", " ", " ", "​", "　"})


@functools.lru_cache(maxsize=1)
def _official_style_provider() -> frozenset[str]:
    return frozenset(_OFFICIAL_STYLE_MEMBERS)


def _provider_shape(s: str) -> str:
    text = s.strip()
    spaces = _official_style_provider()
    return "".join(char if char not in spaces else " " for char in text)


def _global_name_shape(s: str) -> str:
    return "".join(
        char if char not in _OFFICIAL_STYLE_MEMBERS else " " for char in s
    )


def _mirrored_shape(s: str) -> str:
    return "".join(
        " " if char in _OFFICIAL_STYLE_MEMBERS else char for char in s
    )


def _plain_provider() -> frozenset[str]:
    return frozenset(_OFFICIAL_STYLE_MEMBERS)


def _plain_provider_shape(s: str) -> str:
    text = s.strip()
    spaces = _plain_provider()
    return "".join(char if char not in spaces else " " for char in text)


def _multi_char_member_set() -> frozenset[str]:
    return frozenset({"ab", " "})


def _multi_char_shape(s: str) -> str:
    return "".join(
        char if char not in _multi_char_member_set() else " " for char in s
    )


def _double_space_shape(s: str) -> str:
    text = s.strip()
    spaces = _official_style_provider()
    return "".join(char if char not in spaces else "  " for char in text)


def _filtered_generator_shape(s: str) -> str:
    text = s.strip()
    spaces = _official_style_provider()
    return "".join(
        char if char not in spaces else " " for char in text if char != "x"
    )


def _membership_target_shape(s: str) -> str:
    text = s.strip()
    spaces = _official_style_provider()
    return "".join(char if char not in text else " " for char in spaces)


class JoinTranslationCaptureTest(unittest.TestCase):
    def test_provider_shape_captures_with_strip(self) -> None:
        plan = capture_join_translation(_provider_shape)
        self.assertIsInstance(plan, JoinTranslationPlan)
        self.assertEqual(plan.kind, "join_translate")
        self.assertTrue(plan.strip_input)
        self.assertEqual(
            plan.codepoints,
            tuple(sorted(ord(member) for member in _OFFICIAL_STYLE_MEMBERS)),
        )
        expected_pattern = (
            "["
            + "".join(
                f"\\x{{{value:X}}}"
                for value in sorted(
                    ord(member) for member in _OFFICIAL_STYLE_MEMBERS
                )
            )
            + "]"
        )
        self.assertEqual(plan.arrow_pattern, expected_pattern)
        self.assertTrue(plan.matches())

    def test_global_name_shape_captures_without_strip(self) -> None:
        plan = capture_join_translation(_global_name_shape)
        self.assertIsInstance(plan, JoinTranslationPlan)
        self.assertFalse(plan.strip_input)
        self.assertTrue(plan.matches())

    def test_mirrored_membership_captures(self) -> None:
        plan = capture_join_translation(_mirrored_shape)
        self.assertIsInstance(plan, JoinTranslationPlan)
        self.assertFalse(plan.strip_input)

    def test_transform_dispatcher_reaches_join_translation(self) -> None:
        plan = capture_string_transform(_provider_shape)
        self.assertIsInstance(plan, JoinTranslationPlan)

    def test_plain_provider_rejected(self) -> None:
        with self.assertRaises(VectorPredicateCaptureError):
            capture_join_translation(_plain_provider_shape)

    def test_multi_char_member_rejected(self) -> None:
        with self.assertRaises(VectorPredicateCaptureError):
            capture_join_translation(_multi_char_shape)

    def test_non_space_replacement_rejected(self) -> None:
        with self.assertRaises(VectorPredicateCaptureError):
            capture_join_translation(_double_space_shape)

    def test_filtered_generator_rejected(self) -> None:
        with self.assertRaises(VectorPredicateCaptureError):
            capture_join_translation(_filtered_generator_shape)

    def test_membership_target_swapped_rejected(self) -> None:
        with self.assertRaises(VectorPredicateCaptureError):
            capture_join_translation(_membership_target_shape)

    def test_matches_detects_provider_replacement(self) -> None:
        plan = capture_join_translation(_provider_shape)
        original = globals()["_official_style_provider"]
        try:
            replacement = functools.lru_cache(maxsize=1)(
                lambda: frozenset({"\t"})
            )
            globals()["_official_style_provider"] = replacement
            self.assertFalse(plan.matches())
        finally:
            globals()["_official_style_provider"] = original
        self.assertTrue(plan.matches())


if __name__ == "__main__":
    unittest.main()

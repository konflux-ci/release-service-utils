"""Unit tests for the Secret class."""

from __future__ import annotations

import pickle

from rsmodels.secret import Secret, unveil


class TestSecretMasking:
    """Tests for Secret value masking in str and repr."""

    def test_str_returns_mask(self) -> None:
        """Test that str() returns the masked value."""
        s = Secret("my-password")
        assert str(s) == "***SECRET***"

    def test_repr_returns_mask(self) -> None:
        """Test that repr() returns the masked value."""
        s = Secret("my-password")
        assert repr(s) == "***SECRET***"

    def test_named_secret_str(self) -> None:
        """Test that str() includes the name in the mask."""
        s = Secret("my-password", name="db_pass")
        assert str(s) == "***SECRET:db_pass***"

    def test_named_secret_repr(self) -> None:
        """Test that repr() includes the name in the mask."""
        s = Secret("my-password", name="db_pass")
        assert repr(s) == "***SECRET:db_pass***"

    def test_fstring_masks_value(self) -> None:
        """Test that f-string interpolation masks the value."""
        s = Secret("my-password")
        result = f"Token: {s}"
        assert "my-password" not in result
        assert "***SECRET***" in result

    def test_fstring_repr_masks_value(self) -> None:
        """Test that f-string !r interpolation masks the value."""
        s = Secret("my-password")
        result = f"Token: {s!r}"
        assert "my-password" not in result
        assert "***SECRET***" in result

    def test_format_masks_value(self) -> None:
        """Test that str.format masks the value."""
        s = Secret("my-password")
        result = "Token: {}".format(s)
        assert "my-password" not in result

    def test_list_str_masks_value(self) -> None:
        """Test that Secret is masked inside a list str()."""
        cmd = ["curl", "-H", Secret("Bearer token123")]
        result = str(cmd)
        assert "token123" not in result
        assert "***SECRET***" in result

    def test_list_repr_masks_value(self) -> None:
        """Test that Secret is masked inside a list repr()."""
        cmd = ["curl", "-H", Secret("Bearer token123")]
        result = repr(cmd)
        assert "token123" not in result
        assert "***SECRET***" in result

    def test_no_name_attribute_still_masks(self) -> None:
        """Test masking still works if _name attribute is missing."""
        s = Secret("my-password")
        del s._name
        assert str(s) == "***SECRET***"
        assert repr(s) == "***SECRET***"


class TestSecretUnveil:
    """Tests for accessing the actual secret value."""

    def test_unveil_returns_actual_value(self) -> None:
        """Test that unveil() returns the real secret."""
        s = Secret("my-password")
        assert s.unveil() == "my-password"

    def test_module_unveil_with_secret(self) -> None:
        """Test the module-level unveil() with a Secret."""
        s = Secret("my-password")
        assert unveil(s) == "my-password"

    def test_module_unveil_with_plain_string(self) -> None:
        """Test the module-level unveil() with a plain string."""
        assert unveil("plain-text") == "plain-text"


class TestSecretIsStr:
    """Tests that Secret behaves as a str subclass."""

    def test_isinstance_str(self) -> None:
        """Test that Secret is an instance of str."""
        s = Secret("hello")
        assert isinstance(s, str)

    def test_equality_uses_actual_value(self) -> None:
        """Test that equality comparison uses the actual value."""
        s = Secret("hello")
        assert s == "hello"

    def test_len_uses_actual_value(self) -> None:
        """Test that len() operates on the actual value."""
        s = Secret("hello")
        assert len(s) == 5

    def test_contains_uses_actual_value(self) -> None:
        """Test that 'in' operator uses the actual value."""
        s = Secret("hello world")
        assert "world" in s


class TestSecretPickle:
    """Tests for Secret pickling support."""

    def test_pickle_roundtrip(self) -> None:
        """Test that Secret survives pickle/unpickle."""
        s = Secret("my-password")
        restored = pickle.loads(pickle.dumps(s))
        assert isinstance(restored, Secret)
        assert restored.unveil() == "my-password"
        assert str(restored) == "***SECRET***"

    def test_pickle_named_roundtrip(self) -> None:
        """Test that named Secret survives pickle/unpickle."""
        s = Secret("my-password", name="db_pass")
        restored = pickle.loads(pickle.dumps(s))
        assert isinstance(restored, Secret)
        assert restored.unveil() == "my-password"
        assert str(restored) == "***SECRET:db_pass***"

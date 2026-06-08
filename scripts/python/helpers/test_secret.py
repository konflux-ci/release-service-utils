"""Unit tests for the Secret class."""

import pickle

from rsmodels.secret import Secret, unveil


class TestSecret:
    """Tests for the Secret class."""

    def test_create_secret_without_name(self):
        """Test creating a Secret without a name."""
        secret = Secret("my-secret-value")
        assert isinstance(secret, str)
        assert isinstance(secret, Secret)

    def test_create_secret_with_name(self):
        """Test creating a Secret with a name."""
        secret = Secret("my-secret-value", name="api_token")
        assert isinstance(secret, Secret)

    def test_repr_masks_value_without_name(self):
        """Test that repr() masks the secret value when no name is provided."""
        secret = Secret("my-secret-value")
        assert repr(secret) == "***SECRET***"

    def test_repr_masks_value_with_name(self):
        """Test that repr() includes the name when provided."""
        secret = Secret("my-secret-value", name="api_token")
        assert repr(secret) == "***SECRET:api_token***"

    def test_str_masks_value_without_name(self):
        """Test that str() masks the secret value when no name is provided."""
        secret = Secret("my-secret-value")
        assert str(secret) == "***SECRET***"

    def test_str_masks_value_with_name(self):
        """Test that str() includes the name when provided."""
        secret = Secret("my-secret-value", name="api_token")
        assert str(secret) == "***SECRET:api_token***"

    def test_unveil_returns_actual_value(self):
        """Test that unveil() returns the actual secret value."""
        secret = Secret("my-secret-value")
        assert secret.unveil() == "my-secret-value"

    def test_unveil_returns_actual_value_with_name(self):
        """Test that unveil() returns the actual value even with a name."""
        secret = Secret("my-secret-value", name="api_token")
        assert secret.unveil() == "my-secret-value"

    def test_format_with_repr_spec_masks_value(self):
        """Test that f-string with !r masks the value."""
        secret = Secret("my-secret-value")
        formatted = f"Token: {secret!r}"
        assert formatted == "Token: ***SECRET***"

    def test_format_with_repr_spec_masks_value_with_name(self):
        """Test that f-string with !r shows the name."""
        secret = Secret("my-secret-value", name="github_token")
        formatted = f"Token: {secret!r}"
        assert formatted == "Token: ***SECRET:github_token***"

    def test_in_list_repr_masks_value(self):
        """Test that secret is masked when printed in a list."""
        secret = Secret("my-secret-value")
        cmd = ["curl", "-H", f"Authorization: Bearer {secret}"]
        assert "***SECRET***" in repr(cmd)
        assert "my-secret-value" not in repr(cmd)

    def test_pickle_and_unpickle_without_name(self):
        """Test that Secret can be pickled and unpickled without name."""
        secret = Secret("my-secret-value")
        pickled = pickle.dumps(secret)
        unpickled = pickle.loads(pickled)

        assert isinstance(unpickled, Secret)
        assert unpickled.unveil() == "my-secret-value"
        assert repr(unpickled) == "***SECRET***"

    def test_pickle_and_unpickle_with_name(self):
        """Test that Secret can be pickled and unpickled with name."""
        secret = Secret("my-secret-value", name="api_token")
        pickled = pickle.dumps(secret)
        unpickled = pickle.loads(pickled)

        assert isinstance(unpickled, Secret)
        assert unpickled.unveil() == "my-secret-value"
        assert repr(unpickled) == "***SECRET:api_token***"

    def test_works_as_string_in_string_operations(self):
        """Test that Secret works like a normal string in operations."""
        secret = Secret("my-secret-value")

        # String concatenation using unveil
        result = "prefix-" + secret.unveil()
        assert result == "prefix-my-secret-value"

        # Length check via unveil
        assert len(secret.unveil()) == len("my-secret-value")

    def test_comparison_with_unveiled_value(self):
        """Test that unveiled secret can be compared."""
        secret = Secret("my-secret-value")
        assert secret.unveil() == "my-secret-value"

    def test_empty_secret(self):
        """Test creating a Secret with an empty string."""
        secret = Secret("")
        assert secret.unveil() == ""
        assert repr(secret) == "***SECRET***"

    def test_secret_with_special_characters(self):
        """Test Secret with special characters."""
        special_value = "secret!@#$%^&*()_+-={}[]|:;<>?,./~`"
        secret = Secret(special_value)
        assert secret.unveil() == special_value
        assert repr(secret) == "***SECRET***"

    def test_secret_name_with_special_characters(self):
        """Test Secret name with special characters."""
        secret = Secret("value", name="token-123_abc")
        assert repr(secret) == "***SECRET:token-123_abc***"

    def test_print_does_not_reveal_secret(self, capsys):
        """Test that print() does not reveal the secret."""
        secret = Secret("my-secret-value")
        print(secret)
        captured = capsys.readouterr()
        assert "***SECRET***" in captured.out
        assert "my-secret-value" not in captured.out

    def test_logging_does_not_reveal_secret(self, capsys):
        """Test that logging format does not reveal the secret."""
        secret = Secret("my-secret-value", name="token")
        print(f"Using secret: {secret}")
        captured = capsys.readouterr()
        assert "***SECRET:token***" in captured.out
        assert "my-secret-value" not in captured.out


class TestUnveilFunction:
    """Tests for the module-level unveil() function."""

    def test_unveil_with_secret_instance(self):
        """Test unveil() with a Secret instance."""
        secret = Secret("my-secret-value")
        result = unveil(secret)
        assert result == "my-secret-value"

    def test_unveil_with_regular_string(self):
        """Test unveil() with a regular string returns it unchanged."""
        regular_string = "not-a-secret"
        result = unveil(regular_string)
        assert result == "not-a-secret"

    def test_unveil_with_secret_with_name(self):
        """Test unveil() with a named Secret."""
        secret = Secret("my-secret-value", name="token")
        result = unveil(secret)
        assert result == "my-secret-value"

    def test_unveil_with_empty_secret(self):
        """Test unveil() with an empty Secret."""
        secret = Secret("")
        result = unveil(secret)
        assert result == ""

    def test_unveil_with_empty_string(self):
        """Test unveil() with an empty regular string."""
        result = unveil("")
        assert result == ""

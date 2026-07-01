"""Fake implementations for testing."""

from fake.skopeo import FakeSkopeoClient


def patch_skopeo_client() -> None:
    """Monkey-patch skopeo.SkopeoClient with FakeSkopeoClient.

    This function replaces the real SkopeoClient with the fake implementation.
    Must be called before importing any modules that use SkopeoClient.
    """
    import skopeo

    skopeo.SkopeoClient = FakeSkopeoClient


__all__ = ["FakeSkopeoClient", "patch_skopeo_client"]

from find_matching_purl import find_matching_purl


def test_find_matching_purl_success():
    """Test finding a matching PURL in the data."""
    test_data = [
        {
            "purl": "pkg:generic/test1@1.0.0?repository_url=repo1",
            "impact": "high",
        },
        {
            "purl": "pkg:generic/test2@2.0.0?repository_url=repo2",
            "impact": "medium",
        },
    ]

    result = find_matching_purl(test_data, "repo1")
    assert result == "high"


def test_find_matching_purl_first_match():
    """Test that the first matching item is returned."""
    test_data = [
        {
            "purl": "pkg:generic/test1@1.0.0?repository_url=repo1",
            "impact": "first",
        },
        {
            "purl": "pkg:generic/test2@2.0.0?repository_url=repo1",
            "impact": "second",
        },
    ]

    result = find_matching_purl(test_data, "repo1")
    assert result == "first"


def test_find_matching_purl_no_match():
    """Test when no matching PURL is found."""
    test_data = [
        {
            "purl": "pkg:generic/test1@1.0.0?repository_url=repo1",
            "impact": "high",
        },
    ]

    result = find_matching_purl(test_data, "repo2")
    assert result is None


def test_find_matching_purl_empty_data():
    """Test with empty data list."""
    test_data = []

    result = find_matching_purl(test_data, "repo1")
    assert result is None


def test_find_matching_purl_missing_purl_key():
    """Test handling items missing the 'purl' key."""
    test_data = [
        {"impact": "high"},
        {
            "purl": "pkg:generic/test1@1.0.0?repository_url=repo1",
            "impact": "medium",
        },
    ]

    result = find_matching_purl(test_data, "repo1")
    assert result == "medium"


def test_find_matching_purl_invalid_purl():
    """Test handling items with invalid PURL strings."""
    test_data = [
        {"purl": "invalid-purl", "impact": "high"},
        {
            "purl": "pkg:generic/test1@1.0.0?repository_url=repo1",
            "impact": "medium",
        },
    ]

    result = find_matching_purl(test_data, "repo1")
    assert result == "medium"


def test_find_matching_purl_with_multiple_qualifiers():
    """Test parsing a PURL with multiple qualifiers."""
    test_data = [
        {
            "purl": "pkg:generic/test@1.0.0?repository_url=repo1&checksum=abc123",
            "impact": "high",
        },
    ]

    result = find_matching_purl(test_data, "repo1")
    assert result == "high"


def test_find_matching_purl_without_repository_url():
    """Test parsing a PURL without repository_url qualifier."""
    test_data = [
        {
            "purl": "pkg:generic/test@1.0.0",
            "impact": "high",
        },
        {
            "purl": "pkg:generic/test2@2.0.0?repository_url=repo1",
            "impact": "medium",
        },
    ]

    result = find_matching_purl(test_data, "repo1")
    assert result == "medium"

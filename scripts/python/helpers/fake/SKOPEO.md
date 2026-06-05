# Fake Skopeo Client Implementation Summary

## What Was Implemented

A complete fake/mock implementation of `SkopeoClient` for testing purposes, based on the design discussion.

## Files Created

1. **`scripts/python/helpers/fake/__init__.py`**
   - Exports `FakeSkopeoClient` and `patch_skopeo_client()`
   - The `patch_skopeo_client()` function performs monkey-patching

2. **`scripts/python/helpers/fake/skopeo.py`**
   - `FakeSkopeoClient` class - main implementation
   - Loads YAML config from `RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP` env var
   - Implements `inspect()` and `copy()` with full method signatures
   - Supports regex matching, first-match-wins rule evaluation
   - Validates config at load time
   - Raises `SkopeoClientError` when no match found

3. **`scripts/python/helpers/fake/README.md`**
   - Comprehensive documentation
   - Usage examples
   - YAML format reference
   - Troubleshooting guide

4. **`scripts/python/helpers/fake/example_config.yaml`**
   - Demonstrates all features: exact matching, regex, success/failure cases
   - Ready-to-use examples for common test scenarios

5. **`scripts/python/helpers/fake/test_fake_skopeo.py`**
   - 13 unit tests covering all functionality
   - All tests passing
   - Validates load-time checks, runtime matching, error handling

6. **`scripts/python/helpers/fake/example_test.sh`**
   - Working end-to-end example using bash wrapper
   - Demonstrates integration with `publish_index_image`
   - Shows how to use in Tekton/CI tests

## Key Design Decisions

### Activation Mechanism
- **Environment variable**: `RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP` points to YAML config
- **Monkey patching**: `patch_skopeo_client()` replaces real client before imports
- **Drop-in replacement**: Same constructor and method signatures as `SkopeoClient`

### YAML Structure
- Top-level operations: `inspect`, `copy`
- Each operation has list of rules with `match` and optional `return`
- First matching rule wins (top-to-bottom evaluation)

### Matching Logic
- Only fields specified in `match` must match
- Extra parameters in actual calls are ignored
- Secrets are always ignored in matching
- `None` values don't match specified patterns
- Regex support via `{regex: "pattern"}` with fullmatch semantics

### Validation
- **Load-time validation**: YAML syntax, rule structure, return types
- **Runtime validation**: Regex patterns applied during matching
- **Error messages**: Detailed, showing attempted params and config file path

### Return Value Handling

**For `inspect()`:**
- `format` specified in match → return must be string
- No `format` in match → return must be dict
- Auto-detected based on match rule

**For `copy()`:**
- Omit `return` section → success (returns `None`)
- `return: {success: true}` → explicit success
- `return: {success: false, ...}` → raises `SkopeoClientError`
- Default values: `returncode=1`, `stdout=""`, `stderr=""`

## Usage Pattern for Tests

```bash
# 1. Create mock config
cat > mock-config.yaml <<EOF
inspect:
  - match:
      image: "docker://quay.io/target:tag"
      format: "{{.Digest}}"
    return: "sha256:different"

copy:
  - match:
      source: "docker://quay.io/source@sha256:abc"
      destination: "docker://quay.io/target:tag"
EOF

# 2. Set environment variable
export RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP=/path/to/mock-config.yaml

# 3. Create bash wrapper
publish_index_image() {
    python3 - "$@" <<'PYTHON'
import sys
sys.path.insert(0, '/path/to/helpers')
sys.path.insert(0, '/path/to/tasks/internal')

from fake import patch_skopeo_client
patch_skopeo_client()

from publish_index_image import main
sys.exit(main())
PYTHON
}

# 4. Run your test
publish_index_image --source-index "..." --target-index "..."
```

## Testing the Implementation

```bash
# Run unit tests
pytest scripts/python/helpers/fake/test_fake_skopeo.py -v

# Run integration example
./scripts/python/helpers/fake/example_test.sh
```

## What's NOT Included

Based on our design discussion, the following were explicitly excluded:

- Multiple config file support (only single file)
- Special debug/strict/recording modes
- Validation of dict field names (type-only validation)
- Matching on constructor parameters
- Matching on credential values
- Template parsing for `format` parameter (returns pre-formatted strings)

These can be added later if needed without breaking existing configs.

## Next Steps

1. **In your other project**: Create test YAML configs specific to your test scenarios
2. **Update CI/Tekton tasks**: Use the bash wrapper pattern to inject fake client
3. **Write tests**: Use `example_test.sh` as a template for your specific test cases
4. **Iterate**: Add more rules to your YAML configs as you encounter new test scenarios


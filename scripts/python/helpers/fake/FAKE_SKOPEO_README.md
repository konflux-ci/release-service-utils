# Fake Skopeo Client for Testing

Fake skopeo allows you to mock skopeo operations (`inspect`, `copy`) by defining expected responses in a YAML configuration file.

## Quick Start

### 1. Create a mock configuration file

```yaml
inspect:
  - match:
      image: "docker://quay.io/source/image@sha256:abc123"
      format: "{{.Digest}}"
    return: "sha256:abc123"

copy:
  - match:
      source: "docker://quay.io/source/image@sha256:abc123"
      destination: "docker://quay.io/dest/image:tag"
    # No return = success
```

### 2. Use the fake client in tests

#### Option A: Using bash wrapper (for Tekton tests)

```bash
# In your test setup, create a bash function that intercepts the script call
publish_index_image() {
    python3 -c "
import sys
sys.argv[0] = 'publish_index_image'
sys.path.insert(0, '/path/to/scripts/python/helpers')
sys.path.insert(0, '/path/to/scripts/python/tasks/internal')

# Patch BEFORE importing publish_index_image
from fake import patch_skopeo_client
patch_skopeo_client()

# Now import and run
from publish_index_image import main
sys.exit(main())
" "\$@"
}

# Set the config file location
export RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP=/path/to/mock-config.yaml

# Run your test
publish_index_image \
  --source-index "quay.io/source/image@sha256:abc123" \
  --target-index "quay.io/dest/image:tag" \
  --retries 3 \
  --source-credential-path /path/to/src-cred \
  --target-credential-path /path/to/dest-cred
```

#### Option B: Direct Python usage

```python
import os
os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = "/path/to/config.yaml"

from fake import patch_skopeo_client
patch_skopeo_client()

# Now any code that imports SkopeoClient gets the fake version
from publish_index_image import main
main()
```

## YAML Configuration Format

### Top-level structure

```yaml
inspect:
  - match: {...}
    return: ...
  - match: {...}
    return: ...

copy:
  - match: {...}
    return: ...
```

### Inspect rules

#### With format parameter (returns string)

```yaml
inspect:
  - match:
      image: "docker://quay.io/image:tag"
      format: "{{.Digest}}"
    return: "sha256:abc123"
```

#### Without format parameter (returns dict)

```yaml
inspect:
  - match:
      image: "docker://quay.io/image:tag"
    return:
      Digest: "sha256:abc123"
      Name: "quay.io/image"
      RepoTags: ["v1.0", "latest"]
```

#### Using regex

```yaml
inspect:
  - match:
      image:
        regex: "docker://quay.io/.*@sha256:[a-f0-9]{64}"
      format: "{{.Digest}}"
    return: "sha256:0123456789abcdef..."
```

### Copy rules

#### Success (no return section)

```yaml
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
    # Omit return for success
```

#### Explicit success

```yaml
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
    return:
      success: true
```

#### Failure

```yaml
copy:
  - match:
      source: "docker://quay.io/bad:tag"
      destination: "docker://quay.io/dest:tag"
    return:
      success: false
      stderr: "Error: manifest unknown"
      returncode: 1  # optional, defaults to 1
```

## Matching behavior

### Field matching

- Only fields specified in the `match` section need to match
- Extra parameters in the actual call are ignored
- `None` values don't match specified patterns
- Credentials (`Secret` objects) are always ignored in matching

### Match order

- Rules are evaluated top-to-bottom
- **First matching rule wins**
- Put specific rules before generic catch-all rules

### Regex matching

- Use `{regex: "pattern"}` syntax for regex fields
- Regex must match the **entire string** (`re.fullmatch`)
- Invalid regex patterns cause load-time errors

## Validation

The fake client validates configuration at load time:

- YAML syntax must be valid
- Each rule must have a `match` section
- `inspect` rules with `format` must return strings
- `inspect` rules without `format` must return dicts
- `copy` rules must return dicts (if return section exists)
- Operation names must be valid (`inspect`, `copy`)

## Error handling

When no rule matches, the fake client raises `SkopeoClientError` with:
- Error message: "No mock match found"
- Attempted parameters shown in stderr
- Reference to the config file path

Example error:
```
MOCK ERROR: No matching rule found for inspect()
Attempted with:
  image: "docker://quay.io/actual:tag"
  format: "{{.Digest}}"
Config file: /path/to/mock-config.yaml
```

## Environment Variables

- **`RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP`** (required): Path to YAML config file

## Running Tests

```bash
# Install test dependencies
pip install pytest pyyaml

# Run the tests
pytest fake/test_fake_skopeo.py -v
```

## Example Use Cases

### Testing successful publish

```yaml
inspect:
  - match:
      image: "docker://quay.io/target/image:tag"
      format: "{{.Digest}}"
    return: "sha256:different"  # Different from source

copy:
  - match:
      source: "docker://quay.io/source/image@sha256:abc123"
      destination: "docker://quay.io/target/image:tag"
```

### Testing idempotent publish (same digest)

```yaml
inspect:
  - match:
      image: "docker://quay.io/target/image:tag"
      format: "{{.Digest}}"
    return: "sha256:abc123"  # Same as source - should skip copy
```

### Testing copy failure

```yaml
inspect:
  - match:
      image: "docker://quay.io/target/image:tag"
      format: "{{.Digest}}"
    return: "sha256:different"

copy:
  - match:
      source: "docker://quay.io/source/image@sha256:abc123"
      destination: "docker://quay.io/target/image:tag"
    return:
      success: false
      stderr: "Error: authentication required"
```

## See Also

- `example_config.yaml` - Comprehensive example configuration
- `test_fake_skopeo.py` - Test suite demonstrating usage

# Framework Architecture

Class-based framework for tasks and helpers to enable dependency tracking and smart test selection.

## Helper Classes

Helpers encapsulate reusable functionality (API clients, data processing, subprocess wrappers). All helpers inherit from `Helper` or `BinaryHelper`:

```python
from release_service_utils.framework import Helper, BinaryHelper

class FileHelper(Helper):
    """File and path operations."""
    
    def load_json_dict(self, path: Path) -> dict[str, Any]:
        """Load JSON file whose root must be an object."""
        ...

class GitHelper(BinaryHelper):
    """Git CLI wrapper."""
    
    bin = "git"  # Binary this helper wraps (for dependency tracking)
    
    def clone(self, parent_dir: Path, clone_url: str, ...) -> Path:
        """Clone a repository."""
        ...
```

**Key points:**
- Use `Helper` for general helpers (API clients, data processors)
- Use `BinaryHelper` for CLI wrappers (git, skopeo, yq) — requires `bin` attribute
- Helpers are stateless — all state passed as arguments
- One singleton instance per helper for backward compatibility

**Backward compatibility:**
Every helper module exports its methods as module-level functions via a singleton:

```python
# In src/helpers/file/file.py
class FileHelper(Helper):
    def load_json_dict(self, path: Path) -> dict[str, Any]:
        ...

# Singleton + exports for backward compatibility
_helper = FileHelper()
load_json_dict = _helper.load_json_dict

# Old code still works:
from release_service_utils.helpers.file import load_json_dict
result = load_json_dict(path)

# New code can use class:
from release_service_utils.helpers.file import FileHelper
helper = FileHelper()
result = helper.load_json_dict(path)
```

## Task Classes

Tasks declare helper dependencies via class attributes. Three task types exist:

```python
from release_service_utils.framework import Task, InternalTask, ManagedTask
from release_service_utils.helpers.file import FileHelper
from release_service_utils.helpers.vcs import GitHelper

class CreateAdvisoryTask(InternalTask):
    """Internal task for creating advisories."""
    
    # Declare helper dependencies
    helpers = {
        "file": FileHelper,
        "git": GitHelper,
    }
    
    # Declare Tekton result file env vars
    results = ["RESULT_RESULT", "RESULT_ADVISORY_URL"]
    
    def run(self) -> None:
        """Execute task logic."""
        # Access helpers via self.helpers
        data = self.helpers.file.load_json_dict(path)
        self.helpers.git.clone(parent_dir, url)
        
        # Access results via self.results
        self.results.RESULT_RESULT.write_text("success")

class RequestAdvisoryCreationTask(ManagedTask):
    """Managed task that spawns internal tasks."""
    
    helpers = {"file": FileHelper}
    
    # Declare which internal tasks this can spawn
    internal_tasks = [CreateAdvisoryTask]
    
    def run(self) -> None:
        """Execute task logic."""
        self.create_internal_task(CreateAdvisoryTask, params={...})
```

**Task types:**
- `Task` — Base class, rarely used directly
- `InternalTask` — Auto-catches exceptions, writes errors to Tekton results
- `ManagedTask` — Validates `create_internal_task()` calls against `internal_tasks`

**Benefits:**
- Dependency tracking via `helpers` dict → enables smart test selection
- Binary tracking via `BinaryHelper.bin` → map Dockerfile changes to affected tasks
- Type safety and IDE autocomplete for helper access
- Framework validates internal task spawning

## Converting to Framework

**Converting a helper:**

1. Create helper class inheriting from `Helper` (or `BinaryHelper` if wrapping CLI)
2. Move functions as methods (keep signatures/docstrings)
3. Add singleton instance at bottom
4. Export methods as module-level functions
5. Update `__init__.py` to export the class

**Converting a task:**

1. Create task class inheriting from `InternalTask` or `ManagedTask`
2. Declare `helpers = {"name": HelperClass}` for dependencies
3. Declare `results = ["ENV_VAR_NAME"]` for Tekton results (InternalTask only)
4. Declare `internal_tasks = [TaskClass]` for spawnable tasks (ManagedTask only)
5. Move logic into `run(self)` method
6. Replace helper calls: `helper.func()` → `self.helpers.name.func()`
7. Replace result access: `Path(os.environ["RESULT_*"])` → `self.results.RESULT_*`

**Example conversion:**

Before (function-based):
```python
from release_service_utils.helpers.file import load_json_dict
from release_service_utils.helpers.vcs import clone

def main():
    data = load_json_dict(Path("input.json"))
    clone(Path("/tmp"), "https://github.com/org/repo")
```

After (class-based):
```python
from release_service_utils.framework import InternalTask
from release_service_utils.helpers.file import FileHelper
from release_service_utils.helpers.vcs import GitHelper

class MyTask(InternalTask):
    helpers = {"file": FileHelper, "git": GitHelper}
    results = ["RESULT_OUTPUT"]
    
    def run(self):
        data = self.helpers.file.load_json_dict(Path("input.json"))
        self.helpers.git.clone(Path("/tmp"), "https://github.com/org/repo")
        self.results.RESULT_OUTPUT.write_text("done")

# Backward compat entry point
def main():
    task = MyTask()
    task.run()
```

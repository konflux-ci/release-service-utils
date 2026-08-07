# Contributing

Contributions of all kinds are welcome. In particular, pull requests are appreciated. The authors and maintainers will endeavor to help walk you through any issues in the pull request discussion, so please feel free to open a pull request even if you are new to such things.

## Code of Conduct

Our [company values](https://www.redhat.com/en/open-culture-guide) guide us in our day-to-day interactions and decision-making. Our open source projects are no exception and they will define the standards for how to engage with the project through a code of conduct.

Please, make sure you read both of them before contributing, so you can help us to maintain a healthy community.

## Requesting Support

Before you ask a question, it is best to search for existing Issues that might help you. In case you have found a suitable issue and still need clarification, you can write your question in this issue. It is also advisable to search the internet for answers first.

If you then still feel the need to ask a question and need clarification, we recommend the following:

* Open an [Issue](/issues/new).
* Provide as much context as you can about what you're running into.
* Provide project and platform versions (Python, uv, container runtime, etc), depending on what seems relevant.
* The community will then take care of the issue as soon as possible.

## Reporting Bugs

We use GitHub issues to track bugs and errors. If you run into an issue with the project:

* Open an [Issue](/issues/new).
* Explain the behavior you would expect and the actual behavior.
* Please provide as much context as possible and describe the reproduction steps that someone else can follow to recreate the issue on their own. This usually includes your code. For good bug reports you should isolate the problem and create a reduced test case.

Once it's filed:

* The project team will label the issue accordingly.
* A team member will try to reproduce the issue with your provided steps. If there are no reproduction steps or no obvious way to reproduce the issue, the team will ask you for those steps and mark the issue as `needs-reproducer`. Bugs with this tag will not be addressed until they are reproduced.
* If the team is able to reproduce the issue, it will be marked `needs-fix` and left to be implemented by someone. Other labels can be used in addition to better describe the issue or its criticality.

## Requesting a Feature

Enhancement suggestions are tracked as [GitHub issues](/issues).

- Use a **clear and descriptive title** for the issue to identify the suggestion.
- Provide a **step-by-step description of the suggested enhancement** in as many details as possible.
- Describe the current behavior, the expected one, and why you expect this behavior. At this point you can also list which alternatives do not work for you.
- **Explain why this enhancement would be useful** to other users. You may also want to point out the other projects that solved it better and could serve as inspiration.

## Submitting Changes

Before contributing code or documentation to this project, make sure you read the following sections.

### Commit Message Formatting and Standards

The project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification and enforces it using [gitlint](https://jorisroovers.com/gitlint/). The rules for this project are specified in the [.gitlint](.gitlint) config file. There is also a second rule file for the commit description that can be found in the [.github/gitlint directory](.github/gitlint).

The commit message should contain an overall explanation about the change and the motivation behind it. Please note that mentioning a Jira ticket ID or a GitHub issue, isn't a replacement for that.

**All commits must be cryptographically signed** using `git commit -S` or equivalent.
**All AI assisted commit must be disclosed** using a commit footer like `Assisted-By: Cursor` or `Assisted-By: Claude`.

A well formatted commit would look something like this:

```
feat(JIRA-123): add retry logic to pyxis client

Add exponential backoff retry logic to handle transient failures
when calling Pyxis API endpoints.

Assisted-By: Cursor
Signed-off-by: Your Name <your-name@your-email.com>
```

### Pull Request Title Prefixes

The title prefix should be one of (`chore`|`docs`|`feat`|`fix`|`refactor`|`revert`|`style`|`test`) followed by a colon (`:`) and lowercase title. Optionally, you can include a Jira key.

Examples:

- fix(RELEASE-1234): handle null response from pyxis
- feat: add graphql query support to pyxis wrapper

Title prefixes:

- **chore**: Changes that do not modify functionality (e.g., tool updates, or maintenance tasks).
- **docs**: Documentation updates or additions (e.g., README changes, inline comments).
- **feat**: Introduction of a new feature or functionality.
- **fix**: Bug fixes or corrections to existing functionality.
- **refactor**: Code changes that improve structure or readability without altering functionality.
- **revert**: Reverting a previous commit or pull request.
- **style**: Code formatting or stylistic changes that do not affect functionality (e.g., whitespace, linting).
- **test**: Adding or updating tests (e.g., unit tests, integration tests).

### Pull Requests

All changes must come from a pull request (PR) and cannot be directly committed. While anyone can engage in activity on a PR, pull requests are only approved by team members.

Before a pull request can be merged:

* The content of the PR has to be relevant to the PR itself
* The contribution must follow the style guidelines of this project (see Python Style Guide below)
* Multiple commits should be used if the PR is complex and clarity can be improved, but they should still relate to a single topic
* For code contributions, tests have to be added/modified to ensure the code works
* There has to be at least one approval
* Limit commits to the changes needed for the work and its tests, and split other edits into their own pull requests or commits
* If an AI agent was used in the creation of the commit, show this with a commit footer like `Assisted-By: Cursor` or `Assisted-By: Claude`
* The CI has to pass successfully
* Every comment has to be addressed and resolved

## Python Style Guide

This project follows specific Python coding standards:

### Code Style

- **Python version**: 3.12+ required
- **Type annotations**: Required on all public functions and methods, optional on private ones 
- **Docstrings**: Required on every module and public function (triple-double-quote, imperative mood)
- **Formatting**: Black formatter with line length 95
- **Linting**: Flake8 with line length 95, E203 ignored
- **Ruff docstring linting**: D rules enforced on changed files

### Python Patterns

- **Logging**: Named loggers using `LOGGER = logging.getLogger("module_name")`
- **CLI arguments**: Use argparse for command-line arguments; env vars for Tekton-injected config
- **Entry point**: Use `if __name__ == "__main__": raise SystemExit(main())`
- **Exception chaining**: Always use `raise ... from e`
- **Credentials**: File-based credentials via mounted secrets, not bare env vars
- **Retries**: Use `retry.retry_with_exponential_backoff()` from helpers
- **HTTP requests**: Use `http_client.get_text()` from helpers (includes retries, 429/404 backoff)
- **Tekton results**: Use `tekton.result_paths_from_env()` to read env var paths

### File Organization

- **Helpers**: Cross-cutting helpers go in `src/helpers/`
- **Task scripts**: Task implementations go in `src/tasks/internal/` and `src/tasks/managed/`
- **Utilities**: General utilities go in `utils/`
- **Templates**: Jinja2 templates go in `templates/`

### Data Keys Schema

This repository maintains a json schema for the data struct used in various scripts in this repo. It is stored [here](schemas/dataKeys.json).

If your change adds or removes a key to the data struct, the schema must be updated accordingly as part of your pull request.

## Testing

### Running Tests Locally

```bash
# Install dependencies
uv sync --dev

# Run unit tests with coverage
uv run pytest --cov=. --cov-report=xml:coverage.xml

# Run black formatter check
black --check --line-length 95 .

# Run flake8 linter
flake8 --max-line-length 95 --extend-ignore E203 .

# Run ruff docstring check on specific files
ruff check --select D path/to/file.py
```

### Test Organization

- **Unit tests**: Co-located with source files (e.g., `pyxis/test_pyxis.py` next to `pyxis/pyxis.py`; exception: `utils/` has tests in `utils/tests/`)
- **Integration tests**: Located in `integration-tests/` and not run by pytest
- **Test function names**: `test_<what_is_being_tested>`

### Test Writing Guidelines

- Use `pytest` with `pytest-cov` for coverage
- Aim for high test coverage on new code

### Integration Tests

Integration tests in `integration-tests/` detect which catalog E2E suites are affected by utils changes, patch a temporary catalog fork with the new image, and run the matching tests. These are triggered automatically in CI.

## Development Workflow

1. **Fork and clone** the repository
2. **Create a branch** for your changes
3. **Set up your environment**:
   ```bash
   uv sync --dev
   ```
4. **Make your changes** following the style guide
5. **Add tests** for your changes
6. **Run tests locally**:
   ```bash
   uv run pytest --cov=.
   black --check --line-length 95 .
   flake8 --max-line-length 95 --extend-ignore E203
   ```
7. **Commit your changes** with proper commit message format and signing
8. **Push to your fork** and open a pull request
9. **Address review feedback** and ensure CI passes

## CI Checks

All pull requests must pass the following CI checks:

- **Black Lint**: Code formatting with Black (line length 95)
- **Flake8 Lint**: Code linting with Flake8 (line length 95, E203 ignored)
- **pytest with coverage**: Unit tests with code coverage reporting
- **Ruff docstring check**: Docstring linting on changed Python files
- **gitlint**: Commit message format validation

## Getting Help

If you need help or have questions:

- Check existing [Issues](https://github.com/konflux-ci/release-service-utils/issues)
- Open a new issue with the `question` label
- Reach out to the maintainers in your pull request

Thank you for contributing to release-service-utils!

# Python Development Rules

## Build & Package Management

- Prefer `make` targets for all operations - use Makefile automation
- When direct commands needed: use `uv` exclusively (`uv add`, `uv run`)
- Version-pin all dependencies

## Code Style

- Readability > cleverness
- Let specific errors propagate: never use `try...except`.
- Minimize if/else - prefer early returns and maps
- Minimize branching - flat code flows better
- Functions: small, focused, typed, descriptively named
- Skip abstractions for single-use code
- Don't add excessive, defensive, or unnecessary early return cases
- Collocate related functionality
- 120 char line limit

## Typing

- No `# type: ignore` - fix root issues instead
- No `Any` - use precise types
- Return declared type or fail fast
- No `Optional[T]` or `None` unions as returns
- Use `list`/`dict` not `typing.List`/`Dict`

## Documentation

- Minimal, high-bitrate docs - maximize information/word ratio
- Explain *why*, not *what*
- Simplify code before adding documentation

## Libraries

- polars for all data frame opps - no pandas
- PyTest for all tests - no unittest
- GitHub Actions for CI/CD

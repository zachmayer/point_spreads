# Python Development Rules

## Build & Package Management

- Prefer `make` targets for all operations - use Makefile automation
- When direct commands needed: use `uv` exclusively (`uv add`, `uv run`)
- Version-pin all dependencies

## Code Style

- Readability > cleverness
- Functions: small, focused, typed, descriptively named
- Skip abstractions for single-use code
- Collocate related functionality
- 120 char line limit

## Testing & CI

- PyTest for all tests - no unittest
- GitHub Actions for CI/CD

## Typing

- No `# type: ignore` - fix root issues instead
- Be precise with types - avoid Any

## Documentation

- Minimal, high-bitrate docs - maximize information/word ratio
- Explain *why*, not *what*
- Simplify code before adding documentation

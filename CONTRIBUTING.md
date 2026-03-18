# Contributing

Thanks for taking the time to contribute! Here's everything you need to get started.

## Getting started

1. **Fork** the repository and clone it locally.
2. Create a new branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes, then commit with a clear message:
   ```bash
   git commit -m "feat: describe what you changed"
   ```
4. Push your branch and open a **Pull Request**.

## Commit message format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | When to use |
|--------|-------------|
| `feat:` | A new feature |
| `fix:` | A bug fix |
| `docs:` | Documentation only |
| `chore:` | Maintenance, deps, config |
| `refactor:` | Code cleanup, no behavior change |
| `test:` | Adding or updating tests |

## Code style

- Keep functions small and focused.
- Prefer clarity over cleverness.
- Add comments where the *why* isn't obvious.
- Run the linter before pushing:
  ```bash
  npm run lint   # or your project's equivalent
  ```

## Pull requests

- Link any related issues in the PR description.
- Keep PRs focused — one change per PR when possible.
- A maintainer will review and may request changes.
- Once approved, we'll squash and merge.

## Reporting issues

Open an issue and include:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Your environment (OS, runtime version, etc.)

## Questions?

Open a discussion or reach out in the issues tab. We're happy to help.

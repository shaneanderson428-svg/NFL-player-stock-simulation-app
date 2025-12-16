"""Load environment variables from .env.local for scripts.

Importing this module will attempt to load a `.env.local` file from the
repository root using python-dotenv. It intentionally does not overwrite
existing environment variables.

Usage: simply `import scripts._env` at the top of any script before
reading `os.getenv("RAPIDAPI_KEY")`.
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv may not be installed in CI
    load_dotenv = None  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = ROOT / ".env.local"


def _maybe_load_env() -> None:
    if load_dotenv is None:
        # dotenv not installed — nothing to do. Scripts will still read
        # environment variables from the shell if present.
        return
    # Only load .env.local if it exists. Do not override existing env vars.
    if DOTENV_PATH.exists():
        load_dotenv(dotenv_path=str(DOTENV_PATH), override=False)


def get_rapidapi_key() -> str:
    """Return RAPIDAPI_KEY from the environment, or raise a helpful error.

    This helper is provided for scripts that want an explicit error if the
    key is missing. It is optional — scripts that call `os.getenv` will also
    see the variable after importing this module.
    """
    _maybe_load_env()
    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError(
            "RAPIDAPI_KEY is not set. Create a file named .env.local at the project root with the line:\n"
            "RAPIDAPI_KEY=your_key_here\n\n"
            "That file is ignored by git (see .gitignore). Alternatively, export the variable in your shell."
        )
    return key


# Load environment on import for convenience (so scripts only need to import this module)
_maybe_load_env()

"""Dependency-free browser components for an application's existing HTML renderer."""

from importlib.resources import files


def browser_script() -> bytes:
    """Return the ES module an HTTP host can serve as JavaScript.

    >>> b"export function createBrowser" in browser_script()
    True
    """
    return files("ionflow").joinpath("assets/browser.js").read_bytes()

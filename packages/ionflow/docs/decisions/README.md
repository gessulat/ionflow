# Design decision records

One short file per decision that future readers (humans or coding agents) might
re-litigate. Numbered. When a decision is overturned, write the new record,
link both ways, and shrink the old one to a stub (title, status, the gist);
when code drift merely stales a record's references, append a dated `NOTE`
instead of rewriting history.

Keep each record under a page: **Context** (the forces), **Decision** (what and
why), **Consequences** (what it costs, what it rules out, rejected alternatives).
Write it when the decision is made, while the "why not" is still fresh.

AGENTS.md, at the repo root, stays the operational cheat sheet (how things work today); records
here hold the *why*, so AGENTS.md should link to a record rather than inline
its reasoning.

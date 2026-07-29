"""The reuse policy of a resumed run, as a pure object — no I/O, no store.

`Planner` owns every decision about standing origin parts in for work this run
would otherwise do; the runner only executes what it is told. Pure functions of
(dag, origin manifest, live manifest), so the policy is unit-testable with
plain `Manifest`s and previewable without running anything.
"""

from .state import Slot


class Planner:
    """Reuse decisions against one origin run.

    An empty origin (fresh run) makes every method return nothing, so the
    runner needs no resume conditionals. The three moments:

    - `upfront()` before anything runs,
    - `node_reuse()` when a plan node becomes ready,
    - `member_reuse()` when a fan-out's per-item group becomes ready.
    """

    def __init__(self, dag, origin):
        self.dag = dag
        self.origin = origin
        self.keyed = {n for n, s in dag.steps.items() if s.is_fanout and s.output_table.fanout_key_column()}
        # Steps downstream of a keyed fan-out can't be reused upfront: the
        # fan-out re-runs first (the work-list may have changed), items are
        # then matched by key, and everything else downstream is reused only
        # when the key set is unchanged.
        self.downstream = {f: dag.downstream(f) for f in self.keyed}
        deferred = set().union(*self.downstream.values()) if self.downstream else set()
        self.deferred = deferred
        self.key_roots = {m: r for m, r in dag.roots.items() if r in self.keyed}
        self.lazy = deferred - self.keyed - set(self.key_roots)

    def unchanged(self, manifest, name, excluding=None):
        """True when every keyed fan-out upstream of `name` (bar `excluding`) kept its key set."""
        return all(
            set(manifest.keys(f)) == set(self.origin.keys(f))
            for f, ds in self.downstream.items()
            if f != excluding and name in ds
        )

    def upfront(self):
        """Whole-step reuse decidable before anything runs: every origin part
        of a step that is not downstream of a keyed fan-out."""
        return [s for s, _ in self.origin.slots() if s.step not in self.deferred]

    def node_reuse(self, manifest, name):
        """Deferred whole-step reuse for plan node `name`, decidable once its
        upstream keyed work-lists are known (and only if none of them changed)."""
        if name not in self.lazy or not self.unchanged(manifest, name):
            return []
        return [Slot(name, k) for k in self.origin.keys(name) if Slot(name, k) not in manifest]

    def member_reuse(self, manifest, group):
        """Key-matched reuse for a group's members: an item is reused where its
        key exists in the origin AND no *other* keyed ancestor's work-list
        changed (a changed collect input taints a matching key)."""
        keys = manifest.keys(group.fanout)
        out = []
        for member in group.order:
            root = self.key_roots.get(member)
            if root is None or not self.unchanged(manifest, member, excluding=root):
                continue
            out += [
                Slot(member, k)
                for k in keys
                if Slot(member, k) not in manifest and self.origin.get(member, k) is not None
            ]
        return out

    def preview(self):
        """{step: (reused, computed)} for a resume whose keyed work-lists come
        back unchanged — the informative case is a failed origin, where
        `computed` is exactly the missing work. A keyed fan-out always re-runs,
        so its parts count as computed; -1 computed = width unknown (the step
        never produced parts in the origin)."""
        out = {}
        for name, step in self.dag.steps.items():
            root = self.dag.roots.get(name)
            width = self.origin.width(root) if root else self.origin.width(name) if step.is_fanout else 1
            have = 0 if name in self.keyed else self.origin.width(name)
            out[name] = (have, width - have if width else -1)
        return out

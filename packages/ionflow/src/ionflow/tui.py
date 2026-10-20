from pathlib import Path
from typing import cast

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Static,
    TabbedContent,
    TabPane,
    Tabs,
)

from . import git, lake, query
from .config import SETUP_FIELDS, Config, ConfigMissing, config_from_setup, write_config
from .flow import discover_flows
from .space import Space
from .storage import Storage, TransferPlan, request_refresh
from .store import Store

LEDGER_COLS = ["run_id", "status", "started", "finished", "local", "s3"]


def _slug(flow_id):
    """Widget-id-safe form of a flow_id (Textual ids forbid the dots).

    Injective: "-" cannot appear in a module or class name, so distinct
    flow_ids never collide.
    """
    return flow_id.replace(".", "-")


def _fill(table, relation, columns=None):
    """Fill a DataTable from a DuckDB relation (all columns, or a named subset)."""
    pairs = [(c, relation.columns.index(c)) for c in (columns or relation.columns) if c in relation.columns]
    table.clear(columns=True)
    table.add_columns(*["S3" if c == "s3" else c for c, _ in pairs])
    for row in relation.fetchall():
        table.add_row(
            *[
                ("✓" if row[i] else "○") if c in ("local", "s3") else "" if row[i] is None else str(row[i])
                for c, i in pairs
            ]
        )


class TransferScreen(Screen):
    """Review an explicit direction before any payload is copied."""

    BINDINGS = [Binding("escape", "app.pop_screen", "Back")]

    def __init__(self, storage, direction, flow_id=None, run_id=None, artifact_ids=None):
        super().__init__()
        self.storage = storage
        self.selection = (direction, flow_id, run_id, artifact_ids)
        self.plan: TransferPlan | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Preparing transfer plan…", id="transfer-info")
        yield Button("Confirm transfer", id="confirm-transfer", disabled=True)
        yield Button("Back", id="back-transfer")
        yield Footer()

    def on_mount(self) -> None:
        self.prepare()

    @work(thread=True, exclusive=True)
    def prepare(self) -> None:
        try:
            plan = self.storage.plan(*self.selection)
            self.app.call_from_thread(self.ready, plan)
        except Exception as error:  # noqa: BLE001
            self.app.call_from_thread(self.query_one("#transfer-info", Static).update, str(error))

    def ready(self, plan) -> None:
        self.plan = plan
        summary = plan.summary()
        self.query_one("#transfer-info", Static).update(
            f"{summary['source']} → {summary['target']}\n"
            f"{summary['files']} missing file(s), {summary['bytes']:,} bytes\n"
            f"Runs: {', '.join(summary['runs']) or 'none'}\n"
            f"Artifacts (including parents): {len(summary['artifacts'])}\nTransfer in this direction?"
        )
        self.query_one("#confirm-transfer", Button).disabled = False

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back-transfer":
            self.app.pop_screen()
        elif event.button.id == "confirm-transfer" and self.plan:
            event.button.disabled = True
            self.transfer()

    @work(thread=True, exclusive=True)
    def transfer(self) -> None:
        try:
            self.storage.transfer(
                self.plan,
                lambda p: self.app.call_from_thread(
                    self.query_one("#transfer-info", Static).update,
                    f"Transferred {p['bytes']:,} / {p['total']:,} bytes",
                ),
            )
            self.app.call_from_thread(self.query_one("#transfer-info", Static).update, "Transfer complete.")
        except Exception as error:  # noqa: BLE001
            self.app.call_from_thread(self.query_one("#transfer-info", Static).update, str(error))


class FlowTable(DataTable):
    BINDINGS = [Binding("j", "cursor_down", "Down"), Binding("k", "cursor_up", "Up")]


class RunDetailScreen(Screen):
    """Per-step output preview + resource stats for one run; Esc goes back."""

    BINDINGS = [Binding("escape", "app.pop_screen", "Back")]
    CSS = "#steps { width: 24; border-right: solid $primary; } #preview { width: 1fr; }"
    STATS = "__stats__"  # synthetic list entry; can't collide (steps never start with _)

    def __init__(self, flow_cls, run_id, con):
        super().__init__()
        self.flow_cls = flow_cls
        self.run_id = run_id
        self.con = con

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label(f"{self.flow_cls.flow_id()} · run {self.run_id}")
        yield Horizontal(
            ListView(
                ListItem(Label("▸ stats"), id=f"step-{self.STATS}"),
                *[ListItem(Label(s), id=f"step-{s}") for s in self.flow_cls._dag.steps],
                id="steps",
            ),
            DataTable(id="preview", cursor_type="row"),
        )
        yield Footer()

    def on_mount(self) -> None:
        self._show(self.STATS)

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.item is not None:
            self._show(event.item.id.removeprefix("step-"))

    def _show(self, step) -> None:
        if step == self.STATS:
            rel = query.step_stats(self.con, self.run_id)
        else:
            rel = query.step_rows(self.con, step, self.run_id)
        _fill(self.query_one("#preview", DataTable), rel)


class OverviewScreen(Screen):
    """One tab per flow, each a table of that flow's runs (ledger + custom cols)."""

    BINDINGS = [
        Binding("tab", "next_flow", "Next flow", priority=True),
        Binding("shift+tab", "prev_flow", "Prev flow", priority=True),
    ]
    CSS = "#banner { color: $warning; }"

    def __init__(self, config, flows, warnings, with_data):
        super().__init__()
        self.config = config
        self.flows = flows
        self.warnings = warnings
        self.with_data = with_data
        self._cons = {}

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("\n".join("⚠ " + w for w in self.warnings), id="banner")
        if not self.flows:
            yield Static("No flows found. Set `flows` in .ionflow/config to your flows package.")
        with TabbedContent():
            for name in self.flows:
                with TabPane(name, id=f"flow-{_slug(name)}"):
                    yield FlowTable(cursor_type="cell", id=f"table-{_slug(name)}")
            with TabPane("Artifacts", id="artifacts"):
                yield FlowTable(cursor_type="cell", id="artifact-table")
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(10, self.refresh_tables)
        self.refresh_tables()
        self._focus_active_table()

    def refresh_tables(self) -> None:
        request_refresh(self.config)
        for name, cls in self.flows.items():
            table = self.query_one(f"#table-{_slug(name)}", FlowTable)
            try:
                rel = query.ledger_with_liveness(cls, self.config).order("started DESC")
                _fill(table, rel, LEDGER_COLS + list(cls.params_columns()) + list(cls.ledger_columns()))
            except Exception as e:  # noqa: BLE001 — show the failure in the tab, don't crash
                table.add_columns("error")
                table.add_row(str(e))
        if self.config.local_dir:
            workspace = Storage(self.config)
            rows = workspace.artifacts()
            table = self.query_one("#artifact-table", FlowTable)
            table.clear(columns=True)
            table.add_columns("id", "kind", "name", "revision", "local", "S3")
            for row in rows:
                table.add_row(
                    row["id"],
                    row["kind"],
                    row["name"],
                    str(row["revision"]),
                    "✓" if row["local"] else "○",
                    "✓" if row["s3"] else "○",
                )
            status = workspace.status()
            self.query_one("#banner", Static).update(
                "\n".join(
                    [
                        *self.warnings,
                        f"Metadata checked: {status.get('checked') or 'not yet'}",
                        *([f"Remote unavailable: {status['error']}"] if status.get("error") else []),
                        *([f"Metadata conflicts: {', '.join(status['conflicts'])}"] if status.get("conflicts") else []),
                    ]
                )
            )

    def _active_flow(self):
        """flow_id of the active tab — inverts _slug ("-" never occurs in a flow_id)."""
        return self.query_one(TabbedContent).active.removeprefix("flow-").replace("-", ".")

    def _focus_active_table(self) -> None:
        slug = self.query_one(TabbedContent).active.removeprefix("flow-")
        try:
            self.query_one("#artifact-table" if slug == "artifacts" else f"#table-{slug}", FlowTable).focus()
        except Exception:  # noqa: BLE001
            pass

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        self._focus_active_table()

    def action_next_flow(self) -> None:
        self.query_one(Tabs).action_next_tab()

    def action_prev_flow(self) -> None:
        self.query_one(Tabs).action_previous_tab()

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        table = event.data_table
        cols = ["s3" if str(c.label) == "S3" else str(c.label) for c in table.columns.values()]
        row = table.get_row_at(event.coordinate.row)
        column = cols[event.coordinate.column]
        artifact = table.id == "artifact-table"
        name = self._active_flow()
        if column in ("local", "s3"):
            if not self.config.remote:
                self.notify("No remote configured", severity="warning")
                return
            if row[cols.index(column)] == "✓":
                self.notify("Payload already present")
                return
            self.app.push_screen(
                TransferScreen(
                    Storage(self.config),
                    "down" if column == "local" else "up",
                    None if artifact else name,
                    None if artifact else row[cols.index("run_id")],
                    [row[cols.index("id")]] if artifact else None,
                )
            )
            return
        if not artifact and name in self.flows:
            if row[cols.index("local")] != "✓":
                self.notify("Download this run before reading outputs", severity="warning")
                return
            run_id = row[cols.index("run_id")]
            self.app.push_screen(RunDetailScreen(self.flows[name], run_id, self.flows[name].duck(self.config)))


class SetupScreen(Screen):
    """Shown when the repo isn't configured; writes .ionflow/config.

    The form is generated from SETUP_FIELDS — the same spec that drives
    `ionflow setup`'s prompts.
    """

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Configure ionflow for this repo")
        for key, label, default in SETUP_FIELDS:
            yield Label(label)
            yield Input(value=default or "", id=key)
        yield Button("Save", id="save", variant="primary")
        yield Static("", id="setup-msg")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "save":
            return
        vals = {
            key: self.query_one(f"#{key}", Input).value.strip() or default or "" for key, _, default in SETUP_FIELDS
        }
        if not vals["local_dir"]:
            self.query_one("#setup-msg", Static).update("A local store directory is required.")
            return
        cfg = config_from_setup(vals)
        root = git.repo_root()
        write_config(Path(root) if root else Path.cwd(), cfg)
        cfg = Config.load(Path(root) if root else Path.cwd())
        app = cast("IonflowTUI", self.app)
        app.switch_screen(app.overview_screen(cfg))


def scan_runs(store):
    """Flow names with a catalog in the store — one LIST of the prefix."""
    return set(store.flow_ids())


def startup_warnings(flows, store, with_data):
    """Health check: orphaned stored data + schema drift, as human-readable lines."""
    warnings = []
    for name in sorted(with_data - set(flows)):
        warnings.append(f"{name}: has runs in the inventory but no matching Flow in code")
    for name, cls in flows.items():  # one store serves every flow's check
        if name not in with_data:
            continue
        try:
            warnings += lake.schema_drift(cls, Space.for_flow(store, cls))
        except Exception as e:  # noqa: BLE001
            warnings.append(f"{name}: schema check failed: {e}")
    return warnings


class IonflowTUI(App):
    TITLE = "ionflow"
    BINDINGS = [Binding("q", "quit", "Quit")]

    def __init__(self, config=None):
        super().__init__()
        self._config = config

    def on_mount(self) -> None:
        self.theme = "dracula"
        try:
            cfg = self._config or Config.load()
        except ConfigMissing:
            self.push_screen(SetupScreen())
            return
        self.push_screen(self.overview_screen(cfg))

    def overview_screen(self, cfg) -> OverviewScreen:
        try:
            flows = discover_flows(cfg.flows) if cfg.flows else {}
            import_warnings = []
        except Exception as e:  # noqa: BLE001
            flows, import_warnings = {}, [f"could not import flows '{cfg.flows}': {e}"]
        try:
            store = Store.of(cfg)
            with_ledger = scan_runs(store)
            warnings = import_warnings + startup_warnings(flows, store, with_ledger)
            with_data = set(flows) & with_ledger
        except Exception as e:  # noqa: BLE001
            with_data, warnings = set(), import_warnings + [f"could not read storage: {e}"]
        return OverviewScreen(cfg, flows, warnings, with_data)


def run_tui(config=None):
    IonflowTUI(config).run()

from pathlib import Path

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

from . import discovery, git, query
from .config import Config, ConfigMissing, write_config

LEDGER_COLS = ["run_id", "status", "started", "finished"]  # display subset of the ledger


def _slug(flow_id):
    """Widget-id-safe form of a flow_id (Textual ids forbid the dots).

    Injective: "-" cannot appear in a module or class name, so distinct
    flow_ids never collide.
    """
    return flow_id.replace(".", "-")


def _fill(table, relation, columns=None):
    """Fill a DataTable from a DuckDB relation (all columns, or a named subset)."""
    pairs = [
        (c, relation.columns.index(c))
        for c in (columns or relation.columns)
        if c in relation.columns
    ]
    table.clear(columns=True)
    table.add_columns(*[c for c, _ in pairs])
    for row in relation.fetchall():
        table.add_row(*["" if row[i] is None else str(row[i]) for _, i in pairs])


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
        if self.warnings:
            yield Static("\n".join("⚠ " + w for w in self.warnings), id="banner")
        if not self.flows:
            yield Static("No flows found. Set `flows` in .microflow/config to your flows package.")
        with TabbedContent():
            for name, cls in self.flows.items():
                with TabPane(name + (" [proto]" if cls.prototype else ""), id=f"flow-{_slug(name)}"):
                    yield FlowTable(cursor_type="row", id=f"table-{_slug(name)}")
        yield Footer()

    def on_mount(self) -> None:
        for name, cls in self.flows.items():
            table = self.query_one(f"#table-{_slug(name)}", FlowTable)
            if name not in self.with_data:
                table.add_columns("info")
                table.add_row("no runs yet")
                continue
            try:
                con = cls.duck(self.config)
                self._cons[name] = con
                rel = query.ledger_with_liveness(cls, self.config, con).order("started DESC")
                _fill(table, rel, LEDGER_COLS + list(cls.params_columns()) + list(cls.ledger_columns()))
            except Exception as e:  # noqa: BLE001 — show the failure in the tab, don't crash
                table.add_columns("error")
                table.add_row(str(e))
        self._focus_active_table()

    def _active_flow(self):
        """flow_id of the active tab — inverts _slug ("-" never occurs in a flow_id)."""
        return self.query_one(TabbedContent).active.removeprefix("flow-").replace("-", ".")

    def _focus_active_table(self) -> None:
        if not self.flows:
            return
        slug = self.query_one(TabbedContent).active.removeprefix("flow-")
        try:
            self.query_one(f"#table-{slug}", FlowTable).focus()
        except Exception:  # noqa: BLE001
            pass

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        self._focus_active_table()

    def action_next_flow(self) -> None:
        self.query_one(Tabs).action_next_tab()

    def action_prev_flow(self) -> None:
        self.query_one(Tabs).action_previous_tab()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        name = self._active_flow()
        con = self._cons.get(name)
        if con is None:
            return
        cols = [str(c.label) for c in event.data_table.columns.values()]
        run_id = str(event.data_table.get_row(event.row_key)[cols.index("run_id")])
        self.app.push_screen(RunDetailScreen(self.flows[name], run_id, con))


class SetupScreen(Screen):
    """Shown when the repo isn't configured; writes .microflow/config."""

    FIELDS = [  # (input id, prompt label, default)
        ("bucket", "S3 bucket", ""),
        ("prefix", "Key prefix", "microflow"),
        ("endpoint", "S3 endpoint (blank for AWS)", ""),
        ("region", "Region", "us-east-1"),
        ("flows", "Flows package (e.g. myproject.flows)", ""),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Configure microflow for this repo")
        for name, label, default in self.FIELDS:
            yield Label(label)
            yield Input(value=default, id=name)
        yield Button("Save", id="save", variant="primary")
        yield Static("", id="setup-msg")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "save":
            return
        vals = {
            name: self.query_one(f"#{name}", Input).value.strip() or default or None
            for name, _, default in self.FIELDS
        }
        if not vals["bucket"]:
            self.query_one("#setup-msg", Static).update("Bucket is required.")
            return
        cfg = Config(
            bucket=vals["bucket"],
            prefix=vals["prefix"],
            endpoint_url=vals["endpoint"],
            region=vals["region"],
            flows=vals["flows"],
        )
        root = git.repo_root()
        write_config(Path(root) if root else Path.cwd(), cfg)
        self.app.switch_screen(self.app.overview_screen(cfg))


class MicroflowTUI(App):
    TITLE = "microflow"
    BINDINGS = [Binding("q", "quit", "Quit")]

    def __init__(self, config=None, flows_override=None):
        super().__init__()
        self._config = config
        self._flows_override = flows_override

    def on_mount(self) -> None:
        try:
            cfg = self._config or Config.load()
        except ConfigMissing:
            self.push_screen(SetupScreen())
            return
        if self._flows_override:
            cfg.flows = self._flows_override
        self.push_screen(self.overview_screen(cfg))

    def overview_screen(self, cfg) -> OverviewScreen:
        try:
            flows = discovery.discover_flows(cfg.flows) if cfg.flows else {}
            import_warnings = []
        except Exception as e:  # noqa: BLE001
            flows, import_warnings = {}, [f"could not import flows '{cfg.flows}': {e}"]
        try:
            stable, samples, proto = discovery.scan_runs(cfg)
            warnings = import_warnings + discovery.startup_warnings(flows, cfg, stable, samples)
            # each flow has data only in the namespace its flag points at
            with_data = {n for n, c in flows.items() if n in (proto if c.prototype else stable)}
        except Exception as e:  # noqa: BLE001
            with_data, warnings = set(), import_warnings + [f"could not read S3: {e}"]
        return OverviewScreen(cfg, flows, warnings, with_data)


def run_tui(config=None, flows_override=None):
    MicroflowTUI(config, flows_override).run()

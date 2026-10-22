// Supply the application's htm-compatible renderer. No framework or network dependency.
export function createBrowser(html) {
  const text = (value) => value == null ? "—" : typeof value === "object" ? JSON.stringify(value) : String(value);
  function format(value, kind) {
    if (value == null) return "—";
    if (kind === "bytes") {
      const units = ["B", "KiB", "MiB", "GiB", "TiB"];
      const power = value > 0 ? Math.min(4, Math.floor(Math.log(value) / Math.log(1024))) : 0;
      return `${(value / 1024 ** power).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${units[power]}`;
    }
    if (kind === "integer" || kind === "number") return Number(value).toLocaleString();
    if (kind === "duration") return `${Number(value).toLocaleString()} s`;
    if (kind === "datetime") return new Date(value).toLocaleString();
    if (Array.isArray(value)) return value.map(text).join(", ");
    return text(value);
  }
  function Cell({ row, name, view, href }) {
    const kind = view.references?.[name];
    const value = row[name];
    if (kind && value != null) {
      return (Array.isArray(value) ? value : [value]).map((id, index) => html`${index ? ", " : ""}<a href=${href("artifact", kind, id)}>${id}</a>`);
    }
    return format(value, view.formats?.[name]);
  }
  function Table({ schema, rows, href, rowHref, actions }) {
    const view = schema.view || {};
    const columns = view.columns || Object.keys(schema.fields || {});
    return html`<table class="ionflow-table"><thead><tr>
      ${actions && html`<th scope="col">Storage</th>`}${rowHref && html`<th scope="col">Details</th>`}
      ${columns.map((name) => html`<th scope="col">${view.labels?.[name] || name}</th>`)}
    </tr></thead><tbody>${rows.map((row) => html`<tr>
      ${actions && html`<td>${actions(row)}</td>`}${rowHref && html`<td><a href=${rowHref(row)}>View</a></td>`}
      ${columns.map((name) => html`<td><${Cell} row=${row} name=${name} view=${view} href=${href} /></td>`)}
    </tr>`)}</tbody></table>`;
  }
  function Navigation({ description, href, current }) {
    return html`<nav aria-label="Browse ionflow">
      ${description.flows.map((flow) => html`<a href=${href("flow", flow.id)} class=${current === flow.id ? "active" : ""}>${flow.view.plural || flow.view.label}</a>`)}
      ${description.artifacts.map((schema) => html`<a href=${href("artifact", schema.kind)} class=${current === schema.kind ? "active" : ""}>${schema.view.plural || schema.view.label}</a>`)}
    </nav>`;
  }
  function Record({ schema, row, href }) {
    const view = schema.view || {};
    const names = Array.isArray(schema.fields) ? schema.fields : Object.keys(schema.fields || {});
    return html`<dl class="ionflow-record">${names.map((name) => html`
      <dt>${view.labels?.[name] || name}</dt><dd><${Cell} row=${row} name=${name} view=${view} href=${href} /></dd>
    `)}</dl>`;
  }
  function Artifacts({ schema, rows, selected, href, actions, details }) {
    const row = rows.find((item) => item.id === selected);
    return html`<h2>${schema.view.plural || schema.view.label}</h2>
      ${rows.length ? html`<${Table} schema=${schema} rows=${rows} href=${href} rowHref=${(item) => href("artifact", schema.kind, item.id)} actions=${actions} />` : html`<p>No artifacts registered.</p>`}
      ${row && html`<section><h3>${row.name}</h3>${details ? details(row) : html`<${Record} schema=${schema} row=${row} href=${href} />`}</section>`}`;
  }
  function Runs({ schema, rows, selected, href, actions, details }) {
    const row = rows.find((item) => item.run_id === selected);
    return html`<h2>${schema.view.plural || schema.view.label}</h2>
      ${rows.length ? html`<${Table} schema=${schema} rows=${rows} href=${href} rowHref=${(item) => href("flow", schema.id, item.run_id)} actions=${actions} />` : html`<p>No runs recorded.</p>`}
      ${row && html`<section><h3>${row.run_id}</h3><${Record} schema=${schema} row=${row} href=${href} />${details?.(row)}</section>`}`;
  }
  function Graph({ schema, href }) {
    return html`<ol class="ionflow-graph">${schema.steps.map((step) => html`<li>
      <strong>${step.name}</strong>${step.dependencies.length ? ` ← ${step.dependencies.join(", ")}` : ""}
      ${step.subflow && html` · <a href=${href("flow", step.subflow.flow_id)}>${step.subflow.flow_id}</a>`}
      ${step.branch && ` · per ${step.branch}`}
    </li>`)}</ol>`;
  }
  return { Navigation, Table, Record, Artifacts, Runs, Graph, format };
}

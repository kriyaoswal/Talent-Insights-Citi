import { useEffect, useMemo, useState } from 'react'

export function Loaded({ state, children }) {
  if (state.loading) return <p className="muted">Loading from the Gold layer…</p>
  if (state.error)
    return (
      <div className="error">
        <strong>Could not load data.</strong> {state.error}
        <br />
        Locally, is the API running? (<code>data/talent-etl/.venv/bin/python data/talent-etl/dashboard/api.py</code>)
      </div>
    )
  return children(state.data)
}

// ---------------------------------------------------------------- formats ----

export const fmt = {
  num: (v) => (v == null ? '–' : Number(v).toLocaleString()),
  dec: (v, d = 2) => (v == null ? '–' : Number(v).toFixed(d)),
  pct: (v) => (v == null ? '–' : `${Math.round(Number(v) * 100)}%`),
  yes: (v) => (v ? 'Yes' : ''),
  time: (v) => (v ? `${String(v).slice(0, 16).replace('T', ' ')} UTC` : '–'),
  text: (v) => (v == null || v === '' ? '–' : String(v).replaceAll('_', ' ')),
  title: (v) => (v == null ? '–' : fmt.text(v).replace(/^./, (c) => c.toUpperCase())),
}

// ---------------------------------------------------------------- layout -----

export function Question({ q, title, answer, children }) {
  return (
    <section>
      <p className="eyebrow">{q}</p>
      <h2>{title}</h2>
      <div className="answer">{answer}</div>
      {children}
    </section>
  )
}

export function Panel({ title, note, children }) {
  return (
    <div className="panel">
      {title && <h3>{title}</h3>}
      {note && <p className="muted small">{note}</p>}
      {children}
    </div>
  )
}

export function Grid({ children }) {
  return <div className="grid">{children}</div>
}

export function Caveats({ items }) {
  return (
    <div className="caveats">
      <h3>Caveats</h3>
      <ul>
        {items.map((c) => (
          <li key={c}>{c}</li>
        ))}
      </ul>
    </div>
  )
}

/** "Behind the answer": how the answer was found, and the data it comes from. */
export function Behind({ points, gold, sources }) {
  return (
    <div className="panel behind">
      <h3>Behind the answer</h3>
      <ul className="tight">
        {points.map((p, i) => (
          <li key={i}>{p}</li>
        ))}
      </ul>
      <p className="data-used small">
        <span className="muted">Answer tables (Gold): </span>
        {gold.map((g) => (
          <code key={g}>{g}</code>
        ))}
        <br />
        <span className="muted">Built from: </span>
        {sources.join(' · ')}
      </p>
    </div>
  )
}

// The insights notebook's own charts, exported by data/talent-etl/scripts/export_notebook_charts.py
let chartIndex
function loadCharts() {
  chartIndex ??= fetch('/charts/charts.json').then((r) => (r.ok ? r.json() : {})).catch(() => ({}))
  return chartIndex
}

export function NotebookCharts({ q }) {
  const [charts, setCharts] = useState(null)
  useEffect(() => {
    let live = true
    loadCharts().then((all) => live && setCharts(all[q] || []))
    return () => {
      live = false
    }
  }, [q])
  if (!charts?.length) return null
  return (
    <div className="panel">
      <h3>Charts from the notebook</h3>
      <div className="nb-charts">
        {charts.map((c) => (
          <figure key={c.file}>
            <img src={`/${c.file}`} alt={c.titles.join(' · ') || 'Notebook chart'} loading="lazy" />
          </figure>
        ))}
      </div>
    </div>
  )
}

export function Stat({ label, value, detail }) {
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
      {detail && <div className="muted small">{detail}</div>}
    </div>
  )
}

// ---------------------------------------------------------------- charts -----

/** Horizontal bars. items: [{label, value}]; format turns a value into its label. */
export function Bars({ items, format = fmt.num, max }) {
  const top = max ?? Math.max(...items.map((i) => i.value), 0)
  return (
    <div className="bars">
      {items.map((i) => (
        <div className="bar-row" key={i.label}>
          <span className="bar-label">{fmt.text(i.label)}</span>
          <span className="bar-track">
            <span className="bar-fill" style={{ width: `${top ? (100 * i.value) / top : 0}%` }} />
          </span>
          <span className="bar-value">{format(i.value)}</span>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------- tables -----

/**
 * Sortable, searchable table with paging.
 * columns: [{key, label, format?, align?}]; filters: [{key, label}] adds a dropdown per key.
 */
export function DataTable({ rows, columns, search = [], filters = [], pageSize = 15 }) {
  const [query, setQuery] = useState('')
  const [chosen, setChosen] = useState({})
  const [sort, setSort] = useState({ key: null, dir: 1 })
  const [page, setPage] = useState(0)

  const options = useMemo(
    () =>
      Object.fromEntries(
        filters.map((f) => [f.key, [...new Set(rows.map((r) => r[f.key]))].filter((v) => v != null).sort()]),
      ),
    [rows, filters],
  )

  const shown = useMemo(() => {
    const q = query.trim().toLowerCase()
    let out = rows.filter(
      (r) =>
        filters.every((f) => !chosen[f.key] || String(r[f.key]) === chosen[f.key]) &&
        (!q || search.some((k) => String(r[k] ?? '').toLowerCase().includes(q))),
    )
    if (sort.key) {
      out = [...out].sort((a, b) => {
        const x = a[sort.key]
        const y = b[sort.key]
        if (x == null) return 1
        if (y == null) return -1
        return (x > y ? 1 : x < y ? -1 : 0) * sort.dir
      })
    }
    return out
  }, [rows, query, chosen, sort, filters, search])

  const pages = Math.max(1, Math.ceil(shown.length / pageSize))
  const current = Math.min(page, pages - 1)
  const slice = shown.slice(current * pageSize, (current + 1) * pageSize)

  return (
    <div className="table-wrap">
      {(search.length > 0 || filters.length > 0) && (
        <div className="toolbar">
          {search.length > 0 && (
            <input
              type="search"
              placeholder="Search…"
              value={query}
              onChange={(e) => {
                setQuery(e.target.value)
                setPage(0)
              }}
            />
          )}
          {filters.map((f) => (
            <select
              key={f.key}
              value={chosen[f.key] || ''}
              onChange={(e) => {
                setChosen({ ...chosen, [f.key]: e.target.value })
                setPage(0)
              }}
            >
              <option value="">All {f.label}</option>
              {options[f.key].map((o) => (
                <option key={o} value={o}>
                  {fmt.text(o)}
                </option>
              ))}
            </select>
          ))}
          <span className="muted small">
            {shown.length.toLocaleString()} of {rows.length.toLocaleString()} rows
          </span>
        </div>
      )}
      <div className="scroll">
        <table>
          <thead>
            <tr>
              {columns.map((c) => (
                <th
                  key={c.key}
                  className={c.align === 'right' ? 'right' : ''}
                  onClick={() => setSort({ key: c.key, dir: sort.key === c.key ? -sort.dir : 1 })}
                  title="Sort"
                >
                  {c.label}
                  {sort.key === c.key ? (sort.dir > 0 ? ' ▲' : ' ▼') : ''}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {slice.map((r, i) => (
              <tr key={i}>
                {columns.map((c) => (
                  <td key={c.key} className={c.align === 'right' ? 'right' : ''}>
                    {(c.format || fmt.text)(r[c.key], r)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {pages > 1 && (
        <div className="pager">
          <button disabled={current === 0} onClick={() => setPage(current - 1)}>
            ‹ Previous
          </button>
          <span className="muted small">
            Page {current + 1} of {pages}
          </span>
          <button disabled={current >= pages - 1} onClick={() => setPage(current + 1)}>
            Next ›
          </button>
        </div>
      )}
    </div>
  )
}

// Common column sets
export const NAME = {
  key: 'last_name',
  label: 'Employee',
  format: (_, r) => `${r.first_name} ${r.last_name} (#${r.employee_id})`,
}
export const DEPARTMENT = { key: 'department', label: 'Department' }
export const TEAM = { key: 'team_name', label: 'Team' }
export const PERSON_SEARCH = ['first_name', 'last_name', 'employee_id', 'team_name']
export const DEPT_FILTER = [{ key: 'department', label: 'departments' }]

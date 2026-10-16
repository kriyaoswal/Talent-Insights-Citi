import { useState } from 'react'
import { Employee, Journey, Overview, Pipeline, Q1, Q2, Q3, Q4, Q5, Q6, Q7 } from './pages/index.jsx'
import { useApi } from './services/api.js'

const TABS = [
  ['Journey', 'Data journey', Journey],
  ['Overview', 'Overview', Overview],
  ['Q1', 'Q1 Performance', Q1],
  ['Q2', 'Q2 Skill gaps', Q2],
  ['Q3', 'Q3 Promotion', Q3],
  ['Q4', 'Q4 Training', Q4],
  ['Q5', 'Q5 Attrition', Q5],
  ['Q6', 'Q6 Teams', Q6],
  ['Q7', 'Q7 Skills', Q7],
  ['Employee', 'Employee', Employee],
  ['Pipeline', 'Pipeline', Pipeline],
]

function SourceBadge() {
  const { data, error } = useApi('source')
  if (error) return <span className="badge bad">API not reachable</span>
  if (!data) return <span className="badge">Connecting…</span>
  const run = data.latest_run
  return (
    <span className="source">
      <span className={`badge ${data.mode}`}>{data.mode === 'cloud' ? 'Cloud · S3' : 'Local'}</span>
      <span className="muted small" title={data.lake_root}>
        {data.lake_root}
        {run && (
          <>
            {' '}
            · run <code>{run.run_id}</code> · {run.checks_passed}/{run.checks_total} checks passed
          </>
        )}
      </span>
    </span>
  )
}

export default function App() {
  const [tab, setTab] = useState('Journey')
  const Page = TABS.find(([id]) => id === tab)[2]
  return (
    <div className="app">
      <header>
        <div>
          <h1>Talent Insights</h1>
          <p className="muted small">Performance &amp; talent answers from the talent-etl Gold layer</p>
        </div>
        <SourceBadge />
      </header>
      <nav>
        {TABS.map(([id, label]) => (
          <button key={id} className={id === tab ? 'active' : ''} onClick={() => setTab(id)}>
            {label}
          </button>
        ))}
      </nav>
      <main>
        <Page open={setTab} />
      </main>
    </div>
  )
}

import { useState } from 'react'
import {
  Bars, Caveats, DataTable, DEPARTMENT, DEPT_FILTER, Grid, Loaded, NAME, Panel, PERSON_SEARCH, Question, Stat, TEAM,
  fmt, useApi,
} from './components.jsx'

// ---------------------------------------------------------------- overview ---

export function Overview({ open }) {
  const state = useApi('/api/summary')
  return (
    <section>
      <h2>Answers to the seven questions</h2>
      <p className="muted">
        Every number is read live from the pipeline's Gold layer. Click a card for the detail.
      </p>
      <Loaded state={state}>
        {(d) => (
          <div className="cards">
            {d.cards.map((c) => (
              <button className="card" key={c.q} onClick={() => open(c.q)}>
                <span className="eyebrow">
                  {c.q} · {c.title}
                </span>
                <span className="card-value">{c.value}</span>
                <span className="muted small">{c.detail}</span>
              </button>
            ))}
          </div>
        )}
      </Loaded>
    </section>
  )
}

// ---------------------------------------------------------------- Q1 ---------

export function Q1() {
  const state = useApi('/api/q1')
  return (
    <Loaded state={state}>
      {(d) => {
        const t = Object.fromEntries(d.trend.map((x) => [x.label, x.value]))
        const [prev, latest] = d.periods
        return (
          <Question
            q="Q1"
            title="Performance ratings and review history for each employee"
            answer={
              <>
                Comparing {prev.period} with {latest.period}: <b>{fmt.num(t.improved)}</b> employees improved,{' '}
                <b>{fmt.num(t.stable)}</b> stayed the same and <b>{fmt.num(t.declined)}</b> declined. The average
                rating rose from <b>{prev.avg_rating}</b> to <b>{latest.avg_rating}</b>.
              </>
            }
          >
            <Grid>
              <Panel title="Rating trend per employee">
                <Bars items={d.trend} />
              </Panel>
              <Panel title="Ratings given per period" note="Number of valid reviews at each rating (1 = lowest, 5 = highest)">
                {d.periods.map((p) => (
                  <div key={p.period}>
                    <h4>
                      {p.period} · average {p.avg_rating}
                    </h4>
                    <Bars items={p.levels.map((v, i) => ({ label: `Rating ${i + 1}`, value: v }))} />
                  </div>
                ))}
              </Panel>
            </Grid>
            <Panel title="Every employee">
              <DataTable
                rows={d.employees}
                search={PERSON_SEARCH}
                filters={[...DEPT_FILTER, { key: 'trend', label: 'trends' }]}
                columns={[
                  NAME, DEPARTMENT, TEAM, { key: 'job_level', label: 'Level' },
                  { key: 'previous_rating', label: prev.period, align: 'right', format: fmt.num },
                  { key: 'latest_period_rating', label: latest.period, align: 'right', format: fmt.num },
                  { key: 'trend', label: 'Trend' },
                ]}
              />
            </Panel>
            <Caveats
              items={[
                'Only two review periods, so a trend is a single change, not a pattern over time.',
                'Ratings rose in every department, which may reflect a change in how ratings were given rather than real improvement.',
                `${fmt.num(d.flagged_reviews)} reviews are dated before the employee's hire. They stay in the history but are left out of ratings and trends, which is why ${fmt.num(t.insufficient_data)} employees have no measurable trend.`,
              ]}
            />
          </Question>
        )
      }}
    </Loaded>
  )
}

// ---------------------------------------------------------------- Q2 ---------

export function Q2() {
  const state = useApi('/api/q2')
  return (
    <Loaded state={state}>
      {(d) => (
        <Question
          q="Q2"
          title="Which employees have critical skill gaps in key competencies?"
          answer={
            <>
              <b>{fmt.num(d.employees)} employees</b> ({fmt.pct(d.employees / d.total_employees)}) have at least one
              critical skill gap, across {fmt.num(d.assessments)} assessments. The weakest skill is{' '}
              <b>{d.by_competency[0].label}</b>.
            </>
          }
        >
          <Panel title="How it is worked out">
            <ul className="tight">
              <li>
                <b>Shortfall</b> = the level the job requires − the employee's actual level.
              </li>
              <li>
                <b>Key skill</b> = one the employee's job requires at level 4, the highest level required in the data.
              </li>
              <li>
                <b>Critical gap</b> = 2 or more levels short on any skill, or any shortfall on a key skill. The key rule
                alone adds {fmt.num(d.key_only)} employees.
              </li>
            </ul>
          </Panel>
          <Grid>
            <Panel title="Skills with the most critical gaps" note="Share of each skill's assessments that are critical gaps">
              <Bars items={d.by_competency} format={fmt.pct} />
            </Panel>
            <Panel title="By department" note="Share of each department's employees with a critical gap">
              <Bars items={d.by_department} format={fmt.pct} />
            </Panel>
          </Grid>
          <Panel title="Every critical gap">
            <DataTable
              rows={d.rows}
              search={[...PERSON_SEARCH, 'competency_name']}
              filters={[...DEPT_FILTER, { key: 'competency_name', label: 'skills' }]}
              columns={[
                NAME, DEPARTMENT, { key: 'competency_name', label: 'Skill' },
                { key: 'required_level', label: 'Required', align: 'right', format: fmt.num },
                { key: 'proficiency_level', label: 'Actual', align: 'right', format: fmt.num },
                { key: 'shortfall', label: 'Shortfall', align: 'right', format: fmt.num },
                { key: 'is_key', label: 'Key skill', format: fmt.yes },
              ]}
            />
          </Panel>
          <Caveats
            items={[
              'Each employee was assessed on only 2 of 40 skills, so "no critical gap" means none among the assessed skills.',
              '"Key" and "critical" are our definitions; the data has no key-skill field. Reading the question literally (gaps on key skills only) would flag fewer employees.',
              "Required levels don't follow seniority in this data, so 'key' reflects each person's recorded requirement, not a role standard.",
            ]}
          />
        </Question>
      )}
    </Loaded>
  )
}

// ---------------------------------------------------------------- Q3 ---------

export function Q3() {
  const state = useApi('/api/q3')
  return (
    <Loaded state={state}>
      {(d) => {
        const tiers = Object.fromEntries(d.tiers.map((x) => [x.label, x.value]))
        return (
          <Question
            q="Q3"
            title="Who are the high-potential employees ready for promotion?"
            answer={
              <>
                <b>{fmt.num(d.candidates)}</b> employees are rated High potential and Ready Now.{' '}
                <b>{tiers.A}</b> are tier A (promote now), {tiers.B} tier B and {tiers.C} tier C.{' '}
                <b>{d.retention_priority}</b> tier-A candidates are also at high attrition risk.
              </>
            }
          >
            <Grid>
              <Panel title="Candidates by tier" note="A = latest rating 4-5 · B = 3 · C = 2 or lower, or no valid rating">
                <Bars items={d.tiers.map((x) => ({ ...x, label: `Tier ${x.label}` }))} />
              </Panel>
              <Panel title="Retention priorities" note="Tier A and high recorded attrition risk: promote first">
                <Stat label="retention priorities" value={d.retention_priority} />
              </Panel>
            </Grid>
            <Panel title="Ranked shortlist">
              <DataTable
                rows={d.rows}
                search={PERSON_SEARCH}
                filters={[...DEPT_FILTER, { key: 'tier', label: 'tiers' }]}
                columns={[
                  { key: 'rank', label: 'Rank', align: 'right', format: fmt.num },
                  NAME, DEPARTMENT, { key: 'job_level', label: 'Level' }, { key: 'tier', label: 'Tier' },
                  { key: 'latest_rating', label: 'Rating', align: 'right', format: fmt.num },
                  { key: 'certifications', label: 'Certs', align: 'right', format: fmt.num },
                  { key: 'attrition_risk', label: 'Attrition risk' },
                  { key: 'retention_priority', label: 'Retention priority', format: fmt.yes },
                ]}
              />
            </Panel>
            <Caveats
              items={[
                'Potential and readiness come from a single talent-review cycle.',
                'Tier C includes employees with no valid rating (reviews dated before hire); they may need a fresh review.',
                'Tiers are a recommendation for discussion. Open roles and business needs are not in the data.',
              ]}
            />
          </Question>
        )
      }}
    </Loaded>
  )
}

// ---------------------------------------------------------------- Q4 ---------

export function Q4() {
  const state = useApi('/api/q4')
  return (
    <Loaded state={state}>
      {(d) => (
        <Question
          q="Q4"
          title="What training and development has each employee completed?"
          answer={
            <>
              <b>{fmt.num(d.completed_any)}</b> employees ({fmt.pct(d.completed_any / d.employees)}) completed at least
              one course and <b>{fmt.num(d.certified_any)}</b> ({fmt.pct(d.certified_any / d.employees)}) earned a
              certification. In total: {fmt.num(d.completions)} course completions and {fmt.num(d.certifications)}{' '}
              certifications across {d.courses} courses.
            </>
          }
        >
          <Grid>
            <Panel title="Employees by number of courses completed">
              <Bars items={d.courses_histogram.map((x) => ({ ...x, label: `${x.label} courses` }))} />
            </Panel>
            <Panel title="Most completed courses" note="Employees who completed each course">
              <Bars items={d.top_courses} />
            </Panel>
          </Grid>
          <Panel title="By department">
            <DataTable
              rows={d.departments}
              columns={[
                DEPARTMENT, { key: 'employees', label: 'Employees', align: 'right', format: fmt.num },
                { key: 'completed_a_course', label: 'Completed a course', align: 'right', format: fmt.pct },
                { key: 'certified', label: 'Certified', align: 'right', format: fmt.pct },
                { key: 'courses_per_employee', label: 'Courses per employee', align: 'right', format: (v) => fmt.dec(v) },
              ]}
            />
          </Panel>
          <Panel title="Every employee" note="Open the Employee page to see the course list for one person">
            <DataTable
              rows={d.rows}
              search={PERSON_SEARCH}
              filters={DEPT_FILTER}
              columns={[
                NAME, DEPARTMENT,
                { key: 'courses_completed', label: 'Courses completed', align: 'right', format: fmt.num },
                { key: 'certifications', label: 'Certifications', align: 'right', format: fmt.num },
                { key: 'quizzes_passed', label: 'Quizzes passed', align: 'right', format: fmt.num },
                { key: 'avg_quiz_score', label: 'Avg quiz score', align: 'right', format: (v) => fmt.dec(v, 0) },
                { key: 'last_activity_date', label: 'Last activity' },
              ]}
            />
          </Panel>
          <Caveats
            items={[
              'There is no data on development plans or goals, so this covers training completed only.',
              'The records do not reliably show when people signed up for a course, so this does not say what share of learners finish what they start.',
              '76 learning events had no employee ID and could not be credited to anyone.',
            ]}
          />
        </Question>
      )}
    </Loaded>
  )
}

// ---------------------------------------------------------------- Q5 ---------

export function Q5() {
  const state = useApi('/api/q5')
  return (
    <Loaded state={state}>
      {(d) => {
        const t = Object.fromEntries(d.tiers.map((x) => [x.label, x.value]))
        return (
          <Question
            q="Q5"
            title="Which employees are at risk of attrition based on performance trends?"
            answer={
              <>
                <b>{t.Critical}</b> employees are <b>Critical</b>: their performance is slipping and HR already rated
                their attrition risk High. {t.High} more are High and {t.Watch} are on Watch.{' '}
                <b>{d.critical_high_potential}</b> of the Critical employees are high potential.
              </>
            }
          >
            <Grid>
              <Panel title="Employees by risk tier">
                <Bars items={d.tiers} />
              </Panel>
              <Panel
                title="How the tier is decided"
                note="Performance slipping = rating declined, or latest rating 2 or lower. Columns = HR's recorded attrition risk."
              >
                <table className="grid-table">
                  <thead>
                    <tr>
                      <th />
                      {d.grid.columns.map((c) => (
                        <th key={c} className="right">
                          {c} risk
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {d.grid.rows.map((r, i) => (
                      <tr key={r}>
                        <th>{r}</th>
                        {d.grid.values[i].map((v, j) => (
                          <td key={j} className="right">
                            {fmt.num(v)}
                            <div className="muted small">
                              {i === 0 ? ['Critical', 'High', 'Watch'][j] : ['High', 'Low', 'Low'][j]}
                            </div>
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Panel>
            </Grid>
            <Panel title="Watchlist" note="Sorted with the most urgent first">
              <DataTable
                rows={d.rows}
                search={PERSON_SEARCH}
                filters={[...DEPT_FILTER, { key: 'risk_tier', label: 'tiers' }]}
                columns={[
                  NAME, DEPARTMENT, { key: 'job_level', label: 'Level' },
                  { key: 'previous_rating', label: 'Previous', align: 'right', format: fmt.num },
                  { key: 'latest_period_rating', label: 'Latest', align: 'right', format: fmt.num },
                  { key: 'signal_reason', label: 'Performance warning' },
                  { key: 'recorded_attrition_risk', label: 'HR risk' },
                  { key: 'risk_tier', label: 'Tier' },
                  { key: 'high_potential', label: 'High potential', format: fmt.yes },
                ]}
              />
            </Panel>
            <Caveats
              items={[
                'A trend is one change between two periods: a direction, not a sustained decline.',
                'There is no data on who actually left, so the tiers say who to talk to, not who will leave.',
                'Employees without a rating trend are tiered on their recorded risk alone.',
              ]}
            />
          </Question>
        )
      }}
    </Loaded>
  )
}

// ---------------------------------------------------------------- Q6 ---------

export function Q6() {
  const state = useApi('/api/q6')
  return (
    <Loaded state={state}>
      {(d) => {
        const third = Object.fromEntries(d.thirds.map((x) => [x.rating_band, x]))
        return (
          <Question
            q="Q6"
            title="How does employee performance correlate with team and project outcomes?"
            answer={
              <>
                Teams in the <b>top third by rating</b> delivered <b>{fmt.pct(third['top third'].on_time_rate)}</b> of
                projects on time, against <b>{fmt.pct(third['bottom third'].on_time_rate)}</b> for the bottom third (
                {d.teams_with_projects} teams with {d.projects} projects).
              </>
            }
          >
            <Grid>
              <Panel title="On-time delivery by team rating" note="Teams split into thirds by their people's average rating">
                <Bars
                  items={d.thirds.map((x) => ({ label: `${x.rating_band} (avg ${fmt.dec(x.avg_rating)})`, value: x.on_time_rate }))}
                  format={fmt.pct}
                  max={1}
                />
              </Panel>
              <Panel title="On-time delivery by project type">
                <Bars
                  items={d.project_types.map((x) => ({ label: `${x.project_type} (${x.projects})`, value: x.on_time }))}
                  format={fmt.pct}
                  max={1}
                />
              </Panel>
            </Grid>
            <Panel title="Teams with projects">
              <DataTable
                rows={d.teams}
                search={['team_name', 'department']}
                filters={[...DEPT_FILTER, { key: 'rating_band', label: 'thirds' }]}
                columns={[
                  TEAM, DEPARTMENT, { key: 'headcount', label: 'People', align: 'right', format: fmt.num },
                  { key: 'avg_rating', label: 'Avg rating', align: 'right', format: (v) => fmt.dec(v) },
                  { key: 'low_performer_share', label: 'Low performers', align: 'right', format: fmt.pct },
                  { key: 'rating_band', label: 'Third' },
                  { key: 'projects', label: 'Projects', align: 'right', format: fmt.num },
                  { key: 'on_time_rate', label: 'On time', align: 'right', format: fmt.pct },
                ]}
              />
            </Panel>
            <Panel title="Projects">
              <DataTable
                rows={d.project_rows}
                search={['project_name', 'team_name']}
                columns={[
                  { key: 'project_name', label: 'Project' }, { key: 'project_type', label: 'Type' }, TEAM,
                  { key: 'outcome_status', label: 'Status' },
                  { key: 'outcome_score', label: 'Score', align: 'right', format: (v) => fmt.dec(v, 1) },
                  { key: 'team_avg_rating', label: 'Team rating', align: 'right', format: (v) => fmt.dec(v) },
                ]}
              />
            </Panel>
            <Caveats
              items={[
                `Small sample: ${d.teams_with_projects} teams and ${d.projects} projects. A few projects going the other way would change these percentages a lot.`,
                'There is no record of who worked on which project, only which team owned it.',
                'There are no project dates, so it is unclear whether ratings came before or after a delay. This is an association, not cause and effect.',
              ]}
            />
          </Question>
        )
      }}
    </Loaded>
  )
}

// ---------------------------------------------------------------- Q7 ---------

export function Q7() {
  const state = useApi('/api/q7')
  return (
    <Loaded state={state}>
      {(d) => (
        <Question
          q="Q7"
          title="What is the distribution of skills across the organization?"
          answer={
            <>
              <b>{fmt.pct(d.meets_share)}</b> of {fmt.num(d.assessments)} skill assessments meet the level the job
              requires. The weakest skill is <b>{d.rows[0].competency_name}</b> (average level{' '}
              {fmt.dec(d.rows[0].avg_proficiency)}); the strongest is{' '}
              <b>{d.rows[d.rows.length - 1].competency_name}</b> ({fmt.dec(d.rows[d.rows.length - 1].avg_proficiency)}).
            </>
          }
        >
          <Grid>
            <Panel title="Assessments at each proficiency level">
              <Bars items={d.levels} />
            </Panel>
            <Panel title="Average level by department" note="Departments differ very little; the differences are in individual skills">
              <Bars items={d.by_department} format={(v) => fmt.dec(v)} max={5} />
            </Panel>
          </Grid>
          <Panel title="Every skill" note="Sorted weakest first">
            <DataTable
              rows={d.rows}
              search={['competency_name', 'category']}
              filters={[{ key: 'category', label: 'categories' }]}
              pageSize={20}
              columns={[
                { key: 'competency_name', label: 'Skill' }, { key: 'category', label: 'Category' },
                { key: 'assessed', label: 'Assessed', align: 'right', format: fmt.num },
                { key: 'avg_proficiency', label: 'Avg level', align: 'right', format: (v) => fmt.dec(v) },
                { key: 'meets_required_share', label: 'Meets requirement', align: 'right', format: fmt.pct },
                { key: 'critical_gaps', label: 'Critical gaps', align: 'right', format: fmt.num },
                ...[1, 2, 3, 4, 5].map((i) => ({ key: `level_${i}`, label: `L${i}`, align: 'right', format: fmt.num })),
              ]}
            />
          </Panel>
          <Caveats
            items={[
              'This is a sample, not a full inventory: each employee was assessed on 2 of 40 skills (82-126 assessments per skill).',
              'Differences of a few hundredths between skills or departments are within noise.',
            ]}
          />
        </Question>
      )}
    </Loaded>
  )
}

// ---------------------------------------------------------------- employee ---

export function Employee() {
  const [input, setInput] = useState('58')
  const [id, setId] = useState('58')
  const state = useApi(id ? `/api/employee/${id}` : null)
  return (
    <section>
      <h2>Employee lookup</h2>
      <p className="muted">Everything the Gold layer knows about one person, across all seven questions.</p>
      <form
        className="toolbar"
        onSubmit={(e) => {
          e.preventDefault()
          setId(input.trim())
        }}
      >
        <input value={input} onChange={(e) => setInput(e.target.value)} placeholder="Employee ID, e.g. 58" />
        <button type="submit">Look up</button>
      </form>
      <Loaded state={state}>
        {(d) => {
          const p = d.profile
          return (
            <>
              <h3 className="person">
                {p.first_name} {p.last_name} <span className="muted">#{p.employee_id}</span>
              </h3>
              <p className="muted">
                {p.job_level} · {p.team_name} · {p.department}
              </p>
              <div className="stats">
                <Stat label="Q1 rating trend" value={fmt.title(p.trend)} detail={`latest rating ${fmt.num(p.latest_rating)}`} />
                <Stat
                  label="Q2 critical gaps"
                  value={d.skills.filter((s) => s.is_critical).length}
                  detail={`of ${d.skills.length} assessed skills`}
                />
                <Stat
                  label="Q3 promotion"
                  value={d.promotion ? `Tier ${d.promotion.tier}` : 'Not a candidate'}
                  detail={d.promotion ? `rank ${d.promotion.rank}` : 'not High potential + Ready Now'}
                />
                <Stat
                  label="Q4 courses completed"
                  value={fmt.num(d.learning?.courses_completed)}
                  detail={`${fmt.num(d.learning?.certifications)} certifications`}
                />
                <Stat label="Q5 attrition tier" value={d.attrition.risk_tier} detail={d.attrition.signal_reason} />
              </div>
              <Grid>
                <Panel title="Reviews (Q1)">
                  <DataTable
                    rows={d.reviews}
                    columns={[
                      { key: 'review_period', label: 'Period' },
                      { key: 'rating', label: 'Rating', align: 'right', format: fmt.num },
                      { key: 'manager_comment', label: 'Comment', format: (v) => <span className="wrap">{fmt.text(v)}</span> },
                      { key: 'flag_period_before_hire', label: 'Pre-hire', format: fmt.yes },
                    ]}
                  />
                </Panel>
                <Panel title="Skills (Q2)">
                  <DataTable
                    rows={d.skills}
                    columns={[
                      { key: 'competency_name', label: 'Skill' },
                      { key: 'required_level', label: 'Required', align: 'right', format: fmt.num },
                      { key: 'proficiency_level', label: 'Actual', align: 'right', format: fmt.num },
                      { key: 'is_key', label: 'Key', format: fmt.yes },
                      { key: 'is_critical', label: 'Critical', format: fmt.yes },
                    ]}
                  />
                </Panel>
              </Grid>
              <Panel title="Courses (Q4)">
                <DataTable
                  rows={d.courses}
                  columns={[
                    { key: 'course_name', label: 'Course' },
                    { key: 'completed_date', label: 'Completed' },
                    { key: 'certified_date', label: 'Certified' },
                  ]}
                />
              </Panel>
            </>
          )
        }}
      </Loaded>
    </section>
  )
}

// ---------------------------------------------------------------- pipeline ---

export function Pipeline() {
  const state = useApi('/api/pipeline')
  return (
    <section>
      <h2>Pipeline</h2>
      <p className="muted">From the run record the pipeline writes after every run.</p>
      <Loaded state={state}>
        {(d) => (
          <>
            {d.latest_run && (
              <p>
                Latest successful run <code>{d.latest_run.run_id}</code>, ended {fmt.time(d.latest_run.ended_at)}, took{' '}
                {fmt.dec(d.latest_run.duration_s, 1)} s.
              </p>
            )}
            <Panel title="Records through each step">
              <DataTable
                rows={d.steps}
                columns={[
                  { key: 'step', label: 'Step' }, { key: 'status', label: 'Status' },
                  { key: 'records_read', label: 'Read', align: 'right', format: fmt.num },
                  { key: 'records_written', label: 'Written', align: 'right', format: fmt.num },
                  { key: 'records_quarantined', label: 'Quarantined', align: 'right', format: fmt.num },
                  { key: 'duration_s', label: 'Seconds', align: 'right', format: (v) => fmt.dec(v) },
                ]}
              />
            </Panel>
            <Panel title={`Gold reconciliation checks (${d.checks.length})`}>
              <DataTable rows={d.checks} pageSize={20} columns={[{ key: 'check', label: 'Check' }, { key: 'result', label: 'Result' }]} />
            </Panel>
            <Panel title="Recent runs">
              <DataTable
                rows={d.runs}
                columns={[
                  { key: 'run_id', label: 'Run' }, { key: 'status', label: 'Status' }, { key: 'ended_at', label: 'Ended', format: fmt.time },
                  { key: 'duration_s', label: 'Seconds', align: 'right', format: (v) => fmt.dec(v, 1) },
                  { key: 'error', label: 'Error' },
                ]}
              />
            </Panel>
          </>
        )}
      </Loaded>
    </section>
  )
}

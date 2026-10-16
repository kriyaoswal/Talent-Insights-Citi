import { useEffect, useState } from 'react'

// The dashboard API: on AWS, CloudFront forwards /api/talent-dashboard/* to the
// talent-dashboard Lambda; locally, Vite forwards it to dashboard/api.py.
export const API_BASE = '/api/talent-dashboard'

/** Fetch one dashboard page, e.g. useApi('summary') or useApi('employee/58'). */
export function useApi(page) {
  const [state, setState] = useState({ loading: true })
  useEffect(() => {
    if (!page) return undefined
    let live = true
    setState({ loading: true })
    fetch(`${API_BASE}/${page}`)
      .then(async (res) => {
        const body = await res.json().catch(() => ({}))
        if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`)
        return body
      })
      .then((data) => live && setState({ data }))
      .catch((err) => live && setState({ error: err.message }))
    return () => {
      live = false
    }
  }, [page])
  return state
}

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The page calls /api/...; Vite forwards those requests to the Python API (dashboard/api.py).
const apiPort = process.env.DASHBOARD_PORT || '8510'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: { '/api': `http://127.0.0.1:${apiPort}` },
  },
})

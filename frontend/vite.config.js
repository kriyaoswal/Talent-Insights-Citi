import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    // Local development: the talent dashboard API (data/talent-etl/dashboard/api.py).
    // On AWS, CloudFront routes /api/talent-dashboard/* to the Lambda instead.
    proxy: {
      '/api/talent-dashboard': `http://127.0.0.1:${process.env.DASHBOARD_PORT || '8510'}`,
    },
  },
})

# Canvas Cron Jobs (Local)

Standalone cron jobs (ignored by main repo). Includes Inventory Queue Alert job using SKU resolver.

Setup:
1. cp .env.example .env
2. npm install
3. npm start

Env:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE
- POLL_CRON (default */10 * * * *)

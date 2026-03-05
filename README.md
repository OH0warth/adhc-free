# ADHC Free Cloud (Streamlit-only)

This version runs **entirely inside Streamlit** with a local SQLite database file (`adhc.db`).
No Docker, no Postgres, no Redis, no paid cloud services.

## Deploy (Free)
1. Push this repo to GitHub.
2. Go to Streamlit Community Cloud → New app.
3. Select this repo and set **Main file path** to `app.py`.
4. Deploy.

## What works in this free version
- Generate opportunities (simulated)
- CEO cycle (adopt/reject + create tasks)
- Mark tasks done
- Portfolio summary + audit log


### Streamlit Cloud note
Streamlit Community Cloud runs your repo in a mostly read-only directory. This app stores its SQLite DB at `/tmp/adhc.db` by default (writable).

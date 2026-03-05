import os
import json
import sqlite3
from datetime import datetime
import pandas as pd
import streamlit as st

DB_PATH = os.getenv("ADHC_DB_PATH", "/tmp/adhc.db")

def _ensure_db_dir():
    d = os.path.dirname(DB_PATH)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        problem TEXT NOT NULL,
        audience TEXT NOT NULL,
        monetization TEXT NOT NULL,
        score REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'new',
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        opportunity_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        stage TEXT NOT NULL DEFAULT 'incubating',
        budget_monthly_usd REAL NOT NULL DEFAULT 0,
        notes TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        FOREIGN KEY(opportunity_id) REFERENCES opportunities(id)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER,
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'queued',
        payload TEXT NOT NULL DEFAULT '{}',
        result TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(project_id) REFERENCES projects(id)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()

def audit(conn, actor: str, action: str, details: dict):
    conn.execute(
        "INSERT INTO audit_logs(actor, action, details, created_at) VALUES (?,?,?,?)",
        (actor, action, json.dumps(details), datetime.utcnow().isoformat()),
    )

def research_generate(n: int = 5):
    pool = [
        ("Local service lead router", "Small businesses miss calls/leads and lose revenue", "Local trades (plumbers, HVAC, etc.)", "Pay-per-lead / monthly"),
        ("Etsy listing optimizer", "Sellers struggle with SEO/copy and ranking", "Etsy sellers", "Subscription"),
        ("Meeting follow-up autopilot", "Teams forget action items after meetings", "SMBs / agencies", "Subscription"),
        ("Job applicant tracker", "Candidates lose track of applications and follow-ups", "Job seekers", "Freemium + upgrade"),
        ("Shopify profit dashboard", "Store owners don't know true profit after fees/ads", "Shopify merchants", "Subscription"),
        ("Clinic no-show reducer", "Appointment no-shows cost clinics money", "Dental/medical clinics", "Monthly + per-SMS"),
    ]
    conn = db()
    cur = conn.cursor()
    created = 0
    for i in range(n):
        title, problem, audience, monetization = pool[i % len(pool)]
        score = float(65 + (i * 3) % 30)  # 65-94
        cur.execute(
            """INSERT INTO opportunities(title, problem, audience, monetization, score, status, metadata, created_at)
                 VALUES (?,?,?,?,?,?,?,?)""",
            (title, problem, audience, monetization, score, "new", json.dumps({"source": "research_agent"}), datetime.utcnow().isoformat())
        )
        created += 1
    conn.commit()
    conn.close()
    audit("research_agent", "generate_opportunities", {"n": n, "created": created})

def ceo_cycle(max_new: int = 10, adopt_threshold: float = 75.0, mode: str = "manual"):
    conn = db()
    rows = conn.execute(
        "SELECT * FROM opportunities WHERE status='new' ORDER BY score DESC LIMIT ?",
        (max_new,)
    ).fetchall()

    adopted = []
    tasks_created = 0

    for r in rows:
        decision = "adopt" if float(r["score"]) >= adopt_threshold else "reject"
        if decision == "adopt":
            conn.execute("UPDATE opportunities SET status='adopted' WHERE id=?", (r["id"],))
            cur = conn.execute(
                """INSERT INTO projects(opportunity_id, name, stage, budget_monthly_usd, notes, created_at)
                     VALUES (?,?,?,?,?,?)""",
                (r["id"], r["title"], "incubating", 250.0, "Auto-adopted by CEO agent.", datetime.utcnow().isoformat())
            )
            project_id = cur.lastrowid
            adopted.append(r["title"])

            seed_tasks = [
                ("build", "MVP checklist", "Define MVP scope + tech plan"),
                ("marketing", "Launch landing page", "Create landing page + waitlist"),
                ("research", "Validate demand", "Find 20 prospects + run interviews"),
            ]
            for ttype, title, desc in seed_tasks:
                conn.execute(
                    """INSERT INTO tasks(project_id, type, title, description, status, payload, result, created_at, updated_at)
                         VALUES (?,?,?,?,?,?,?,?,?)""",
                    (project_id, ttype, title, desc, "queued", json.dumps({"from": "ceo_agent", "mode": mode}), "{}", datetime.utcnow().isoformat(), datetime.utcnow().isoformat())
                )
                tasks_created += 1
        else:
            conn.execute("UPDATE opportunities SET status='rejected' WHERE id=?", (r["id"],))
        audit("ceo_agent", "review_opportunity", {"opportunity_id": r["id"], "decision": decision, "score": r["score"]})

    conn.commit()
    conn.close()
    return {"mode": mode, "opportunities_reviewed": len(rows), "adopted_projects": adopted, "tasks_created": tasks_created}

st.set_page_config(page_title="ADHC (Free Cloud)", layout="wide")
init_db()

st.title("ADHC — Autonomous Digital Holding Company (Free Cloud MVP)")

with st.sidebar:
    st.subheader("Controls")
    gen_n = st.number_input("Generate opportunities", min_value=1, max_value=25, value=5, step=1)
    if st.button("Generate"):
        research_generate(int(gen_n))
        st.success("Opportunities generated.")

    st.divider()
    max_new = st.number_input("CEO cycle: max new to review", min_value=1, max_value=50, value=10, step=1)
    threshold = st.slider("Adopt threshold", min_value=0, max_value=100, value=75, step=1)
    if st.button("Run CEO cycle"):
        out = ceo_cycle(int(max_new), float(threshold), mode="manual")
        st.success(f"Cycle done: adopted {len(out['adopted_projects'])}, tasks {out['tasks_created']}")

    st.divider()
    if st.button("Reset DB (danger)"):
        try:
            os.remove(DB_PATH)
        except FileNotFoundError:
            pass
        init_db()
        st.warning("Database reset.")

conn = db()
colA, colB, colC = st.columns(3)
projects_total = conn.execute("SELECT COUNT(*) c FROM projects").fetchone()["c"]
opps_total = conn.execute("SELECT COUNT(*) c FROM opportunities").fetchone()["c"]
tasks_total = conn.execute("SELECT COUNT(*) c FROM tasks").fetchone()["c"]
colA.metric("Opportunities", opps_total)
colB.metric("Projects", projects_total)
colC.metric("Tasks", tasks_total)

st.divider()

st.subheader("Opportunities")
opps = conn.execute("SELECT * FROM opportunities ORDER BY datetime(created_at) DESC LIMIT 200").fetchall()
df_opps = pd.DataFrame([dict(r) for r in opps])
if not df_opps.empty:
    st.dataframe(df_opps, use_container_width=True)
else:
    st.info("No opportunities yet. Click Generate in the sidebar.")

st.divider()

st.subheader("Projects")
projs = conn.execute("SELECT * FROM projects ORDER BY datetime(created_at) DESC LIMIT 200").fetchall()
df_proj = pd.DataFrame([dict(r) for r in projs])
if not df_proj.empty:
    st.dataframe(df_proj, use_container_width=True)
else:
    st.info("No projects yet. Run CEO cycle after generating opportunities.")

st.divider()

st.subheader("Tasks")
tasks = conn.execute("SELECT * FROM tasks ORDER BY datetime(created_at) DESC LIMIT 300").fetchall()
df_tasks = pd.DataFrame([dict(r) for r in tasks])
if not df_tasks.empty:
    st.dataframe(df_tasks, use_container_width=True)

    st.markdown("### Update a task status")
    task_ids = df_tasks["id"].tolist()
    selected = st.selectbox("Task ID", task_ids)
    new_status = st.selectbox("New status", ["queued", "running", "done", "failed", "cancelled"])
    if st.button("Save status"):
        conn.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?", (new_status, datetime.utcnow().isoformat(), int(selected)))
        conn.commit()
        audit("system", "update_task_status", {"task_id": int(selected), "status": new_status})
        st.success("Updated. Refresh the page.")
else:
    st.info("No tasks yet.")

st.divider()

st.subheader("Audit log (latest 50)")
logs = conn.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 50").fetchall()
df_logs = pd.DataFrame([dict(r) for r in logs])
if not df_logs.empty:
    st.dataframe(df_logs, use_container_width=True)
else:
    st.info("No audit logs yet.")

conn.close()

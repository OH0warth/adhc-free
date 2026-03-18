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
    _ensure_db_dir()
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()


def ensure_project_columns():
    conn = db()
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]

    if "mrr" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN mrr REAL NOT NULL DEFAULT 0")
    if "traffic" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN traffic REAL NOT NULL DEFAULT 0")
    if "cashflow_score" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN cashflow_score REAL NOT NULL DEFAULT 0")
    if "automation_score" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN automation_score REAL NOT NULL DEFAULT 0")
    if "profit_potential" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN profit_potential REAL NOT NULL DEFAULT 0")
    if "priority" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'")

    conn.commit()
    conn.close()


def get_setting(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn, key: str, value: str):
    conn.execute(
        """
        INSERT INTO settings(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )


def audit(conn, actor: str, action: str, details: dict):
    conn.execute(
        "INSERT INTO audit_logs(actor, action, details, created_at) VALUES (?,?,?,?)",
        (actor, action, json.dumps(details), datetime.utcnow().isoformat()),
    )


def research_generate(n: int = 5):
    pool = [
        (
            "Local service lead router",
            "Small businesses miss calls and lose leads before they can respond.",
            "Local trades (plumbers, HVAC, electricians, roofers)",
            "Pay-per-lead / monthly retainer",
        ),
        (
            "Clinic no-show reducer",
            "Clinics lose money from missed appointments and wasted staff time.",
            "Dentists, physiotherapists, private clinics",
            "Monthly subscription",
        ),
        (
            "Etsy listing optimizer",
            "Sellers struggle with SEO, titles, and conversion copy.",
            "Etsy sellers",
            "Subscription",
        ),
        (
            "Shopify profit dashboard",
            "Store owners do not know true profit after fees, ads, and returns.",
            "Shopify merchants",
            "Subscription",
        ),
        (
            "Freelancer proposal assistant",
            "Freelancers waste time writing repetitive proposals.",
            "Freelancers and solo consultants",
            "Subscription",
        ),
        (
            "Podcast guest outreach CRM",
            "Podcast hosts lose track of guest outreach and follow-ups.",
            "Podcast creators",
            "Subscription",
        ),
        (
            "Emergency roofer quote page",
            "Homeowners need urgent roofing help and companies pay high lead values.",
            "Roofing companies",
            "Pay-per-lead",
        ),
        (
            "Solar quote matcher",
            "Solar installers need motivated homeowners actively requesting quotes.",
            "Solar installers",
            "Pay-per-lead",
        ),
        (
            "Family law lead page",
            "Law firms pay heavily for qualified inbound leads.",
            "Family law solicitors",
            "Pay-per-lead",
        ),
        (
            "Dental implant lead page",
            "Dental clinics want high-value treatment leads.",
            "Cosmetic and implant dentists",
            "Pay-per-lead / appointment fee",
        ),
    ]

    conn = db()
    cur = conn.cursor()
    created = 0

    for i in range(n):
        title, problem, audience, monetization = pool[i % len(pool)]
        score = float(68 + (i * 5) % 28)  # 68-95

        cur.execute(
            """
            INSERT INTO opportunities(title, problem, audience, monetization, score, status, metadata, created_at)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                title,
                problem,
                audience,
                monetization,
                score,
                "new",
                json.dumps({"source": "research_agent"}),
                datetime.utcnow().isoformat(),
            ),
        )
        created += 1

    audit(conn, "research_agent", "generate_opportunities", {"n": n, "created": created})
    conn.commit()
    conn.close()


def ceo_cycle(max_new: int = 10, adopt_threshold: float = 75.0, mode: str = "manual"):
    conn = db()
    rows = conn.execute(
        "SELECT * FROM opportunities WHERE status='new' ORDER BY score DESC LIMIT ?",
        (max_new,),
    ).fetchall()

    adopted = []
    tasks_created = 0

    for r in rows:
        decision = "adopt" if float(r["score"]) >= adopt_threshold else "reject"

        if decision == "adopt":
            conn.execute("UPDATE opportunities SET status='adopted' WHERE id=?", (r["id"],))
            cur = conn.execute(
                """
                INSERT INTO projects(opportunity_id, name, stage, budget_monthly_usd, notes, created_at)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    r["id"],
                    r["title"],
                    "incubating",
                    250.0,
                    "Auto-adopted by CEO agent.",
                    datetime.utcnow().isoformat(),
                ),
            )
            project_id = cur.lastrowid
            adopted.append(r["title"])

            seed_tasks = [
                ("build", "Build the first lead-gen asset", "Define MVP lead asset structure and launch steps."),
                ("marketing", "Launch landing page", "Generate landing page copy, lead form fields, keywords, and outreach."),
                ("research", "Validate demand", "Find objections, customer pains, and basic competitor positioning."),
            ]

            for ttype, title, desc in seed_tasks:
                conn.execute(
                    """
                    INSERT INTO tasks(project_id, type, title, description, status, payload, result, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        project_id,
                        ttype,
                        title,
                        desc,
                        "queued",
                        json.dumps({"from": "ceo_agent", "mode": mode}),
                        "{}",
                        datetime.utcnow().isoformat(),
                        datetime.utcnow().isoformat(),
                    ),
                )
                tasks_created += 1
        else:
            conn.execute("UPDATE opportunities SET status='rejected' WHERE id=?", (r["id"],))

        audit(
            conn,
            "ceo_agent",
            "review_opportunity",
            {
                "opportunity_id": r["id"],
                "decision": decision,
                "score": r["score"],
            },
        )

    conn.commit()
    conn.close()

    return {
        "mode": mode,
        "opportunities_reviewed": len(rows),
        "adopted_projects": adopted,
        "tasks_created": tasks_created,
    }


def execute_task(task_id: int):
    conn = db()
    task = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()

    if not task:
        conn.close()
        return

    ttype = task["type"]

    if ttype == "build":
        result = {
            "deliverable": "Lead Gen Asset Build Kit",
            "landing_page_sections": [
                "Hero headline",
                "Problem",
                "Solution",
                "Benefits",
                "How it works",
                "CTA",
                "Lead capture form",
            ],
            "tech_stack": [
                "Carrd landing page",
                "Tally form",
                "Google Sheets lead tracker",
                "Email inbox for lead notifications",
            ],
            "launch_steps": [
                "Create landing page",
                "Create form",
                "Connect CTA button to form",
                "Publish page",
                "Send first 50 outreach messages",
                "Track replies and objections",
            ],
            "risks": [
                "Weak niche demand",
                "Low outreach response rate",
                "Poor value proposition clarity",
            ],
        }

    elif ttype == "marketing":
        result = {
            "landing_page": {
                "headline": "Reduce Missed Appointments by 70% — Automatically",
                "subheadline": "Automated SMS reminders for busy clinics that want fewer no-shows and more revenue.",
                "cta": "Book Free Demo",
                "trust_line": "No setup headaches. Works for busy clinics.",
                "problem_section": "Missed appointments cost clinics money, waste staff time, and create gaps in the day.",
                "solution_section": "Automatic SMS reminders confirm patients before they miss their appointment.",
                "benefits": [
                    "Reduce no-shows",
                    "Save staff time",
                    "Improve patient communication",
                    "Increase clinic revenue",
                ],
                "mockup": "Hi Sarah — your appointment is tomorrow at 2:00 PM. Reply YES to confirm.",
            },
            "form_fields": [
                "Clinic Name",
                "Contact Name",
                "Email",
                "Phone",
                "Patients per month",
            ],
            "google_ads_keywords": [
                "appointment reminder software",
                "reduce clinic no shows",
                "sms reminder for dentists",
                "patient reminder system",
            ],
            "cold_outreach_email": """Subject: quick question about missed appointments

Hi,

Quick question.

Do you currently send automated SMS reminders to patients?

I'm testing a simple system that helps clinics reduce missed appointments and save staff time.

Would you be open to a free demo?

Best,""",
            "follow_up_email": """Subject: following up

Hi,

Just checking if reducing missed appointments is something you're currently working on.

Happy to send over a quick demo link if useful.

Best,""",
            "lead_buyer_pitch": """Hi,

I’m testing a clinic automation system focused on reducing no-shows and improving appointment confirmations.

Would you be open to seeing a quick demo this week?""",
        }

    elif ttype == "research":
        result = {
            "deliverable": "Validation Pack",
            "questions": [
                "How do you currently remind patients?",
                "How often do patients miss appointments?",
                "What does a missed appointment cost you?",
                "Would you pay monthly to reduce that problem?",
            ],
            "niche_checks": [
                "Look for competitors charging monthly",
                "Check if clinics already use reminder tools",
                "Find what users complain about in reviews",
            ],
            "target_customers": [
                "dentists",
                "physiotherapists",
                "private clinics",
                "cosmetic clinics",
            ],
            "validation_goal": "Get 5 positive replies from clinics",
        }

    else:
        result = {"note": "No executor for this task type yet."}

    conn.execute(
        "UPDATE tasks SET status=?, result=?, updated_at=? WHERE id=?",
        ("done", json.dumps(result), datetime.utcnow().isoformat(), task_id),
    )

    audit(conn, "executor", "execute_task", {"task_id": task_id, "type": ttype})
    conn.commit()
    conn.close()


def update_cashflow_score(project_id: int):
    conn = db()
    proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()

    if not proj:
        conn.close()
        return

    mrr = float(proj["mrr"])
    traffic = float(proj["traffic"])
    stage = proj["stage"]

    stage_bonus = {
        "incubating": 10,
        "building": 20,
        "marketing": 35,
        "operating": 50,
        "paused": 5,
        "killed": 0,
    }.get(stage, 0)

    score = min(100.0, stage_bonus + (mrr / 100.0) + (traffic / 200.0))

    conn.execute(
        "UPDATE projects SET cashflow_score=? WHERE id=?",
        (score, project_id),
    )
    audit(conn, "analytics_agent", "update_cashflow_score", {"project_id": project_id, "cashflow_score": score})
    conn.commit()
    conn.close()


def update_project_scores(project_id: int):
    conn = db()
    proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()

    if not proj:
        conn.close()
        return

    name = (proj["name"] or "").lower()
    mrr = float(proj["mrr"])
    traffic = float(proj["traffic"])
    stage = proj["stage"]

    automation_score = 50.0
    profit_potential = 50.0

    if "clinic" in name or "lead" in name or "router" in name:
        automation_score += 20
        profit_potential += 20

    if "roof" in name or "solar" in name or "law" in name or "dental" in name:
        automation_score += 15
        profit_potential += 25

    if "shopify" in name or "dashboard" in name:
        automation_score += 10
        profit_potential += 10

    if "freelancer" in name or "proposal" in name:
        automation_score += 5
        profit_potential += 5

    if mrr > 0:
        profit_potential += min(20, mrr / 100)

    if traffic > 0:
        automation_score += min(15, traffic / 200)

    if stage == "operating":
        profit_potential += 10
    elif stage == "marketing":
        profit_potential += 5

    automation_score = min(100, automation_score)
    profit_potential = min(100, profit_potential)

    priority = "high" if (automation_score + profit_potential) / 2 >= 75 else "normal"

    conn.execute(
        "UPDATE projects SET automation_score=?, profit_potential=?, priority=? WHERE id=?",
        (automation_score, profit_potential, priority, project_id),
    )

    audit(
        conn,
        "analytics_agent",
        "update_project_scores",
        {
            "project_id": project_id,
            "automation_score": automation_score,
            "profit_potential": profit_potential,
            "priority": priority,
        },
    )

    conn.commit()
    conn.close()


def promote_project_stage(project_id: int):
    conn = db()
    proj = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()

    if not proj:
        conn.close()
        return

    current_stage = proj["stage"]
    mrr = float(proj["mrr"])
    traffic = float(proj["traffic"])

    if mrr >= 1000:
        new_stage = "operating"
    elif traffic >= 500:
        new_stage = "marketing"
    else:
        new_stage = "building"

    if current_stage != new_stage:
        conn.execute("UPDATE projects SET stage=? WHERE id=?", (new_stage, project_id))
        audit(conn, "ceo_agent", "promote_project_stage", {"project_id": project_id, "from": current_stage, "to": new_stage})

    conn.commit()
    conn.close()
    update_cashflow_score(project_id)


# -----------------------
# UI
# -----------------------

st.set_page_config(page_title="ADHC (Cashflow Cloud MVP)", layout="wide")

init_db()
ensure_project_columns()

st.title("ADHC — Autonomous Digital Holding Company (Cashflow + Lead Gen MVP)")

with st.sidebar:
    st.subheader("Auto-run")
    auto_run = st.toggle("Auto-run on page load", value=False)
    auto_hours = st.number_input("Run at most once every (hours)", min_value=1, max_value=168, value=6)
    min_opps = st.number_input("Keep at least this many NEW opportunities", min_value=1, max_value=100, value=10)

    st.divider()
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
        ensure_project_columns()
        st.warning("Database reset.")

conn = db()

if auto_run:
    last = get_setting(conn, "last_autorun", "1970-01-01T00:00:00")
    last_dt = datetime.fromisoformat(last)
    hours_since = (datetime.utcnow() - last_dt).total_seconds() / 3600

    if hours_since >= float(auto_hours):
        new_count = conn.execute("SELECT COUNT(*) c FROM opportunities WHERE status='new'").fetchone()["c"]
        if new_count < int(min_opps):
            research_generate(int(min_opps) - int(new_count))

        ceo_cycle(max_new=20, adopt_threshold=75.0, mode="auto")

        set_setting(conn, "last_autorun", datetime.utcnow().isoformat())
        conn.commit()
        st.toast("Auto-run completed", icon="✅")

projects_total = conn.execute("SELECT COUNT(*) c FROM projects").fetchone()["c"]
opps_total = conn.execute("SELECT COUNT(*) c FROM opportunities").fetchone()["c"]
tasks_total = conn.execute("SELECT COUNT(*) c FROM tasks").fetchone()["c"]
portfolio_mrr = conn.execute("SELECT COALESCE(SUM(mrr), 0) c FROM projects").fetchone()["c"]

colA, colB, colC, colD = st.columns(4)
colA.metric("Opportunities", opps_total)
colB.metric("Projects", projects_total)
colC.metric("Tasks", tasks_total)
colD.metric("Portfolio MRR", f"${portfolio_mrr:,.0f}")

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

    st.markdown("### Portfolio ranking")
    ranked = conn.execute("""
        SELECT id, name, stage, mrr, traffic, cashflow_score, automation_score, profit_potential, priority
        FROM projects
        ORDER BY
            CASE WHEN priority = 'high' THEN 1 ELSE 2 END,
            profit_potential DESC,
            automation_score DESC,
            cashflow_score DESC
    """).fetchall()

    df_ranked = pd.DataFrame([dict(r) for r in ranked])
    if not df_ranked.empty:
        st.dataframe(df_ranked, use_container_width=True)

        top = df_ranked.iloc[0]
        st.markdown("### Top recommendation")
        st.success(
            f"Focus next on: {top['name']} | "
            f"Priority: {top['priority']} | "
            f"Profit Potential: {top['profit_potential']} | "
            f"Automation Score: {top['automation_score']}"
        )

    st.markdown("### Cashflow controls")
    proj_ids = df_proj["id"].tolist()
    pid = st.selectbox("Project ID", proj_ids)

    mrr = st.number_input("Set MRR ($)", min_value=0.0, value=0.0, step=50.0)
    traffic = st.number_input("Set Traffic (monthly visits)", min_value=0.0, value=0.0, step=100.0)

    if st.button("Save project metrics"):
        conn.execute(
            "UPDATE projects SET mrr=?, traffic=? WHERE id=?",
            (float(mrr), float(traffic), int(pid)),
        )
        audit(
            conn,
            "system",
            "update_project_metrics",
            {
                "project_id": int(pid),
                "mrr": float(mrr),
                "traffic": float(traffic),
            },
        )
        conn.commit()
        update_cashflow_score(int(pid))
        promote_project_stage(int(pid))
        update_project_scores(int(pid))
        st.success("Saved and scored.")

    if st.button("Recalculate all project scores"):
        for proj_id in proj_ids:
            update_cashflow_score(int(proj_id))
            promote_project_stage(int(proj_id))
            update_project_scores(int(proj_id))
        st.success("All scores updated.")
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
    if st.button("Save task status"):
        conn.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
            (new_status, datetime.utcnow().isoformat(), int(selected)),
        )
        audit(conn, "system", "update_task_status", {"task_id": int(selected), "status": new_status})
        conn.commit()
        st.success("Updated.")

    st.markdown("### Execute a task")
    exec_id = st.selectbox("Execute Task ID", task_ids, key="exec_task_id")
    if st.button("Execute selected task"):
        execute_task(int(exec_id))
        st.success("Task executed. Refresh or scroll to inspect results in the Tasks table.")
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

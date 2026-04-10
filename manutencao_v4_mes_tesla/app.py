import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = "manutencao_v4.db"


# --------------------------- PAGE CONFIG ---------------------------
st.set_page_config(
    page_title="MES Maintenance V4",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --------------------------- STYLES ---------------------------
def inject_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg: #0b1220;
            --bg-soft: rgba(20, 30, 48, 0.82);
            --card: rgba(17, 24, 39, 0.86);
            --line: rgba(148, 163, 184, 0.18);
            --text: #e5eefc;
            --muted: #8ea0b8;
            --accent: #38bdf8;
            --accent-2: #22d3ee;
            --good: #22c55e;
            --warn: #f59e0b;
            --bad: #ef4444;
            --violet: #8b5cf6;
        }

        .stApp {
            background:
              radial-gradient(circle at top right, rgba(56,189,248,0.13), transparent 24%),
              radial-gradient(circle at top left, rgba(139,92,246,0.10), transparent 25%),
              linear-gradient(180deg, #08111d 0%, #0b1220 55%, #0f172a 100%);
            color: var(--text);
        }

        [data-testid="stHeader"] {
            background: transparent;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(8,17,29,0.96), rgba(15,23,42,0.96));
            border-right: 1px solid var(--line);
        }

        .hero {
            background: linear-gradient(135deg, rgba(13,26,44,0.88), rgba(10,18,32,0.95));
            border: 1px solid rgba(56,189,248,0.18);
            border-radius: 22px;
            padding: 26px 28px;
            box-shadow: 0 0 0 1px rgba(255,255,255,0.02) inset, 0 12px 32px rgba(2,6,23,0.35);
            margin-bottom: 18px;
        }

        .hero h1 {
            color: #f8fbff;
            font-size: 34px;
            margin: 0;
            letter-spacing: .4px;
        }

        .hero p {
            color: var(--muted);
            margin-top: 8px;
            margin-bottom: 0;
            font-size: 14px;
        }

        .kpi {
            background: linear-gradient(180deg, rgba(17,24,39,0.88), rgba(9,14,24,0.92));
            border: 1px solid rgba(56,189,248,0.15);
            border-radius: 20px;
            padding: 18px 18px;
            min-height: 120px;
            box-shadow: 0 10px 25px rgba(2,6,23,0.28), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .kpi .label {
            color: var(--muted);
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .kpi .value {
            color: #f8fbff;
            font-size: 34px;
            font-weight: 700;
            margin-top: 10px;
            line-height: 1;
        }

        .kpi .sub {
            color: #8ce0ff;
            font-size: 13px;
            margin-top: 10px;
        }

        .panel {
            background: var(--bg-soft);
            border: 1px solid var(--line);
            border-radius: 20px;
            padding: 18px;
            margin-bottom: 16px;
            box-shadow: 0 8px 22px rgba(2,6,23,0.25);
        }

        .section-title {
            font-size: 20px;
            font-weight: 700;
            color: #f8fbff;
            margin-bottom: 12px;
        }

        .status-chip {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: .3px;
            border: 1px solid rgba(255,255,255,0.08);
        }

        .chip-open { background: rgba(245,158,11,.16); color: #ffd58a; }
        .chip-stop { background: rgba(239,68,68,.16); color: #ff9e9e; }
        .chip-work { background: rgba(56,189,248,.16); color: #9de7ff; }
        .chip-pause { background: rgba(139,92,246,.16); color: #cfb6ff; }
        .chip-done { background: rgba(34,197,94,.16); color: #9ff0bb; }

        div[data-testid="stMetric"] {
            background: transparent;
            border: none;
        }

        .small-note {
            color: var(--muted);
            font-size: 12px;
            margin-top: -4px;
        }

        .footer-note {
            color: var(--muted);
            font-size: 12px;
            text-align: center;
            padding: 10px 0 24px 0;
        }

        .stButton > button, .stDownloadButton > button {
            border-radius: 12px;
            border: 1px solid rgba(56,189,248,0.28);
            background: linear-gradient(180deg, rgba(17,24,39,0.95), rgba(8,14,23,0.95));
            color: #eaf7ff;
            font-weight: 600;
        }

        .stButton > button:hover, .stDownloadButton > button:hover {
            border-color: rgba(56,189,248,0.56);
            color: white;
        }

        .stTextInput input, .stTextArea textarea, .stSelectbox div[data-baseweb="select"] > div,
        .stNumberInput input, .stDateInput input, .stTimeInput input {
            background: rgba(10,18,32,0.92) !important;
            color: #eff6ff !important;
            border-radius: 12px !important;
            border: 1px solid rgba(148,163,184,.18) !important;
        }

        .stDataFrame, .stTable {
            border-radius: 18px;
            overflow: hidden;
        }

        .alert-banner {
            padding: 16px 18px;
            border-radius: 16px;
            border: 1px solid rgba(239,68,68,0.28);
            background: linear-gradient(90deg, rgba(127,29,29,0.35), rgba(69,10,10,0.35));
            color: #fecaca;
            font-weight: 700;
            margin-bottom: 12px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# --------------------------- DB LAYER ---------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            full_name TEXT NOT NULL,
            hourly_rate REAL DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS machines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            sector TEXT NOT NULL,
            criticality TEXT NOT NULL,
            standard_mtbf_hours REAL DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            description TEXT NOT NULL,
            stock_qty REAL DEFAULT 0,
            unit_cost REAL DEFAULT 0,
            min_stock REAL DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS work_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            os_code TEXT UNIQUE NOT NULL,
            open_datetime TEXT NOT NULL,
            requester TEXT NOT NULL,
            sector TEXT NOT NULL,
            machine_code TEXT NOT NULL,
            machine_name TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            criticality TEXT NOT NULL,
            description TEXT NOT NULL,
            priority TEXT NOT NULL,
            status TEXT NOT NULL,
            assigned_technician TEXT,
            start_datetime TEXT,
            pause_datetime TEXT,
            end_datetime TEXT,
            downtime_minutes REAL DEFAULT 0,
            response_minutes REAL DEFAULT 0,
            repair_minutes REAL DEFAULT 0,
            paused_minutes REAL DEFAULT 0,
            root_cause TEXT,
            action_taken TEXT,
            observations TEXT,
            labor_cost REAL DEFAULT 0,
            parts_cost REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            whatsapp_alert_sent INTEGER DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS work_order_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            os_code TEXT NOT NULL,
            part_code TEXT NOT NULL,
            part_description TEXT NOT NULL,
            qty_used REAL NOT NULL,
            unit_cost REAL NOT NULL,
            total_cost REAL NOT NULL,
            used_at TEXT NOT NULL,
            used_by TEXT
        )
        """
    )

    conn.commit()

    seed_defaults(conn)
    conn.close()


def seed_defaults(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    users = [
        ("operador", "1234", "Operador", "Operador Padrão", 0),
        ("manutencao", "1234", "Manutenção", "Técnico Padrão", 85),
        ("gestor", "1234", "Gestor", "Gestor Padrão", 120),
    ]
    for item in users:
        cur.execute(
            "INSERT OR IGNORE INTO users (username, password, role, full_name, hourly_rate) VALUES (?, ?, ?, ?, ?)",
            item,
        )

    machines = [
        ("PRD-001", "Prensa Hidráulica 01", "Estamparia", "Alta", 120),
        ("SLT-004", "Slitter 04", "Conversão", "Alta", 96),
        ("EMB-002", "Linha de Embutir 02", "Montagem", "Média", 80),
        ("BAN-003", "Bancada CNC 03", "Usinagem", "Média", 72),
    ]
    for item in machines:
        cur.execute(
            "INSERT OR IGNORE INTO machines (code, name, sector, criticality, standard_mtbf_hours) VALUES (?, ?, ?, ?, ?)",
            item,
        )

    parts = [
        ("ROL-6205", "Rolamento 6205", 12, 34.5, 4),
        ("COR-A45", "Correia A45", 8, 52.0, 3),
        ("SEN-IND", "Sensor Indutivo", 5, 118.9, 2),
        ("FUS-10A", "Fusível 10A", 30, 4.2, 10),
    ]
    for item in parts:
        cur.execute(
            "INSERT OR IGNORE INTO parts (code, description, stock_qty, unit_cost, min_stock) VALUES (?, ?, ?, ?, ?)",
            item,
        )

    conn.commit()


# --------------------------- HELPERS ---------------------------
def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def execute(sql: str, params: tuple = ()) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    conn.close()


def fetch_one(sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    conn.close()
    return row


def fetch_all(sql: str, params: tuple = ()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def fmt_minutes(minutes: float) -> str:
    if minutes is None:
        return "0 min"
    minutes = max(0, float(minutes))
    h = int(minutes // 60)
    m = int(minutes % 60)
    if h > 0:
        return f"{h}h {m}min"
    return f"{m} min"


def status_chip(status: str) -> str:
    mapping = {
        "Aberta": "chip-open",
        "Máquina Parada": "chip-stop",
        "Em Manutenção": "chip-work",
        "Pausada": "chip-pause",
        "Finalizada": "chip-done",
    }
    css = mapping.get(status, "chip-open")
    return f'<span class="status-chip {css}">{status}</span>'


def generate_os_code() -> str:
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.now().strftime("%Y%m%d")
    cur.execute("SELECT COUNT(*) AS total FROM work_orders WHERE os_code LIKE ?", (f"OS-{today}-%",))
    total = cur.fetchone()[0] + 1
    conn.close()
    return f"OS-{today}-{total:03d}"


def calculate_live_minutes(start_value: Optional[str]) -> float:
    start_dt = parse_dt(start_value)
    if not start_dt:
        return 0.0
    return round((datetime.now() - start_dt).total_seconds() / 60, 1)


def current_user_full_name() -> str:
    return st.session_state.get("full_name", "Usuário")


# --------------------------- AUTH ---------------------------
def login_screen() -> None:
    st.markdown(
        """
        <div class="hero">
            <h1>⚙️ MES Maintenance V4</h1>
            <p>Monitoramento de paradas, OS em tempo real, custos, estoque e indicadores industriais em visual sala de controle.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Acesso ao sistema")
        with st.form("login_form"):
            username = st.text_input("Usuário")
            password = st.text_input("Senha", type="password")
            submit = st.form_submit_button("Entrar")
        st.markdown('<p class="small-note">Usuários iniciais: operador / manutencao / gestor | senha: 1234</p>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        if submit:
            row = fetch_one(
                "SELECT username, role, full_name FROM users WHERE username = ? AND password = ?",
                (username.strip(), password),
            )
            if row:
                st.session_state["logged_in"] = True
                st.session_state["username"] = row["username"]
                st.session_state["role"] = row["role"]
                st.session_state["full_name"] = row["full_name"]
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")


def logout() -> None:
    for key in ["logged_in", "username", "role", "full_name"]:
        st.session_state.pop(key, None)
    st.rerun()


# --------------------------- DATA OPS ---------------------------
def open_work_order(requester: str, machine_code: str, issue_type: str, criticality: str, description: str, priority: str):
    machine = fetch_one("SELECT * FROM machines WHERE code = ?", (machine_code,))
    if not machine:
        st.error("Máquina não encontrada.")
        return

    os_code = generate_os_code()
    execute(
        """
        INSERT INTO work_orders (
            os_code, open_datetime, requester, sector, machine_code, machine_name,
            issue_type, criticality, description, priority, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            os_code,
            now_str(),
            requester,
            machine["sector"],
            machine["code"],
            machine["name"],
            issue_type,
            criticality,
            description,
            priority,
            "Máquina Parada",
        ),
    )
    st.success(f"OS {os_code} aberta com sucesso.")
    st.info("Alerta de máquina parada gerado. Integração de WhatsApp está preparada para ativação por credenciais.")


def start_work_order(os_code: str, technician: str) -> None:
    wo = fetch_one("SELECT * FROM work_orders WHERE os_code = ?", (os_code,))
    if not wo or wo["status"] not in ["Máquina Parada", "Aberta", "Pausada"]:
        return

    started = now_str()
    response_minutes = calculate_live_minutes(wo["open_datetime"])

    if wo["status"] == "Pausada" and wo["pause_datetime"]:
        paused_now = calculate_live_minutes(wo["pause_datetime"])
        paused_total = float(wo["paused_minutes"] or 0) + paused_now
    else:
        paused_total = float(wo["paused_minutes"] or 0)

    execute(
        """
        UPDATE work_orders
        SET start_datetime = COALESCE(start_datetime, ?),
            assigned_technician = ?,
            response_minutes = CASE WHEN response_minutes = 0 THEN ? ELSE response_minutes END,
            status = 'Em Manutenção',
            pause_datetime = NULL,
            paused_minutes = ?
        WHERE os_code = ?
        """,
        (started, technician, response_minutes, paused_total, os_code),
    )


def pause_work_order(os_code: str) -> None:
    execute(
        "UPDATE work_orders SET status = 'Pausada', pause_datetime = ? WHERE os_code = ?",
        (now_str(), os_code),
    )


def finish_work_order(os_code: str, root_cause: str, action_taken: str, observations: str) -> None:
    wo = fetch_one("SELECT * FROM work_orders WHERE os_code = ?", (os_code,))
    if not wo:
        return

    end_dt = datetime.now()
    open_dt = parse_dt(wo["open_datetime"])
    start_dt = parse_dt(wo["start_datetime"])

    paused_total = float(wo["paused_minutes"] or 0)
    if wo["status"] == "Pausada" and wo["pause_datetime"]:
        paused_total += calculate_live_minutes(wo["pause_datetime"])

    downtime = round((end_dt - open_dt).total_seconds() / 60, 1) if open_dt else 0
    raw_repair = round((end_dt - start_dt).total_seconds() / 60, 1) if start_dt else 0
    repair = max(0, round(raw_repair - paused_total, 1))

    tech = fetch_one("SELECT hourly_rate FROM users WHERE full_name = ?", (wo["assigned_technician"],))
    hourly_rate = float(tech["hourly_rate"] or 0) if tech else 0
    labor_cost = round((repair / 60) * hourly_rate, 2)
    parts_cost = float(wo["parts_cost"] or 0)
    total_cost = round(labor_cost + parts_cost, 2)

    execute(
        """
        UPDATE work_orders
        SET end_datetime = ?,
            status = 'Finalizada',
            downtime_minutes = ?,
            repair_minutes = ?,
            paused_minutes = ?,
            root_cause = ?,
            action_taken = ?,
            observations = ?,
            labor_cost = ?,
            total_cost = ?
        WHERE os_code = ?
        """,
        (now_str(), downtime, repair, paused_total, root_cause, action_taken, observations, labor_cost, total_cost, os_code),
    )


def consume_part(os_code: str, part_code: str, qty_used: float, used_by: str) -> bool:
    part = fetch_one("SELECT * FROM parts WHERE code = ?", (part_code,))
    wo = fetch_one("SELECT * FROM work_orders WHERE os_code = ?", (os_code,))
    if not part or not wo:
        return False
    if float(part["stock_qty"] or 0) < qty_used:
        return False

    total_cost = round(qty_used * float(part["unit_cost"] or 0), 2)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE parts SET stock_qty = stock_qty - ? WHERE code = ?",
        (qty_used, part_code),
    )
    cur.execute(
        """
        INSERT INTO work_order_parts (os_code, part_code, part_description, qty_used, unit_cost, total_cost, used_at, used_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            os_code,
            part_code,
            part["description"],
            qty_used,
            float(part["unit_cost"] or 0),
            total_cost,
            now_str(),
            used_by,
        ),
    )
    cur.execute(
        "UPDATE work_orders SET parts_cost = parts_cost + ?, total_cost = total_cost + ? WHERE os_code = ?",
        (total_cost, total_cost, os_code),
    )
    conn.commit()
    conn.close()
    return True


# --------------------------- SIDEBAR ---------------------------
def sidebar_nav() -> str:
    with st.sidebar:
        st.markdown("### 🧠 Controle Industrial")
        st.caption(f"Conectado como **{current_user_full_name()}** · {st.session_state.get('role', '-')}")
        choice = st.radio(
            "Navegação",
            [
                "Dashboard",
                "Abrir OS",
                "Painel de OS",
                "Estoque e Peças",
                "Cadastros",
                "Histórico",
                "Configuração WhatsApp",
            ],
        )
        st.divider()
        if st.button("Sair do sistema", use_container_width=True):
            logout()
    return choice


# --------------------------- VIEWS ---------------------------
def render_header() -> None:
    st.markdown(
        f"""
        <div class="hero">
            <h1>🏭 Sala de Controle de Manutenção</h1>
            <p>OS online, monitoramento de máquinas, controle de peças, custos, SLA e indicadores de manutenção em visual MES/Tesla.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def dashboard_view() -> None:
    render_header()

    df = query_df("SELECT * FROM work_orders ORDER BY id DESC")
    parts_df = query_df("SELECT * FROM parts WHERE is_active = 1")

    total_os = len(df)
    abertas = int((df["status"].isin(["Máquina Parada", "Aberta", "Pausada", "Em Manutenção"])).sum()) if not df.empty else 0
    paradas = int((df["status"] == "Máquina Parada").sum()) if not df.empty else 0
    em_manut = int((df["status"] == "Em Manutenção").sum()) if not df.empty else 0

    mttr = round(df.loc[df["repair_minutes"] > 0, "repair_minutes"].mean(), 1) if not df.empty and (df["repair_minutes"] > 0).any() else 0
    mtbf = 0
    if not df.empty:
        grouped = df.groupby("machine_code").size()
        machines = query_df("SELECT code, standard_mtbf_hours FROM machines WHERE is_active = 1")
        if not machines.empty:
            merged = machines.merge(grouped.rename("breakdowns"), left_on="code", right_index=True, how="left").fillna({"breakdowns": 0})
            merged["calc_mtbf"] = merged.apply(lambda r: (r["standard_mtbf_hours"] / r["breakdowns"]) if r["breakdowns"] > 0 else r["standard_mtbf_hours"], axis=1)
            mtbf = round(merged["calc_mtbf"].mean(), 1)

    total_cost = round(df["total_cost"].sum(), 2) if not df.empty else 0
    low_stock = int((parts_df["stock_qty"] <= parts_df["min_stock"]).sum()) if not parts_df.empty else 0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    cards = [
        (k1, "OS Totais", total_os, "ciclo total do sistema"),
        (k2, "Abertas", abertas, "OS ainda em andamento"),
        (k3, "Máquinas Paradas", paradas, "alerta imediato"),
        (k4, "Em Manutenção", em_manut, "atendimento em curso"),
        (k5, "MTTR", f"{mttr} min", "tempo médio de reparo"),
        (k6, "MTBF", f"{mtbf} h", "média estimada por máquina"),
    ]
    for col, label, value, sub in cards:
        with col:
            st.markdown(f'<div class="kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>', unsafe_allow_html=True)

    c1, c2 = st.columns([1.2, 1])
    with c1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Ocorrências por máquina</div>', unsafe_allow_html=True)
        if not df.empty:
            chart_df = df.groupby(["machine_name"]).size().reset_index(name="Ocorrências").sort_values("Ocorrências", ascending=False)
            fig = px.bar(chart_df.head(10), x="Ocorrências", y="machine_name", orientation="h", text_auto=True)
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#dbeafe")
            fig.update_yaxes(title="")
            fig.update_xaxes(title="")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados ainda.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Radar operacional</div>', unsafe_allow_html=True)
        st.metric("Custo total", f"R$ {total_cost:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        st.metric("Peças em estoque crítico", low_stock)
        pending_sla = 0
        if not df.empty:
            active = df[df["status"].isin(["Máquina Parada", "Em Manutenção", "Pausada"])]
            if not active.empty:
                pending_sla = int((active["response_minutes"] > 30).sum())
        st.metric("SLA > 30 min", pending_sla)
        st.markdown('<p class="small-note">Use a página “Configuração WhatsApp” para inserir as credenciais do envio real. Nesta versão a integração está preparada e documentada.</p>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    c3, c4 = st.columns([1, 1])
    with c3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">OS em andamento</div>', unsafe_allow_html=True)
        active = df[df["status"].isin(["Máquina Parada", "Em Manutenção", "Pausada"])] if not df.empty else pd.DataFrame()
        if not active.empty:
            display = active[["os_code", "machine_name", "sector", "status", "priority", "assigned_technician", "open_datetime"]].copy()
            st.dataframe(display, use_container_width=True, hide_index=True)
        else:
            st.success("Nenhuma OS em andamento agora.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c4:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Últimas finalizações</div>', unsafe_allow_html=True)
        done = df[df["status"] == "Finalizada"].head(8) if not df.empty else pd.DataFrame()
        if not done.empty:
            display = done[["os_code", "machine_name", "repair_minutes", "downtime_minutes", "total_cost", "assigned_technician"]].copy()
            st.dataframe(display, use_container_width=True, hide_index=True)
        else:
            st.info("Ainda não há OS finalizadas.")
        st.markdown('</div>', unsafe_allow_html=True)


def open_os_view() -> None:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Abrir ordem de serviço</div>', unsafe_allow_html=True)

    machines = query_df("SELECT code, name, sector, criticality FROM machines WHERE is_active = 1 ORDER BY name")
    if machines.empty:
        st.warning("Cadastre máquinas antes de abrir OS.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    machine_options = {f"{r['code']} · {r['name']} · {r['sector']}": r['code'] for _, r in machines.iterrows()}

    with st.form("form_open_os"):
        c1, c2, c3 = st.columns(3)
        with c1:
            requester = st.text_input("Solicitante", value=current_user_full_name())
            selected_machine = st.selectbox("Máquina", list(machine_options.keys()))
        with c2:
            issue_type = st.selectbox("Tipo de ocorrência", ["Elétrica", "Mecânica", "Instrumentação", "Pneumática", "Setup", "Segurança", "Outro"])
            criticality = st.selectbox("Criticidade", ["Alta", "Média", "Baixa"])
        with c3:
            priority = st.selectbox("Prioridade", ["Urgente", "Alta", "Normal", "Baixa"])
            st.text_input("Data/Hora", value=now_str(), disabled=True)
        description = st.text_area("Descrição da falha", placeholder="Descreva o problema, sintomas, ruído, erro, condição da máquina...")
        submitted = st.form_submit_button("Abrir OS e disparar alerta")

    if submitted:
        if not requester.strip() or not description.strip():
            st.error("Preencha solicitante e descrição.")
        else:
            open_work_order(requester.strip(), machine_options[selected_machine], issue_type, criticality, description.strip(), priority)
    st.markdown('</div>', unsafe_allow_html=True)


def os_panel_view() -> None:
    df = query_df("SELECT * FROM work_orders ORDER BY id DESC")
    if df.empty:
        st.info("Nenhuma OS aberta ainda.")
        return

    critical_active = df[df["status"] == "Máquina Parada"]
    if not critical_active.empty:
        first = critical_active.iloc[0]
        st.markdown(
            f'<div class="alert-banner">🚨 ALERTA: {first["machine_name"]} parada · {first["os_code"]} · prioridade {first["priority"]}</div>',
            unsafe_allow_html=True,
        )

    active = df[df["status"].isin(["Máquina Parada", "Aberta", "Em Manutenção", "Pausada"])]
    st.markdown('<div class="section-title">Painel operacional das OS</div>', unsafe_allow_html=True)

    for _, row in active.iterrows():
        with st.container():
            st.markdown('<div class="panel">', unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1.6, 1, 1.1])
            with c1:
                st.markdown(f"**{row['os_code']} · {row['machine_name']}**")
                st.markdown(f"{status_chip(row['status'])}", unsafe_allow_html=True)
                st.write(f"Setor: {row['sector']}  |  Tipo: {row['issue_type']}  |  Prioridade: {row['priority']}")
                st.write(f"Descrição: {row['description']}")
            with c2:
                st.write(f"Abertura: {row['open_datetime']}")
                st.write(f"Técnico: {row['assigned_technician'] or '-'}")
                live_stop = calculate_live_minutes(row["open_datetime"])
                st.write(f"Parada atual: {fmt_minutes(live_stop)}")
            with c3:
                st.write(f"Resposta: {fmt_minutes(float(row['response_minutes'] or 0))}")
                if row["status"] == "Em Manutenção" and row["start_datetime"]:
                    live_repair = max(0, calculate_live_minutes(row["start_datetime"]) - float(row["paused_minutes"] or 0))
                    st.write(f"Reparo em curso: {fmt_minutes(live_repair)}")
                else:
                    st.write(f"Pausas acumuladas: {fmt_minutes(float(row['paused_minutes'] or 0))}")

            b1, b2, b3 = st.columns([1, 1, 1.5])
            techs_df = query_df("SELECT full_name FROM users WHERE role IN ('Manutenção', 'Gestor') ORDER BY full_name")
            tech_options = techs_df["full_name"].tolist() if not techs_df.empty else [current_user_full_name()]

            with b1:
                selected_tech = st.selectbox(
                    f"Técnico {row['os_code']}",
                    tech_options,
                    index=tech_options.index(row['assigned_technician']) if row['assigned_technician'] in tech_options else 0,
                    key=f"tech_{row['os_code']}",
                )
                if st.button(f"▶️ Iniciar/Retomar {row['os_code']}", key=f"start_{row['os_code']}", use_container_width=True):
                    start_work_order(row["os_code"], selected_tech)
                    st.rerun()
            with b2:
                if st.button(f"⏸️ Pausar {row['os_code']}", key=f"pause_{row['os_code']}", use_container_width=True):
                    pause_work_order(row["os_code"])
                    st.rerun()
            with b3:
                with st.expander(f"Finalizar {row['os_code']}"):
                    root_cause = st.text_input("Causa raiz", key=f"cause_{row['os_code']}")
                    action_taken = st.text_area("Ação executada", key=f"action_{row['os_code']}")
                    observations = st.text_area("Observações finais", key=f"obs_{row['os_code']}")
                    if st.button(f"✅ Confirmar finalização {row['os_code']}", key=f"finish_{row['os_code']}", use_container_width=True):
                        finish_work_order(row["os_code"], root_cause, action_taken, observations)
                        st.success(f"{row['os_code']} finalizada.")
                        st.rerun()

            with st.expander(f"Peças consumidas · {row['os_code']}"):
                c4, c5 = st.columns([1.5, 1])
                parts = query_df("SELECT code, description, stock_qty, unit_cost FROM parts WHERE is_active = 1 ORDER BY description")
                if parts.empty:
                    st.warning("Cadastre peças antes de lançar consumo.")
                else:
                    part_map = {f"{r['code']} · {r['description']} · estoque {r['stock_qty']}": r['code'] for _, r in parts.iterrows()}
                    with c4:
                        chosen_part = st.selectbox("Peça", list(part_map.keys()), key=f"part_sel_{row['os_code']}")
                    with c5:
                        qty = st.number_input("Quantidade", min_value=0.1, step=0.1, value=1.0, key=f"part_qty_{row['os_code']}")
                    if st.button(f"Adicionar peça em {row['os_code']}", key=f"add_part_{row['os_code']}"):
                        ok = consume_part(row["os_code"], part_map[chosen_part], float(qty), current_user_full_name())
                        if ok:
                            st.success("Peça lançada e estoque baixado.")
                            st.rerun()
                        else:
                            st.error("Estoque insuficiente ou item inválido.")

                used_parts = query_df("SELECT part_code, part_description, qty_used, total_cost, used_at FROM work_order_parts WHERE os_code = ? ORDER BY id DESC", (row["os_code"],))
                if not used_parts.empty:
                    st.dataframe(used_parts, use_container_width=True, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)


def stock_view() -> None:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Estoque e peças</div>', unsafe_allow_html=True)

    parts = query_df("SELECT * FROM parts WHERE is_active = 1 ORDER BY description")
    c1, c2 = st.columns([1, 1.4])
    with c1:
        with st.form("new_part"):
            st.markdown("**Cadastrar peça**")
            code = st.text_input("Código")
            desc = st.text_input("Descrição")
            q1, q2, q3 = st.columns(3)
            with q1:
                stock = st.number_input("Estoque", min_value=0.0, step=1.0)
            with q2:
                cost = st.number_input("Custo unitário", min_value=0.0, step=1.0)
            with q3:
                min_stock = st.number_input("Estoque mínimo", min_value=0.0, step=1.0)
            submit = st.form_submit_button("Salvar peça")
        if submit:
            try:
                execute(
                    "INSERT INTO parts (code, description, stock_qty, unit_cost, min_stock) VALUES (?, ?, ?, ?, ?)",
                    (code.strip().upper(), desc.strip(), stock, cost, min_stock),
                )
                st.success("Peça cadastrada.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("Código de peça já existe.")
    with c2:
        if not parts.empty:
            parts_show = parts[["code", "description", "stock_qty", "unit_cost", "min_stock"]].copy()
            parts_show["Situação"] = parts_show.apply(lambda r: "Crítico" if r["stock_qty"] <= r["min_stock"] else "OK", axis=1)
            st.dataframe(parts_show, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma peça cadastrada.")
    st.markdown('</div>', unsafe_allow_html=True)


def cadastros_view() -> None:
    t1, t2, t3 = st.tabs(["Máquinas", "Usuários", "Técnicos"])

    with t1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        with st.form("machine_form"):
            st.markdown("**Cadastro de máquina**")
            c1, c2, c3 = st.columns(3)
            with c1:
                code = st.text_input("Código da máquina")
                name = st.text_input("Nome da máquina")
            with c2:
                sector = st.text_input("Setor")
                criticality = st.selectbox("Criticidade", ["Alta", "Média", "Baixa"])
            with c3:
                mtbf = st.number_input("MTBF padrão (h)", min_value=0.0, step=1.0, value=72.0)
                submit_machine = st.form_submit_button("Salvar máquina")
        if submit_machine:
            try:
                execute(
                    "INSERT INTO machines (code, name, sector, criticality, standard_mtbf_hours) VALUES (?, ?, ?, ?, ?)",
                    (code.strip().upper(), name.strip(), sector.strip(), criticality, mtbf),
                )
                st.success("Máquina cadastrada.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("Código de máquina já existe.")
        machines = query_df("SELECT code, name, sector, criticality, standard_mtbf_hours FROM machines WHERE is_active = 1 ORDER BY name")
        st.dataframe(machines, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with t2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        with st.form("user_form"):
            st.markdown("**Cadastro de usuário**")
            c1, c2, c3 = st.columns(3)
            with c1:
                username = st.text_input("Login")
                password = st.text_input("Senha")
            with c2:
                full_name = st.text_input("Nome completo")
                role = st.selectbox("Perfil", ["Operador", "Manutenção", "Gestor"])
            with c3:
                rate = st.number_input("Valor hora", min_value=0.0, step=1.0)
                submit_user = st.form_submit_button("Salvar usuário")
        if submit_user:
            try:
                execute(
                    "INSERT INTO users (username, password, role, full_name, hourly_rate) VALUES (?, ?, ?, ?, ?)",
                    (username.strip(), password, role, full_name.strip(), rate),
                )
                st.success("Usuário cadastrado.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("Login já existe.")
        users = query_df("SELECT username, role, full_name, hourly_rate FROM users ORDER BY full_name")
        st.dataframe(users, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with t3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        techs = query_df("SELECT full_name, role, hourly_rate FROM users WHERE role IN ('Manutenção', 'Gestor') ORDER BY full_name")
        st.dataframe(techs, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)


def history_view() -> None:
    df = query_df("SELECT * FROM work_orders ORDER BY id DESC")
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Histórico e exportação</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("Sem histórico ainda.")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    f1, f2, f3 = st.columns(3)
    with f1:
        machine_filter = st.selectbox("Máquina", ["Todas"] + sorted(df["machine_name"].dropna().unique().tolist()))
    with f2:
        status_filter = st.selectbox("Status", ["Todos"] + sorted(df["status"].dropna().unique().tolist()))
    with f3:
        sector_filter = st.selectbox("Setor", ["Todos"] + sorted(df["sector"].dropna().unique().tolist()))

    filtered = df.copy()
    if machine_filter != "Todas":
        filtered = filtered[filtered["machine_name"] == machine_filter]
    if status_filter != "Todos":
        filtered = filtered[filtered["status"] == status_filter]
    if sector_filter != "Todos":
        filtered = filtered[filtered["sector"] == sector_filter]

    show = filtered[[
        "os_code", "open_datetime", "machine_name", "sector", "status", "assigned_technician",
        "response_minutes", "repair_minutes", "downtime_minutes", "parts_cost", "labor_cost", "total_cost"
    ]].copy()

    st.dataframe(show, use_container_width=True, hide_index=True)
    csv = show.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Baixar histórico em CSV", csv, file_name="historico_manutencao.csv", mime="text/csv")
    st.markdown('</div>', unsafe_allow_html=True)


def whatsapp_view() -> None:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Configuração de alerta por WhatsApp</div>', unsafe_allow_html=True)
    st.write("Esta versão já deixa a integração preparada para uso com **Twilio WhatsApp API**.")

    st.code(
        """# Windows
set TWILIO_ACCOUNT_SID=seu_account_sid
set TWILIO_AUTH_TOKEN=seu_auth_token
set TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
set TWILIO_WHATSAPP_TO=whatsapp:+55SEUNUMERO

# Linux
export TWILIO_ACCOUNT_SID=seu_account_sid
export TWILIO_AUTH_TOKEN=seu_auth_token
export TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
export TWILIO_WHATSAPP_TO=whatsapp:+55SEUNUMERO
"""
    )
    st.write("No ambiente local, depois de configurar as variáveis, você pode incluir a função de envio real no ponto de abertura da OS.")
    st.write("A lógica do sistema já está pensada para disparar o alerta quando uma OS é aberta com status **Máquina Parada**.")
    st.markdown('<p class="small-note">Para volumes maiores e múltiplos acessos externos, o próximo passo recomendado é migrar do SQLite para PostgreSQL.</p>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# --------------------------- MAIN ---------------------------
def main() -> None:
    inject_css()
    init_db()

    if not st.session_state.get("logged_in"):
        login_screen()
        st.stop()

    page = sidebar_nav()

    if page == "Dashboard":
        dashboard_view()
    elif page == "Abrir OS":
        open_os_view()
    elif page == "Painel de OS":
        os_panel_view()
    elif page == "Estoque e Peças":
        stock_view()
    elif page == "Cadastros":
        cadastros_view()
    elif page == "Histórico":
        history_view()
    elif page == "Configuração WhatsApp":
        whatsapp_view()

    st.markdown('<div class="footer-note">MES Maintenance V4 · visual inspirado em sala de controle · Streamlit + SQLite</div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()

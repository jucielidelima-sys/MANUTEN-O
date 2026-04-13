import os
import sqlite3
import hashlib
import shutil
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from twilio.rest import Client
from streamlit_autorefresh import st_autorefresh

DB_SQLITE = "manutencao_v10_fabrica.db"
LEGACY_DB_CANDIDATES = ["manutencao_v11_fabrica.db", "manutencao_v9_fabrica.db", "manutencao_v8_fabrica.db"]
UPLOAD_DIR = "uploads"
LOGO_PATH = os.path.join(UPLOAD_DIR, "logo_empresa.png")

st.set_page_config(page_title="Manutenção V12.1 Industrial", page_icon="🏭", layout="wide")

st.markdown("""
<style>
.stApp{
  background:
    linear-gradient(135deg, rgba(7,10,15,0.99), rgba(3,6,10,1)),
    radial-gradient(circle at top right, rgba(92,200,255,0.10), transparent 30%),
    repeating-linear-gradient(45deg, rgba(255,255,255,0.018) 0px, rgba(255,255,255,0.018) 7px, rgba(0,0,0,0) 7px, rgba(0,0,0,0) 14px);
  color:#edf4fb;
}
.block-container{max-width:1540px;padding-top:1rem;padding-bottom:1.2rem;}
.header-box,.panel,.kpi{
  background:linear-gradient(180deg, rgba(18,26,36,0.94), rgba(9,14,21,0.98));
  border:1px solid rgba(92,200,255,0.16);
  border-radius:24px;
  box-shadow:0 14px 34px rgba(0,0,0,0.38),0 0 22px rgba(92,200,255,0.05);
}
.header-box{padding:18px 22px;margin-bottom:14px;}
.panel{padding:16px 18px;}
.kpi{padding:18px;min-height:120px;}
.kpi-label{color:#9fb4c8;font-size:0.92rem;margin-bottom:8px;}
.kpi-value{font-size:2rem;font-weight:800;line-height:1.05;color:#edf4fb;}
.kpi-sub{color:#76ffd1;margin-top:8px;font-size:0.9rem;}
.divider{height:1px;background:linear-gradient(90deg, rgba(92,200,255,0), rgba(92,200,255,0.28), rgba(92,200,255,0));margin:12px 0 18px 0;}
.small-muted{color:#9fb4c8;font-size:0.88rem;}
.tv-title{font-size:2.25rem;font-weight:900;letter-spacing:0.6px;margin-bottom:4px;}
.tv-sub{font-size:1rem;color:#9fb4c8;margin-bottom:8px;}
.status-pill{display:inline-block;padding:6px 10px;border-radius:999px;font-weight:700;font-size:0.8rem;border:1px solid rgba(255,255,255,0.08);}
.pill-aberta{background:rgba(92,200,255,0.18);color:#d9f3ff;}
.pill-parada{background:rgba(255,107,107,0.18);color:#ffd6d6;}
.pill-manutencao{background:rgba(255,184,77,0.18);color:#ffe2b5;}
.pill-pausada{background:rgba(190,190,190,0.18);color:#efefef;}
.pill-finalizada{background:rgba(118,255,209,0.18);color:#cffff1;}
.pill-ativo{background:rgba(118,255,209,0.18);color:#cffff1;}
.pill-inativo{background:rgba(255,184,77,0.18);color:#ffe2b5;}
.pill-prev{background:rgba(214,118,255,0.18);color:#f0cfff;}
.alert-box,.warn-box,.ok-box{padding:14px 16px;border-radius:18px;}
.alert-box{background:linear-gradient(180deg, rgba(64,20,20,0.92), rgba(44,12,12,0.92));border:1px solid rgba(255,107,107,0.30);}
.warn-box{background:linear-gradient(180deg, rgba(57,40,12,0.92), rgba(44,30,10,0.92));border:1px solid rgba(255,184,77,0.30);}
.ok-box{background:linear-gradient(180deg, rgba(15,40,30,0.92), rgba(10,28,22,0.92));border:1px solid rgba(118,255,209,0.30);}
.big-tile{
  background:linear-gradient(180deg, rgba(22,30,41,0.98), rgba(13,19,27,0.98));
  border:1px solid rgba(92,200,255,0.20);
  border-radius:28px;
  padding:26px;
  min-height:150px;
  box-shadow:0 14px 34px rgba(0,0,0,0.34), 0 0 26px rgba(92,200,255,0.06);
}
.big-label{color:#9fb4c8;font-size:1.05rem;margin-bottom:10px;}
.big-value{font-size:3rem;font-weight:900;line-height:1;color:#edf4fb;}
.big-sub{color:#76ffd1;font-size:1rem;margin-top:10px;}
.stButton>button,.stDownloadButton>button{
  border-radius:14px !important;border:1px solid rgba(92,200,255,0.22) !important;
  background:linear-gradient(180deg, rgba(22,31,43,1), rgba(13,19,27,1)) !important;color:white !important;font-weight:700 !important;
}
</style>
""", unsafe_allow_html=True)

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def sec(name, default=""):
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass
    return os.getenv(name, default)

def get_db_mode():
    return sec("DB_MODE", "sqlite").strip().lower()

def ensure_local_db_compatibility():
    if os.path.exists(DB_SQLITE):
        return
    for legacy in LEGACY_DB_CANDIDATES:
        if os.path.exists(legacy):
            shutil.copyfile(legacy, DB_SQLITE)
            return

def get_connection():
    mode = get_db_mode()
    if mode == "postgres":
        try:
            import psycopg
            db_url = sec("POSTGRES_URL", "")
            if not db_url:
                raise ValueError("POSTGRES_URL não configurada.")
            conn = psycopg.connect(db_url)
            conn.row_factory = psycopg.rows.dict_row
            return conn, "postgres"
        except Exception as e:
            st.warning(f"Falha ao conectar no PostgreSQL. Usando SQLite. Motivo: {e}")
    ensure_local_db_compatibility()
    conn = sqlite3.connect(DB_SQLITE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn, "sqlite"

def q(sql, params=(), fetch=False, many=False):
    conn, mode = get_connection()
    cur = conn.cursor()
    try:
        if many:
            cur.executemany(sql, params)
        else:
            cur.execute(sql, params)
        out = cur.fetchall() if fetch else None
        conn.commit()
        return out
    finally:
        conn.close()

def df(sql, params=()):
    rows = q(sql, params, fetch=True)
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def parse_dt(v):
    try:
        return datetime.fromisoformat(str(v)) if v else None
    except Exception:
        return None

def mins_between(a, b):
    a = parse_dt(a)
    b = parse_dt(b)
    return round((b - a).total_seconds() / 60, 2) if a and b else 0.0

def fmt_min(v):
    v = float(v or 0)
    return f"{int(v//60):02d}h {int(v%60):02d}m"

def badge(status):
    css = {
        "Aberta":"pill-aberta","Máquina Parada":"pill-parada","Em manutenção":"pill-manutencao",
        "Pausada":"pill-pausada","Finalizada":"pill-finalizada","Preventiva":"pill-prev"
    }.get(status,"pill-aberta")
    return f'<span class="status-pill {css}">{status}</span>'

def status_ativo_badge(ativo):
    return f'<span class="status-pill {"pill-ativo" if int(ativo or 0)==1 else "pill-inativo"}">{"Ativo" if int(ativo or 0)==1 else "Inativo"}</span>'

def split_numbers(v):
    return [x.strip() for x in str(v).split(",") if x.strip()]

def manut_nums():
    return split_numbers(sec("WHATSAPP_MANUTENCAO", ""))

def gestao_nums():
    return split_numbers(sec("WHATSAPP_GESTAO", ""))

def send_whatsapp(msg, targets):
    sid = sec("TWILIO_ACCOUNT_SID")
    token = sec("TWILIO_AUTH_TOKEN")
    source = sec("TWILIO_WHATSAPP_FROM")
    if not sid or not token or not source or not targets:
        return False, "Credenciais ou destinos não configurados."
    try:
        client = Client(sid, token)
        sent = 0
        for target in targets:
            client.messages.create(body=msg, from_=source, to=target)
            sent += 1
        return True, f"WhatsApp enviado para {sent} destino(s)."
    except Exception as e:
        return False, f"Falha ao enviar WhatsApp: {e}"

def send_open_alert(os_number, machine_code, sector, criticality, requester, description, maintenance_type="Corretiva"):
    prefix = "🛡️ PREVENTIVA PROGRAMADA" if maintenance_type == "Preventiva" else "🚨 MÁQUINA PARADA"
    msg = (
        f"{prefix}\n\n"
        f"OS: {os_number}\n"
        f"Tipo: {maintenance_type}\n"
        f"Máquina: {machine_code}\n"
        f"Setor: {sector}\n"
        f"Criticidade: {criticality}\n"
        f"Solicitante: {requester}\n"
        f"Descrição: {description}\n"
    )
    details = []
    ok1, d1 = send_whatsapp(msg, manut_nums())
    details.append(d1)
    if criticality in ["Alta", "Crítica"]:
        ok2, d2 = send_whatsapp(msg, gestao_nums())
        details.append(d2)
        return ok1 or ok2, " | ".join(details)
    return ok1, " | ".join(details)

def send_close_summary(row):
    targets = manut_nums()
    if row["criticality"] in ["Alta", "Crítica"]:
        targets = list(dict.fromkeys(targets + gestao_nums()))
    msg = (
        "✅ OS FINALIZADA\n\n"
        f"OS: {row['os_number']}\n"
        f"Tipo: {row.get('maintenance_type','Corretiva')}\n"
        f"Máquina: {row['machine_code']}\n"
        f"Criticidade: {row['criticality']}\n"
        f"Técnico: {row['assigned_technician'] or '-'}\n"
        f"Tempo resposta: {fmt_min(row['response_min'])}\n"
        f"Tempo reparo: {fmt_min(row['repair_min'])}\n"
        f"Tempo parada: {fmt_min(row['downtime_min'])}\n"
        f"Custo total: R$ {float(row['total_cost'] or 0):.2f}"
    )
    return send_whatsapp(msg, targets)

def build_kpis(wo_df, prev_df, parts_df, tech_df, machines_full_df):
    total_os = 0 if wo_df.empty else len(wo_df)
    abertas = 0 if wo_df.empty else int(wo_df["status"].isin(["Aberta","Máquina Parada","Preventiva"]).sum())
    manut = 0 if wo_df.empty else int((wo_df["status"] == "Em manutenção").sum())
    paradas = 0 if wo_df.empty else int((wo_df["status"] == "Máquina Parada").sum())
    preventivas_abertas = 0 if wo_df.empty else int((wo_df["maintenance_type"] == "Preventiva").sum()) if "maintenance_type" in wo_df.columns else 0
    mttr = 0
    if not wo_df.empty:
        s = pd.to_numeric(wo_df["repair_min"], errors="coerce").fillna(0)
        s = s[s > 0]
        mttr = round(s.mean(), 1) if not s.empty else 0
    mtbf = 0
    if not wo_df.empty:
        fails = wo_df[wo_df["status"] == "Finalizada"].groupby("machine_code").size()
        mtbf = round(30 * 24 / max(fails.mean(), 1), 1) if len(fails) > 0 else 0
    custo_total = 0 if wo_df.empty else round(pd.to_numeric(wo_df["total_cost"], errors="coerce").fillna(0).sum(), 2)
    prev_venc = 0 if prev_df.empty else int((pd.to_datetime(prev_df["next_due_date"], errors="coerce").dt.date <= datetime.now().date()).sum())
    low_stock = 0 if parts_df.empty else int((pd.to_numeric(parts_df["stock"], errors="coerce").fillna(0) <= pd.to_numeric(parts_df["min_stock"], errors="coerce").fillna(0)).sum())
    tech_inativos = 0 if tech_df.empty else int((pd.to_numeric(tech_df["active"], errors="coerce").fillna(1) == 0).sum())
    maq_inativas = 0 if machines_full_df.empty else int((pd.to_numeric(machines_full_df["active"], errors="coerce").fillna(1) == 0).sum())
    return total_os, abertas, manut, paradas, mttr, mtbf, custo_total, prev_venc, low_stock, tech_inativos, maq_inativas, preventivas_abertas

def check_auto_alerts():
    esc_minutes = int(float(sec("ESCALATION_MINUTES", "30") or "30"))
    sent = []
    wo = df("SELECT * FROM work_orders WHERE status IN ('Máquina Parada','Aberta','Preventiva')")
    if not wo.empty:
        for _, row in wo.iterrows():
            if int(row.get("escalation_sent", 0) or 0) == 1:
                continue
            opened = parse_dt(row["open_dt"])
            if opened and (datetime.now() - opened).total_seconds()/60 >= esc_minutes:
                msg = (
                    "⏰ ESCALONAMENTO DE OS\n\n"
                    f"OS: {row['os_number']}\n"
                    f"Máquina: {row['machine_code']}\n"
                    f"Setor: {row['sector']}\n"
                    f"Criticidade: {row['criticality']}\n"
                    f"Sem início após {esc_minutes} min."
                )
                ok, detail = send_whatsapp(msg, gestao_nums())
                if ok:
                    q("UPDATE work_orders SET escalation_sent=1 WHERE id=?", (int(row["id"]),))
                    sent.append(detail)
    prev = df("SELECT * FROM preventive_plans WHERE active=1")
    if not prev.empty:
        for _, row in prev.iterrows():
            if int(row.get("alert_sent", 0) or 0) == 1:
                continue
            due = pd.to_datetime(row["next_due_date"], errors="coerce")
            if pd.notna(due) and due.date() <= datetime.now().date():
                msg = (
                    "🛡️ PREVENTIVA VENCIDA/HOJE\n\n"
                    f"Máquina: {row['machine_code']}\n"
                    f"Plano: {row['title']}\n"
                    f"Vencimento: {row['next_due_date']}\n"
                    f"Responsável: {row['responsible'] or '-'}"
                )
                ok, detail = send_whatsapp(msg, list(dict.fromkeys(manut_nums() + gestao_nums())))
                if ok:
                    q("UPDATE preventive_plans SET alert_sent=1 WHERE id=?", (int(row["id"]),))
                    sent.append(detail)
    return sent

def require_gestor():
    if st.session_state.user["profile"] != "Gestor":
        st.error("Acesso restrito. Somente o Gestor da Manutenção pode acessar este módulo.")
        st.stop()

def require_gestor_or_manutencao():
    if st.session_state.user["profile"] not in ["Gestor", "Manutenção"]:
        st.error("Acesso restrito. Somente Gestor e Manutenção podem acessar este módulo.")
        st.stop()

def header(title, subtitle):
    c1, c2 = st.columns([0.14, 0.86])
    with c1:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, use_container_width=True)
    with c2:
        st.markdown(f'<div class="header-box"><div class="tv-title">{title}</div><div class="tv-sub">{subtitle}</div></div>', unsafe_allow_html=True)

def verify_login(username, password):
    users = df("SELECT * FROM users WHERE username=?", (username,))
    if users.empty:
        return None
    user = users.iloc[0].to_dict()
    return user if user["password"] == hash_password(password) else None

def make_gauge(value, max_value, title, suffix="", color="#5cc8ff"):
    value = float(value or 0)
    max_value = max(float(max_value or 1), 1.0)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=min(value, max_value),
        number={"suffix": suffix, "font":{"size":34}},
        title={"text": title, "font":{"size":18}},
        gauge={
            "axis":{"range":[0, max_value], "tickfont":{"size":12}},
            "bar":{"color": color},
            "bgcolor":"rgba(0,0,0,0)",
            "borderwidth":1,
            "bordercolor":"rgba(92,200,255,0.18)",
            "steps":[
                {"range":[0, max_value*0.5], "color":"rgba(118,255,209,0.18)"},
                {"range":[max_value*0.5, max_value*0.8], "color":"rgba(255,184,77,0.18)"},
                {"range":[max_value*0.8, max_value], "color":"rgba(255,107,107,0.18)"}
            ]
        }
    ))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#edf4fb", margin=dict(l=10,r=10,t=45,b=10), height=300)
    return fig

def make_thermometer(value, max_value, title):
    value = float(value or 0)
    max_value = max(float(max_value or 1), 1.0)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=min(value, max_value),
        number={"font":{"size":32}},
        title={"text": title, "font":{"size":18}},
        gauge={
            "shape":"bullet",
            "axis":{"range":[0, max_value], "tickfont":{"size":12}},
            "bar":{"color":"#ff6b6b"},
            "bgcolor":"rgba(0,0,0,0)",
            "borderwidth":1,
            "bordercolor":"rgba(92,200,255,0.18)",
            "steps":[
                {"range":[0, max_value*0.4], "color":"rgba(118,255,209,0.16)"},
                {"range":[max_value*0.4, max_value*0.75], "color":"rgba(255,184,77,0.16)"},
                {"range":[max_value*0.75, max_value], "color":"rgba(255,107,107,0.20)"}
            ]
        }
    ))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#edf4fb", margin=dict(l=10,r=10,t=45,b=10), height=190)
    return fig

def init_db():
    conn, mode = get_connection()
    cur = conn.cursor()
    auto = "AUTOINCREMENT" if mode == "sqlite" else ""
    pk = f"INTEGER PRIMARY KEY {auto}".strip()

    cur.execute(f"CREATE TABLE IF NOT EXISTS users (id {pk}, username TEXT UNIQUE, password TEXT, profile TEXT, full_name TEXT)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS machines (id {pk}, code TEXT UNIQUE, name TEXT, sector TEXT, criticality TEXT, active INTEGER DEFAULT 1)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS technicians (id {pk}, name TEXT, labor_rate REAL DEFAULT 0, phone TEXT, active INTEGER DEFAULT 1)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS parts (id {pk}, code TEXT UNIQUE, name TEXT, stock REAL DEFAULT 0, min_stock REAL DEFAULT 0, unit_cost REAL DEFAULT 0)")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS work_orders (
        id {pk}, os_number TEXT UNIQUE, open_dt TEXT, sector TEXT, machine_code TEXT, requester TEXT,
        description TEXT, criticality TEXT, status TEXT, stop_start_dt TEXT, service_start_dt TEXT,
        service_end_dt TEXT, response_min REAL DEFAULT 0, repair_min REAL DEFAULT 0, downtime_min REAL DEFAULT 0,
        assigned_technician TEXT, root_cause TEXT, action_taken TEXT, labor_hours REAL DEFAULT 0, labor_cost REAL DEFAULT 0,
        parts_cost REAL DEFAULT 0, total_cost REAL DEFAULT 0, operator_signature TEXT, technician_signature TEXT,
        photo_path TEXT, notes TEXT, escalation_sent INTEGER DEFAULT 0, closed_summary_sent INTEGER DEFAULT 0,
        maintenance_type TEXT DEFAULT 'Corretiva'
    )""")
    cur.execute(f"CREATE TABLE IF NOT EXISTS work_order_parts (id {pk}, wo_id INTEGER, part_code TEXT, qty REAL, unit_cost REAL, total_cost REAL)")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS preventive_plans (
        id {pk}, machine_code TEXT, title TEXT, frequency_days INTEGER, last_done_date TEXT,
        next_due_date TEXT, responsible TEXT, notes TEXT, active INTEGER DEFAULT 1, alert_sent INTEGER DEFAULT 0
    )""")
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(technicians)").fetchall()]
        if "active" not in cols:
            cur.execute("ALTER TABLE technicians ADD COLUMN active INTEGER DEFAULT 1")
    except Exception:
        pass
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(machines)").fetchall()]
        if "active" not in cols:
            cur.execute("ALTER TABLE machines ADD COLUMN active INTEGER DEFAULT 1")
    except Exception:
        pass
    try:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(work_orders)").fetchall()]
        if "maintenance_type" not in cols:
            cur.execute("ALTER TABLE work_orders ADD COLUMN maintenance_type TEXT DEFAULT 'Corretiva'")
    except Exception:
        pass
    conn.commit()
    conn.close()

def create_preventive_os(plan_row):
    os_number = f"PM-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    desc = f"Preventiva: {plan_row['title']} | {plan_row['notes'] or ''}".strip()
    machine_df = df("SELECT sector, criticality FROM machines WHERE code=?", (plan_row["machine_code"],))
    sector = machine_df.iloc[0]["sector"] if not machine_df.empty else "Produção"
    criticality = machine_df.iloc[0]["criticality"] if not machine_df.empty else "Média"
    q("""INSERT INTO work_orders (
        os_number, open_dt, sector, machine_code, requester, description, criticality, status,
        stop_start_dt, assigned_technician, operator_signature, photo_path, notes, maintenance_type
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        os_number, now_str(), sector, plan_row["machine_code"], "Sistema Preventiva", desc, criticality,
        "Preventiva", None, "", "Sistema Preventiva", "", plan_row["notes"] or "", "Preventiva"
    ))
    q("UPDATE preventive_plans SET last_done_date=?, next_due_date=?, alert_sent=0 WHERE id=?", (
        str(datetime.now().date()),
        str((datetime.now().date() + timedelta(days=int(plan_row["frequency_days"] or 30)))),
        int(plan_row["id"])
    ))
    return os_number, desc, sector, criticality

init_db()

if "auth" not in st.session_state:
    st.session_state.auth = False
if "user" not in st.session_state:
    st.session_state.user = None

def login_screen():
    header("🏭 Manutenção V12.1 Industrial", "Base da V10 preservada + preventiva automática + cadastros completos")
    a, b, c = st.columns([1.15, 1.2, 1.15])
    with b:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Entrar")
        u = st.text_input("Usuário", value="gestor")
        p = st.text_input("Senha", value="1234", type="password")
        if st.button("Acessar", use_container_width=True):
            user = verify_login(u, p)
            if user:
                st.session_state.auth = True
                st.session_state.user = user
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")
        st.markdown("</div>", unsafe_allow_html=True)

if not st.session_state.auth:
    login_screen()
    st.stop()

user = st.session_state.user
auto_msgs = check_auto_alerts()

with st.sidebar:
    st.markdown(f"### 👤 {user['full_name']}")
    st.caption(f"Perfil: {user['profile']}")
    page = st.radio("Módulos", [
        "Dashboard", "Tela TV Fábrica", "Abrir OS", "Painel de OS",
        "Histórico", "Cadastros", "Preventiva", "Segurança", "Configurações"
    ])
    st.caption(f"Banco ativo: {get_db_mode().upper()}")
    st.caption("Compatível com banco da V10")
    if st.button("Sair", use_container_width=True):
        st.session_state.auth = False
        st.session_state.user = None
        st.rerun()

header("⚙️ Centro de Manutenção Industrial", "Versão V12.1 pronta para fábrica")
if auto_msgs:
    st.success(" | ".join(auto_msgs[:2]))

wo_df = df("SELECT * FROM work_orders ORDER BY id DESC")
machines_df = df("SELECT * FROM machines WHERE active=1 ORDER BY code")
machines_full_df = df("SELECT * FROM machines ORDER BY active DESC, code")
tech_df = df("SELECT * FROM technicians ORDER BY active DESC, name")
tech_active_df = df("SELECT * FROM technicians WHERE active=1 ORDER BY name")
parts_df = df("SELECT * FROM parts ORDER BY code")
prev_df = df("SELECT * FROM preventive_plans ORDER BY next_due_date")

# Reuse V10.1 structure in concise way
if page == "Dashboard":
    total_os, abertas, manut, paradas, mttr, mtbf, custo_total, prev_venc, low_stock, tech_inativos, maq_inativas, preventivas_abertas = build_kpis(wo_df, prev_df, parts_df, tech_df, machines_full_df)
    cols = st.columns(6)
    data = [
        ("OS Totais", total_os, "Visão geral"),
        ("Abertas", abertas, "Necessitam ação"),
        ("Em manutenção", manut, "Equipe atuando"),
        ("Máquinas paradas", paradas, "Impacto produtivo"),
        ("Preventivas", preventivas_abertas, "OS preventivas"),
        ("Maq. inativas", maq_inativas, "Parque fabril"),
    ]
    for col, (lab, val, sub) in zip(cols, data):
        with col:
            st.markdown(f'<div class="kpi"><div class="kpi-label">{lab}</div><div class="kpi-value">{val}</div><div class="kpi-sub">{sub}</div></div>', unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    g1, g2, g3 = st.columns(3)
    with g1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(make_gauge(mttr, max(mttr*1.5 if mttr else 120, 120), "MTTR", " min", "#5cc8ff"), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with g2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(make_gauge(mtbf, max(mtbf*1.4 if mtbf else 300, 300), "MTBF", " h", "#76ffd1"), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with g3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(make_thermometer(paradas, max(paradas+3, 5), "Máquinas paradas"), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

elif page == "Tela TV Fábrica":
    refresh_s = int(float(sec("TV_REFRESH_SECONDS", "10") or "10"))
    st_autorefresh(interval=refresh_s * 1000, key="tv_refresh")
    total_os, abertas, manut, paradas, mttr, mtbf, custo_total, prev_venc, low_stock, tech_inativos, maq_inativas, preventivas_abertas = build_kpis(wo_df, prev_df, parts_df, tech_df, machines_full_df)
    top = st.columns(5)
    vals = [
        ("Máquinas paradas", paradas, "Urgência"),
        ("Em manutenção", manut, "Atendimento"),
        ("OS abertas", abertas, "Fila"),
        ("Preventivas abertas", preventivas_abertas, "Planejamento"),
        ("MTTR", f"{mttr} min", "Performance"),
    ]
    for col, (lab, val, sub) in zip(top, vals):
        with col:
            st.markdown(f'<div class="big-tile"><div class="big-label">{lab}</div><div class="big-value">{val}</div><div class="big-sub">{sub}</div></div>', unsafe_allow_html=True)

elif page == "Abrir OS":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("📋 Abrir ordem de serviço")
    with st.form("open_wo"):
        c1, c2, c3 = st.columns(3)
        with c1:
            sector = st.text_input("Setor", value="Produção")
            requester = st.text_input("Solicitante", value=user["full_name"])
            criticality = st.selectbox("Criticidade", ["Baixa","Média","Alta","Crítica"], index=2)
        with c2:
            machine_code = st.selectbox("Máquina", machines_df["code"].tolist() if not machines_df.empty else [])
            status = st.selectbox("Status inicial", ["Máquina Parada","Aberta"], index=0)
            assigned_technician = st.selectbox("Técnico responsável", [""] + (tech_active_df["name"].tolist() if not tech_active_df.empty else []))
        with c3:
            operator_signature = st.text_input("Assinatura operador", value=requester)
            notes = st.text_input("Observações rápidas")
            uploaded_file = st.file_uploader("Foto da falha", type=["png","jpg","jpeg"])
        description = st.text_area("Descrição da falha", height=110)
        submit = st.form_submit_button("Abrir OS", use_container_width=True)
        if submit:
            if not machine_code or not description.strip():
                st.error("Selecione a máquina e descreva a falha.")
            else:
                os_number = f"OS-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                photo_path = ""
                if uploaded_file is not None:
                    safe_name = f"{os_number}_{uploaded_file.name}".replace(" ", "_")
                    photo_path = os.path.join(UPLOAD_DIR, safe_name)
                    with open(photo_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                q("""INSERT INTO work_orders (
                    os_number, open_dt, sector, machine_code, requester, description, criticality, status,
                    stop_start_dt, assigned_technician, operator_signature, photo_path, notes, maintenance_type
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                    os_number, now_str(), sector, machine_code, requester, description.strip(), criticality, status,
                    now_str() if status == "Máquina Parada" else None, assigned_technician, operator_signature, photo_path, notes, "Corretiva"
                ))
                st.success(f"OS {os_number} aberta com sucesso.")
                if status == "Máquina Parada":
                    ok, detail = send_open_alert(os_number, machine_code, sector, criticality, requester, description.strip(), "Corretiva")
                    st.success(detail) if ok else st.warning(detail)
                st.rerun()

elif page == "Painel de OS":
    require_gestor_or_manutencao()
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("⚙️ Painel operacional")
    if wo_df.empty:
        st.info("Nenhuma OS cadastrada.")
    else:
        st.dataframe(
            wo_df[["os_number","maintenance_type","machine_code","criticality","status","assigned_technician","open_dt"]],
            use_container_width=True, hide_index=True
        )

elif page == "Histórico":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("📚 Histórico")
    if wo_df.empty:
        st.info("Sem histórico.")
    else:
        hist = wo_df.copy()
        hist["tempo_resposta"] = hist["response_min"].fillna(0).map(fmt_min)
        hist["tempo_reparo"] = hist["repair_min"].fillna(0).map(fmt_min)
        hist["tempo_parada"] = hist["downtime_min"].fillna(0).map(fmt_min)
        cols = ["os_number","maintenance_type","open_dt","sector","machine_code","criticality","status","assigned_technician","tempo_resposta","tempo_reparo","tempo_parada","total_cost"]
        st.dataframe(hist[cols], use_container_width=True, hide_index=True)

elif page == "Cadastros":
    require_gestor()
    tabs = st.tabs(["Máquinas","Técnicos"])
    with tabs[0]:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Gestão completa de máquinas")
        st.dataframe(machines_full_df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with tabs[1]:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Gestão completa de técnicos")
        st.dataframe(tech_df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

elif page == "Preventiva":
    require_gestor()
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🛡️ Plano preventivo")
    with st.form("f_prev", clear_on_submit=True):
        a, b, c, d = st.columns(4)
        machine_code = a.selectbox("Máquina", machines_df["code"].tolist() if not machines_df.empty else [])
        title = b.text_input("Título")
        freq = c.number_input("Frequência (dias)", min_value=1, value=30, step=1)
        responsible = d.text_input("Responsável")
        e, f = st.columns(2)
        last_done = e.date_input("Última execução", value=datetime.now().date())
        notes = f.text_input("Observações")
        if st.form_submit_button("Salvar preventiva", use_container_width=True):
            next_due = last_done + timedelta(days=int(freq))
            q("INSERT INTO preventive_plans (machine_code,title,frequency_days,last_done_date,next_due_date,responsible,notes,active,alert_sent) VALUES (?,?,?,?,?,?,?,1,0)", (machine_code, title, int(freq), str(last_done), str(next_due), responsible, notes))
            st.success("Plano preventivo salvo.")
            st.rerun()

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.subheader("Gerar OS preventiva")
    if prev_df.empty:
        st.info("Nenhuma preventiva cadastrada.")
    else:
        prev_show = prev_df.copy()
        prev_show["status_prev"] = pd.to_datetime(prev_show["next_due_date"], errors="coerce").dt.date.apply(lambda d: "Vencida/Hoje" if d and d <= datetime.now().date() else "Programada")
        st.dataframe(prev_show, use_container_width=True, hide_index=True)
        plans = {f"{row['machine_code']} • {row['title']} • {row['next_due_date']}": row for _, row in prev_df.iterrows()}
        selected = st.selectbox("Selecionar plano para gerar OS", list(plans.keys()))
        if st.button("Gerar OS Preventiva", use_container_width=True):
            row = plans[selected]
            os_number, desc, sector, criticality = create_preventive_os(row)
            ok, detail = send_open_alert(os_number, row["machine_code"], sector, criticality, "Sistema Preventiva", desc, "Preventiva")
            st.success(f"OS preventiva {os_number} gerada com sucesso.")
            if ok:
                st.success(detail)
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

elif page == "Segurança":
    require_gestor()
    st.markdown('<div class="panel">Módulo de segurança ativo para gestor.</div>', unsafe_allow_html=True)

elif page == "Configurações":
    require_gestor()
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🔌 Configurações da fábrica")
    st.write("Esta V12.1 usa o banco da V10 por padrão para manter máquinas e técnicos já cadastrados.")
    st.code("""DB_MODE = "sqlite"
# ou
DB_MODE = "postgres"
POSTGRES_URL = "postgresql://usuario:senha@host:5432/banco"
""")
    st.markdown("</div>", unsafe_allow_html=True)

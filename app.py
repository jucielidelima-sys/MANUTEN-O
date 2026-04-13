import os
import sqlite3
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse

import pandas as pd
import plotly.express as px
import streamlit as st
from twilio.rest import Client
from streamlit_autorefresh import st_autorefresh

DB_SQLITE = "manutencao_v8_fabrica.db"
UPLOAD_DIR = "uploads"
LOGO_PATH = os.path.join(UPLOAD_DIR, "logo_empresa.png")

st.set_page_config(page_title="Manutenção V8 Fábrica", page_icon="🏭", layout="wide")

st.markdown("""
<style>
.stApp{
  background:
    linear-gradient(135deg, rgba(8,12,18,0.98), rgba(5,8,12,0.99)),
    repeating-linear-gradient(45deg, rgba(255,255,255,0.02) 0px, rgba(255,255,255,0.02) 8px, rgba(0,0,0,0) 8px, rgba(0,0,0,0) 16px);
  color:#edf4fb;
}
.block-container{max-width:1500px;padding-top:1rem;padding-bottom:1.2rem;}
.header-box,.panel,.kpi{
  background:linear-gradient(180deg, rgba(18,26,36,0.96), rgba(10,15,22,0.97));
  border:1px solid rgba(92,200,255,0.18);
  border-radius:22px;
  box-shadow:0 12px 28px rgba(0,0,0,0.34),0 0 20px rgba(92,200,255,0.05);
}
.header-box{padding:18px 22px;margin-bottom:14px;}
.panel{padding:16px 18px;}
.kpi{padding:18px;min-height:120px;}
.kpi-label{color:#9fb4c8;font-size:0.92rem;margin-bottom:8px;}
.kpi-value{font-size:2rem;font-weight:800;line-height:1.05;color:#edf4fb;}
.kpi-sub{color:#76ffd1;margin-top:8px;font-size:0.9rem;}
.divider{height:1px;background:linear-gradient(90deg, rgba(92,200,255,0), rgba(92,200,255,0.28), rgba(92,200,255,0));margin:12px 0 18px 0;}
.small-muted{color:#9fb4c8;font-size:0.88rem;}
.tv-title{font-size:2.25rem;font-weight:900;letter-spacing:0.5px;margin-bottom:4px;}
.tv-sub{font-size:1rem;color:#9fb4c8;margin-bottom:8px;}
.status-pill{display:inline-block;padding:6px 10px;border-radius:999px;font-weight:700;font-size:0.8rem;border:1px solid rgba(255,255,255,0.08);}
.pill-aberta{background:rgba(92,200,255,0.18);color:#d9f3ff;}
.pill-parada{background:rgba(255,107,107,0.18);color:#ffd6d6;}
.pill-manutencao{background:rgba(255,184,77,0.18);color:#ffe2b5;}
.pill-pausada{background:rgba(190,190,190,0.18);color:#efefef;}
.pill-finalizada{background:rgba(118,255,209,0.18);color:#cffff1;}
.alert-box,.warn-box{padding:14px 16px;border-radius:18px;}
.alert-box{background:linear-gradient(180deg, rgba(64,20,20,0.92), rgba(44,12,12,0.92));border:1px solid rgba(255,107,107,0.30);}
.warn-box{background:linear-gradient(180deg, rgba(57,40,12,0.92), rgba(44,30,10,0.92));border:1px solid rgba(255,184,77,0.30);}
.big-tile{
  background:linear-gradient(180deg, rgba(22,30,41,0.98), rgba(13,19,27,0.98));
  border:1px solid rgba(92,200,255,0.22);
  border-radius:28px;
  padding:26px;
  min-height:150px;
  box-shadow:0 14px 34px rgba(0,0,0,0.34), 0 0 26px rgba(92,200,255,0.06);
}
.big-label{color:#9fb4c8;font-size:1.05rem;margin-bottom:10px;}
.big-value{font-size:3rem;font-weight:900;line-height:1;color:#edf4fb;}
.big-sub{color:#76ffd1;font-size:1rem;margin-top:10px;}
.stButton>button,.stDownloadButton>button{
  border-radius:14px !important;border:1px solid rgba(92,200,255,0.25) !important;
  background:linear-gradient(180deg, rgba(24,34,46,1), rgba(16,22,30,1)) !important;color:white !important;font-weight:700 !important;
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

def init_db():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    conn, mode = get_connection()
    cur = conn.cursor()

    auto = "AUTOINCREMENT" if mode == "sqlite" else ""
    pk = f"INTEGER PRIMARY KEY {auto}".strip()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS users (
            id {pk},
            username TEXT UNIQUE,
            password TEXT,
            profile TEXT,
            full_name TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS machines (
            id {pk},
            code TEXT UNIQUE,
            name TEXT,
            sector TEXT,
            criticality TEXT,
            active INTEGER DEFAULT 1
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS technicians (
            id {pk},
            name TEXT,
            labor_rate REAL DEFAULT 0,
            phone TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS parts (
            id {pk},
            code TEXT UNIQUE,
            name TEXT,
            stock REAL DEFAULT 0,
            min_stock REAL DEFAULT 0,
            unit_cost REAL DEFAULT 0
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS work_orders (
            id {pk},
            os_number TEXT UNIQUE,
            open_dt TEXT,
            sector TEXT,
            machine_code TEXT,
            requester TEXT,
            description TEXT,
            criticality TEXT,
            status TEXT,
            stop_start_dt TEXT,
            service_start_dt TEXT,
            service_end_dt TEXT,
            response_min REAL DEFAULT 0,
            repair_min REAL DEFAULT 0,
            downtime_min REAL DEFAULT 0,
            assigned_technician TEXT,
            root_cause TEXT,
            action_taken TEXT,
            labor_hours REAL DEFAULT 0,
            labor_cost REAL DEFAULT 0,
            parts_cost REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            operator_signature TEXT,
            technician_signature TEXT,
            photo_path TEXT,
            notes TEXT,
            escalation_sent INTEGER DEFAULT 0,
            closed_summary_sent INTEGER DEFAULT 0
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS work_order_parts (
            id {pk},
            wo_id INTEGER,
            part_code TEXT,
            qty REAL,
            unit_cost REAL,
            total_cost REAL
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS preventive_plans (
            id {pk},
            machine_code TEXT,
            title TEXT,
            frequency_days INTEGER,
            last_done_date TEXT,
            next_due_date TEXT,
            responsible TEXT,
            notes TEXT,
            active INTEGER DEFAULT 1,
            alert_sent INTEGER DEFAULT 0
        )
    """)

    if cur.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO users (username,password,profile,full_name) VALUES (?,?,?,?)",
            [
                ("operador", hash_password("1234"), "Operador", "Operador Padrão"),
                ("manutencao", hash_password("1234"), "Manutenção", "Equipe de Manutenção"),
                ("gestor", hash_password("1234"), "Gestor", "Gestor da Manutenção"),
            ]
        )

    if cur.execute("SELECT COUNT(*) FROM machines").fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO machines (code,name,sector,criticality,active) VALUES (?,?,?,?,1)",
            [
                ("SLT-01","Slitter Principal","Conversão","Alta"),
                ("PRS-02","Prensa Hidráulica","Estamparia","Média"),
                ("EMB-03","Embutidora","Montagem","Alta"),
                ("BNC-04","Bancada de Teste","Qualidade","Baixa"),
            ]
        )

    if cur.execute("SELECT COUNT(*) FROM technicians").fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO technicians (name,labor_rate,phone) VALUES (?,?,?)",
            [
                ("Carlos Manutenção",85.0,""),
                ("Fernanda Mecânica",92.0,""),
                ("Rafael Elétrica",98.0,""),
            ]
        )

    if cur.execute("SELECT COUNT(*) FROM parts").fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO parts (code,name,stock,min_stock,unit_cost) VALUES (?,?,?,?,?)",
            [
                ("ROL-6204","Rolamento 6204",20,5,28.5),
                ("COR-A36","Correia A36",10,3,65.0),
                ("SEN-PNP","Sensor PNP",8,2,115.0),
                ("CNT-09A","Contator 9A",12,4,54.9),
            ]
        )

    users = cur.execute("SELECT id,password FROM users").fetchall()
    for row in users:
        uid = row[0] if not isinstance(row, dict) else row["id"]
        pw = row[1] if not isinstance(row, dict) else row["password"]
        if pw and len(str(pw)) != 64:
            cur.execute("UPDATE users SET password=? WHERE id=?", (hash_password(str(pw)), uid))

    conn.commit()
    conn.close()

def verify_login(username, password):
    users = df("SELECT * FROM users WHERE username=?", (username,))
    if users.empty:
        return None
    user = users.iloc[0].to_dict()
    return user if user["password"] == hash_password(password) else None

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
    css = {"Aberta":"pill-aberta","Máquina Parada":"pill-parada","Em manutenção":"pill-manutencao","Pausada":"pill-pausada","Finalizada":"pill-finalizada"}.get(status,"pill-aberta")
    return f'<span class="status-pill {css}">{status}</span>'

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

def send_open_alert(os_number, machine_code, sector, criticality, requester, description):
    msg = (
        "🚨 MÁQUINA PARADA\n\n"
        f"OS: {os_number}\n"
        f"Máquina: {machine_code}\n"
        f"Setor: {sector}\n"
        f"Criticidade: {criticality}\n"
        f"Solicitante: {requester}\n"
        f"Problema: {description}\n\n"
        "Ação imediata necessária."
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
        f"Máquina: {row['machine_code']}\n"
        f"Criticidade: {row['criticality']}\n"
        f"Técnico: {row['assigned_technician'] or '-'}\n"
        f"Tempo resposta: {fmt_min(row['response_min'])}\n"
        f"Tempo reparo: {fmt_min(row['repair_min'])}\n"
        f"Tempo parada: {fmt_min(row['downtime_min'])}\n"
        f"Custo total: R$ {float(row['total_cost'] or 0):.2f}"
    )
    return send_whatsapp(msg, targets)

def build_kpis(wo_df, prev_df, parts_df):
    total_os = 0 if wo_df.empty else len(wo_df)
    abertas = 0 if wo_df.empty else int(wo_df["status"].isin(["Aberta","Máquina Parada"]).sum())
    manut = 0 if wo_df.empty else int((wo_df["status"] == "Em manutenção").sum())
    paradas = 0 if wo_df.empty else int((wo_df["status"] == "Máquina Parada").sum())
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
    return total_os, abertas, manut, paradas, mttr, mtbf, custo_total, prev_venc, low_stock

def check_auto_alerts():
    esc_minutes = int(float(sec("ESCALATION_MINUTES", "30") or "30"))
    sent = []

    wo = df("SELECT * FROM work_orders WHERE status IN ('Máquina Parada','Aberta')")
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
        st.error("Acesso restrito. Somente o Gestor da Manutenção pode acessar cadastros.")
        st.stop()

def header(title, subtitle):
    c1, c2 = st.columns([0.14, 0.86])
    with c1:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, use_container_width=True)
    with c2:
        st.markdown(f'<div class="header-box"><div class="tv-title">{title}</div><div class="tv-sub">{subtitle}</div></div>', unsafe_allow_html=True)

init_db()

if "auth" not in st.session_state:
    st.session_state.auth = False
if "user" not in st.session_state:
    st.session_state.user = None

def login_screen():
    header("🏭 Manutenção V8 Fábrica", "Cadastros restritos ao Gestor • PostgreSQL pronto • WhatsApp • Tela TV")
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
    if st.button("Sair", use_container_width=True):
        st.session_state.auth = False
        st.session_state.user = None
        st.rerun()

header("⚙️ Centro de Manutenção Industrial", "")
if auto_msgs:
    st.success(" | ".join(auto_msgs[:2]))

wo_df = df("SELECT * FROM work_orders ORDER BY id DESC")
machines_df = df("SELECT * FROM machines WHERE active=1 ORDER BY code")
tech_df = df("SELECT * FROM technicians ORDER BY name")
parts_df = df("SELECT * FROM parts ORDER BY code")
prev_df = df("SELECT * FROM preventive_plans ORDER BY next_due_date")

if page == "Dashboard":
    total_os, abertas, manut, paradas, mttr, mtbf, custo_total, prev_venc, low_stock = build_kpis(wo_df, prev_df, parts_df)
    cols = st.columns(6)
    data = [
        ("OS Totais", total_os, "Visão geral"),
        ("Abertas", abertas, "Necessitam ação"),
        ("Em manutenção", manut, "Equipe atuando"),
        ("Máquinas paradas", paradas, "Impacto produtivo"),
        ("MTTR", f"{mttr} min", "Tempo médio reparo"),
        ("MTBF", f"{mtbf} h", "Estimado"),
    ]
    for col, (lab, val, sub) in zip(cols, data):
        with col:
            st.markdown(f'<div class="kpi"><div class="kpi-label">{lab}</div><div class="kpi-value">{val}</div><div class="kpi-sub">{sub}</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Custos por máquina")
        if not wo_df.empty:
            costs = wo_df.groupby("machine_code", dropna=False)["total_cost"].sum().reset_index()
            costs["machine_code"] = costs["machine_code"].fillna("Sem máquina")
            fig = px.bar(costs, x="machine_code", y="total_cost", text="total_cost")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#edf4fb", height=320, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados.")
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Falhas por máquina")
        if not wo_df.empty:
            occ = wo_df.groupby("machine_code", dropna=False).size().reset_index(name="OS")
            occ["machine_code"] = occ["machine_code"].fillna("Sem máquina")
            fig = px.bar(occ, x="machine_code", y="OS", text="OS")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#edf4fb", height=320, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados.")
        st.markdown("</div>", unsafe_allow_html=True)

    with c3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Alertas")
        st.write(f"**Custo total:** R$ {custo_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        st.write(f"**Preventivas vencidas/hoje:** {prev_venc}")
        st.write(f"**Peças em estoque mínimo:** {low_stock}")
        st.markdown("</div>", unsafe_allow_html=True)

elif page == "Tela TV Fábrica":
    refresh_s = int(float(sec("TV_REFRESH_SECONDS", "10") or "10"))
    st_autorefresh(interval=refresh_s * 1000, key="tv_refresh")
    total_os, abertas, manut, paradas, mttr, mtbf, custo_total, prev_venc, low_stock = build_kpis(wo_df, prev_df, parts_df)

    top = st.columns(5)
    vals = [
        ("Máquinas paradas", paradas, "Urgência"),
        ("Em manutenção", manut, "Atendimento"),
        ("OS abertas", abertas, "Fila"),
        ("Preventivas vencidas", prev_venc, "Planejamento"),
        ("MTTR", f"{mttr} min", "Performance"),
    ]
    for col, (lab, val, sub) in zip(top, vals):
        with col:
            st.markdown(f'<div class="big-tile"><div class="big-label">{lab}</div><div class="big-value">{val}</div><div class="big-sub">{sub}</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    left, right = st.columns([1.2, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader(f"OS abertas em tempo real • refresh {refresh_s}s")
        live = wo_df[wo_df["status"].isin(["Aberta","Máquina Parada","Em manutenção","Pausada"])].copy() if not wo_df.empty else pd.DataFrame()
        if live.empty:
            st.success("Nenhuma OS aberta no momento.")
        else:
            st.dataframe(live[["os_number","machine_code","sector","criticality","status","assigned_technician","open_dt"]], use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Ranking de falhas")
        if not wo_df.empty:
            rank = wo_df.groupby("machine_code", dropna=False).size().reset_index(name="OS").sort_values("OS", ascending=False)
            rank["machine_code"] = rank["machine_code"].fillna("Sem máquina")
            fig = px.bar(rank.head(10), x="machine_code", y="OS", text="OS")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#edf4fb", height=420, margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados.")
        st.markdown("</div>", unsafe_allow_html=True)

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
            assigned_technician = st.selectbox("Técnico responsável", [""] + (tech_df["name"].tolist() if not tech_df.empty else []))
        with c3:
            operator_signature = st.text_input("Assinatura operador", value=requester)
            notes = st.text_input("Observações rápidas")
            uploaded_file = st.file_uploader("Foto da falha", type=["png","jpg","jpeg"])
        description = st.text_area("Descrição da falha", height=110)
        submit = st.form_submit_button("Abrir OS", use_container_width=True)

        if submit:
            if not machine_code:
                st.error("Selecione uma máquina.")
            elif not description.strip():
                st.error("Descreva a falha.")
            else:
                os_number = f"OS-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                photo_path = ""
                if uploaded_file is not None:
                    safe_name = f"{os_number}_{uploaded_file.name}".replace(" ", "_")
                    photo_path = os.path.join(UPLOAD_DIR, safe_name)
                    with open(photo_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                q("""
                    INSERT INTO work_orders (
                        os_number, open_dt, sector, machine_code, requester, description, criticality, status,
                        stop_start_dt, assigned_technician, operator_signature, photo_path, notes
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    os_number, now_str(), sector, machine_code, requester, description.strip(), criticality, status,
                    now_str() if status == "Máquina Parada" else None, assigned_technician, operator_signature, photo_path, notes
                ))
                st.success(f"OS {os_number} aberta com sucesso.")
                if status == "Máquina Parada":
                    ok, detail = send_open_alert(os_number, machine_code, sector, criticality, requester, description.strip())
                    st.success(detail) if ok else st.warning(detail)
                    st.markdown('<div class="alert-box"><strong>🚨 ALERTA:</strong> máquina marcada como parada.</div>', unsafe_allow_html=True)
                st.rerun()

elif page == "Painel de OS":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("⚙️ Painel operacional")
    if wo_df.empty:
        st.info("Nenhuma OS cadastrada.")
    else:
        f1, f2, f3 = st.columns(3)
        with f1:
            status_filter = st.selectbox("Filtrar status", ["Todos","Aberta","Máquina Parada","Em manutenção","Pausada","Finalizada"])
        with f2:
            machine_filter = st.selectbox("Filtrar máquina", ["Todas"] + wo_df["machine_code"].fillna("Sem máquina").unique().tolist())
        with f3:
            tech_opts = sorted(wo_df["assigned_technician"].fillna("").replace("", "Sem técnico").unique().tolist())
            tech_filter = st.selectbox("Filtrar técnico", ["Todos"] + tech_opts)

        panel_df = wo_df.copy()
        if status_filter != "Todos":
            panel_df = panel_df[panel_df["status"] == status_filter]
        if machine_filter != "Todas":
            panel_df = panel_df[panel_df["machine_code"].fillna("Sem máquina") == machine_filter]
        if tech_filter != "Todos":
            val = "" if tech_filter == "Sem técnico" else tech_filter
            panel_df = panel_df[panel_df["assigned_technician"].fillna("") == val]

        for _, row in panel_df.iterrows():
            st.markdown(f'<div class="panel" style="margin-bottom:14px;"><div style="display:flex;justify-content:space-between;gap:16px;align-items:center;flex-wrap:wrap;"><div><div style="font-size:1.05rem;font-weight:800;">{row["os_number"]} • {row["machine_code"] or "-"}</div><div class="small-muted">{row["sector"]} • {row["criticality"]} • Solicitante: {row["requester"]}</div></div><div>{badge(row["status"])}</div></div></div>', unsafe_allow_html=True)
            desc, act = st.columns([1.55, 1.2])
            with desc:
                st.write(f"**Falha:** {row['description']}")
                st.write(f"**Abertura:** {row['open_dt']}")
                st.write(f"**Técnico:** {row['assigned_technician'] or '-'}")
                st.write(f"**Parada total:** {fmt_min(row['downtime_min'])}")
                if row.get("photo_path") and os.path.exists(row["photo_path"]):
                    st.image(row["photo_path"], caption="Foto da falha", use_container_width=True)
            with act:
                a1, a2 = st.columns(2)
                with a1:
                    if row["status"] in ["Aberta","Máquina Parada"] and st.button(f"Iniciar {row['id']}", use_container_width=True):
                        start = now_str()
                        response = mins_between(row["open_dt"], start)
                        q("UPDATE work_orders SET service_start_dt=?, status=?, response_min=? WHERE id=?", (start, "Em manutenção", response, int(row["id"])))
                        st.rerun()
                    if row["status"] == "Em manutenção" and st.button(f"Pausar {row['id']}", use_container_width=True):
                        q("UPDATE work_orders SET status=? WHERE id=?", ("Pausada", int(row["id"])))
                        st.rerun()
                with a2:
                    if row["status"] == "Pausada" and st.button(f"Retomar {row['id']}", use_container_width=True):
                        q("UPDATE work_orders SET status=? WHERE id=?", ("Em manutenção", int(row["id"])))
                        st.rerun()
                    if row["status"] in ["Em manutenção","Pausada"] and st.button(f"Finalizar {row['id']}", use_container_width=True):
                        end = now_str()
                        repair = mins_between(row["service_start_dt"], end)
                        downtime = mins_between(row["stop_start_dt"] or row["open_dt"], end)
                        labor_hours = round(repair / 60.0, 2)
                        rate = 0.0
                        if row["assigned_technician"]:
                            t = df("SELECT labor_rate FROM technicians WHERE name=?", (row["assigned_technician"],))
                            if not t.empty:
                                rate = float(t.iloc[0]["labor_rate"] or 0)
                        labor_cost = round(labor_hours * rate, 2)
                        total = round(labor_cost + float(row["parts_cost"] or 0), 2)
                        q("UPDATE work_orders SET service_end_dt=?, status=?, repair_min=?, downtime_min=?, labor_hours=?, labor_cost=?, total_cost=? WHERE id=?", (end, "Finalizada", repair, downtime, labor_hours, labor_cost, total, int(row["id"])))
                        fresh = df("SELECT * FROM work_orders WHERE id=?", (int(row["id"]),))
                        if not fresh.empty and int(fresh.iloc[0]["closed_summary_sent"] or 0) == 0:
                            ok, detail = send_close_summary(fresh.iloc[0].to_dict())
                            if ok:
                                q("UPDATE work_orders SET closed_summary_sent=1 WHERE id=?", (int(row["id"]),))
                                st.success(detail)
                        st.rerun()

                with st.expander("Registrar causa, ação, assinatura técnica e peças"):
                    root_cause = st.text_input(f"Causa raiz {row['id']}", value=row["root_cause"] or "")
                    action_taken = st.text_area(f"Ação executada {row['id']}", value=row["action_taken"] or "", height=100)
                    tech_sign = st.text_input(f"Assinatura técnico {row['id']}", value=row["technician_signature"] or (row["assigned_technician"] or ""))
                    st.markdown("**Baixa de peças**")
                    if parts_df.empty:
                        st.info("Cadastre peças primeiro.")
                    else:
                        pc1, pc2, pc3 = st.columns([1.3, 0.8, 0.8])
                        with pc1:
                            part_code = st.selectbox(f"Peça {row['id']}", parts_df["code"].tolist(), key=f"part_{row['id']}")
                        with pc2:
                            qty = st.number_input(f"Qtd {row['id']}", min_value=0.0, value=1.0, step=1.0, key=f"qty_{row['id']}")
                        with pc3:
                            if st.button(f"Adicionar peça {row['id']}", key=f"addpart_{row['id']}", use_container_width=True):
                                p = df("SELECT * FROM parts WHERE code=?", (part_code,))
                                if p.empty:
                                    st.error("Peça não encontrada.")
                                else:
                                    stock = float(p.iloc[0]["stock"] or 0)
                                    unit = float(p.iloc[0]["unit_cost"] or 0)
                                    if qty <= 0:
                                        st.error("Quantidade inválida.")
                                    elif qty > stock:
                                        st.error(f"Estoque insuficiente. Disponível: {stock}")
                                    else:
                                        total_cost = round(qty * unit, 2)
                                        q("INSERT INTO work_order_parts (wo_id, part_code, qty, unit_cost, total_cost) VALUES (?,?,?,?,?)", (int(row["id"]), part_code, qty, unit, total_cost))
                                        q("UPDATE parts SET stock=stock-? WHERE code=?", (qty, part_code))
                                        q("UPDATE work_orders SET parts_cost=COALESCE(parts_cost,0)+?, total_cost=COALESCE(total_cost,0)+? WHERE id=?", (total_cost, total_cost, int(row["id"])))
                                        st.success("Peça baixada do estoque.")
                                        st.rerun()

                    used = df("SELECT part_code, qty, unit_cost, total_cost FROM work_order_parts WHERE wo_id=?", (int(row["id"]),))
                    if not used.empty:
                        st.dataframe(used, use_container_width=True, hide_index=True)

                    if st.button(f"Salvar detalhes {row['id']}", key=f"save_{row['id']}", use_container_width=True):
                        q("UPDATE work_orders SET root_cause=?, action_taken=?, technician_signature=? WHERE id=?", (root_cause, action_taken, tech_sign, int(row["id"])))
                        st.success("Detalhes atualizados.")

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
        view = hist[["os_number","open_dt","sector","machine_code","criticality","status","assigned_technician","tempo_resposta","tempo_reparo","tempo_parada","total_cost"]]
        st.dataframe(view, use_container_width=True, hide_index=True)
        st.download_button("Exportar CSV", data=hist.to_csv(index=False).encode("utf-8-sig"), file_name="historico_manutencao.csv", mime="text/csv", use_container_width=True)

elif page == "Cadastros":
    require_gestor()
    t1, t2, t3, t4 = st.tabs(["Máquinas","Técnicos","Peças","Usuários"])

    with t1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de máquinas")
        with st.form("f_machine", clear_on_submit=True):
            a, b, c, d = st.columns(4)
            code = a.text_input("Código")
            name = b.text_input("Nome")
            sector = c.text_input("Setor")
            criticality = d.selectbox("Criticidade", ["Baixa","Média","Alta","Crítica"])
            if st.form_submit_button("Salvar máquina", use_container_width=True):
                try:
                    q("INSERT INTO machines (code,name,sector,criticality,active) VALUES (?,?,?,?,1)", (code, name, sector, criticality))
                    st.success("Máquina cadastrada.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Não foi possível salvar: {e}")
        st.dataframe(machines_df, use_container_width=True, hide_index=True)

    with t2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de técnicos")
        with st.form("f_tech", clear_on_submit=True):
            a, b, c = st.columns(3)
            name = a.text_input("Nome técnico")
            rate = b.number_input("Custo hora", min_value=0.0, value=85.0, step=1.0)
            phone = c.text_input("Telefone")
            if st.form_submit_button("Salvar técnico", use_container_width=True):
                q("INSERT INTO technicians (name,labor_rate,phone) VALUES (?,?,?)", (name, rate, phone))
                st.success("Técnico cadastrado.")
                st.rerun()
        st.dataframe(tech_df, use_container_width=True, hide_index=True)

    with t3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de peças")
        with st.form("f_part", clear_on_submit=True):
            a, b, c, d, e = st.columns(5)
            pcode = a.text_input("Código")
            pname = b.text_input("Descrição")
            stock = c.number_input("Estoque", min_value=0.0, value=0.0, step=1.0)
            min_stock = d.number_input("Estoque mínimo", min_value=0.0, value=0.0, step=1.0)
            cost = e.number_input("Custo unitário", min_value=0.0, value=0.0, step=0.1)
            if st.form_submit_button("Salvar peça", use_container_width=True):
                try:
                    q("INSERT INTO parts (code,name,stock,min_stock,unit_cost) VALUES (?,?,?,?,?)", (pcode, pname, stock, min_stock, cost))
                    st.success("Peça cadastrada.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Não foi possível salvar: {e}")
        st.dataframe(parts_df, use_container_width=True, hide_index=True)

    with t4:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de usuários")
        users_df = df("SELECT username, profile, full_name FROM users ORDER BY username")
        with st.form("f_user", clear_on_submit=True):
            a, b, c, d = st.columns(4)
            un = a.text_input("Usuário")
            pw = b.text_input("Senha")
            pf = c.selectbox("Perfil", ["Operador","Manutenção","Gestor"])
            fn = d.text_input("Nome completo")
            if st.form_submit_button("Salvar usuário", use_container_width=True):
                try:
                    q("INSERT INTO users (username,password,profile,full_name) VALUES (?,?,?,?)", (un, hash_password(pw), pf, fn))
                    st.success("Usuário cadastrado.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Não foi possível salvar: {e}")
        st.dataframe(users_df, use_container_width=True, hide_index=True)

elif page == "Preventiva":
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
    if prev_df.empty:
        st.info("Nenhuma preventiva cadastrada.")
    else:
        show = prev_df.copy()
        show["status_prev"] = pd.to_datetime(show["next_due_date"], errors="coerce").dt.date.apply(lambda d: "Vencida/Hoje" if d and d <= datetime.now().date() else "Programada")
        st.dataframe(show, use_container_width=True, hide_index=True)

elif page == "Segurança":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🔐 Segurança")
    st.caption("As senhas são gravadas com hash SHA-256.")
    with st.form("change_password"):
        current = st.text_input("Senha atual", type="password")
        new1 = st.text_input("Nova senha", type="password")
        new2 = st.text_input("Confirmar nova senha", type="password")
        submit = st.form_submit_button("Alterar senha", use_container_width=True)
        if submit:
            user_db = verify_login(user["username"], current)
            if not user_db:
                st.error("Senha atual inválida.")
            elif not new1 or len(new1) < 4:
                st.error("A nova senha precisa ter pelo menos 4 caracteres.")
            elif new1 != new2:
                st.error("As senhas não conferem.")
            else:
                q("UPDATE users SET password=? WHERE username=?", (hash_password(new1), user["username"]))
                st.success("Senha alterada com sucesso.")

elif page == "Configurações":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🔌 Configurações da fábrica")
    st.code("""# Banco
DB_MODE = "sqlite"  # ou "postgres"
POSTGRES_URL = "postgresql://usuario:senha@host:5432/banco"

# WhatsApp
TWILIO_ACCOUNT_SID = "seu_account_sid"
TWILIO_AUTH_TOKEN = "seu_auth_token"
TWILIO_WHATSAPP_FROM = "whatsapp:+14155238886"

WHATSAPP_MANUTENCAO = "whatsapp:+5547999204759,whatsapp:+5547989190422"
WHATSAPP_GESTAO = "whatsapp:+5546991144902"

ESCALATION_MINUTES = "30"
TV_REFRESH_SECONDS = "10"
""")
    st.write("Cadastros: acesso exclusivo do Gestor da Manutenção.")
    st.write("DB_MODE = postgres ativa PostgreSQL quando POSTGRES_URL estiver configurada.")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.subheader("Logo da empresa")
    logo = st.file_uploader("Enviar logo PNG/JPG", type=["png","jpg","jpeg"])
    if logo is not None:
        with open(LOGO_PATH, "wb") as f:
            f.write(logo.getbuffer())
        st.success("Logo salva com sucesso.")
        st.image(LOGO_PATH, width=180)
    elif os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=180)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    test_msg = st.text_area("Mensagem de teste", value="🚨 Teste da V8 fábrica completa.")
    t1, t2 = st.columns(2)
    with t1:
        if st.button("Enviar teste manutenção", use_container_width=True):
            ok, detail = send_whatsapp(test_msg, manut_nums())
            st.success(detail) if ok else st.error(detail)
    with t2:
        if st.button("Enviar teste gestão", use_container_width=True):
            ok, detail = send_whatsapp(test_msg, gestao_nums())
            st.success(detail) if ok else st.error(detail)

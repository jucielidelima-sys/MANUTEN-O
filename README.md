import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from twilio.rest import Client

DB_PATH = "manutencao_v5_whatsapp.db"
UPLOAD_DIR = "uploads"

st.set_page_config(page_title="Manutenção V5 + WhatsApp", page_icon="🏭", layout="wide")

st.markdown("""
<style>
:root {
    --bg: #0b1118;
    --panel: rgba(18, 26, 36, 0.92);
    --panel2: rgba(24, 34, 46, 0.94);
    --line: rgba(92, 200, 255, 0.22);
    --accent: #5cc8ff;
    --accent2: #76ffd1;
    --text: #eef5fb;
    --muted: #9fb4c8;
    --danger: #ff6b6b;
    --warn: #ffb84d;
}
.stApp {
    background:
      linear-gradient(135deg, rgba(10,15,20,0.98), rgba(8,12,18,0.98)),
      repeating-linear-gradient(
        45deg,
        rgba(255,255,255,0.02) 0px,
        rgba(255,255,255,0.02) 8px,
        rgba(0,0,0,0) 8px,
        rgba(0,0,0,0) 16px
      );
    color: var(--text);
}
.block-container {
    max-width: 1450px;
    padding-top: 1.1rem;
    padding-bottom: 1.4rem;
}
.panel {
    background: linear-gradient(180deg, rgba(21,29,39,0.95), rgba(14,20,28,0.95));
    border: 1px solid var(--line);
    border-radius: 22px;
    padding: 18px;
    box-shadow: 0 12px 28px rgba(0,0,0,0.32), 0 0 24px rgba(92,200,255,0.06);
}
.kpi {
    background: linear-gradient(180deg, rgba(21,29,39,0.98), rgba(14,20,28,0.98));
    border: 1px solid rgba(92,200,255,0.22);
    border-radius: 22px;
    padding: 18px;
    min-height: 118px;
    box-shadow: 0 10px 26px rgba(0,0,0,0.28), 0 0 22px rgba(92,200,255,0.06);
}
.kpi-label { color: var(--muted); font-size: 0.92rem; margin-bottom: 8px; }
.kpi-value { color: var(--text); font-size: 2rem; font-weight: 800; line-height: 1.05; }
.kpi-sub { color: var(--accent2); margin-top: 8px; font-size: 0.9rem; }
.header-box {
    background: linear-gradient(135deg, rgba(20,28,38,0.96), rgba(12,18,26,0.94));
    border: 1px solid var(--line);
    border-radius: 24px;
    padding: 18px 22px;
    margin-bottom: 14px;
    box-shadow: 0 12px 30px rgba(0,0,0,0.35), 0 0 22px rgba(92,200,255,0.08);
}
.small-muted { color: var(--muted); font-size: 0.88rem; }
.divider {
    height: 1px;
    background: linear-gradient(90deg, rgba(92,200,255,0), rgba(92,200,255,0.28), rgba(92,200,255,0));
    margin: 12px 0 18px 0;
}
.status-pill {
    display: inline-block;
    padding: 6px 10px;
    border-radius: 999px;
    font-weight: 700;
    font-size: 0.8rem;
    border: 1px solid rgba(255,255,255,0.08);
}
.pill-aberta { background: rgba(92,200,255,0.18); color: #d9f3ff; }
.pill-parada { background: rgba(255,107,107,0.18); color: #ffd6d6; }
.pill-manutencao { background: rgba(255,184,77,0.18); color: #ffe2b5; }
.pill-pausada { background: rgba(180,180,180,0.18); color: #efefef; }
.pill-finalizada { background: rgba(118,255,209,0.18); color: #cffff1; }
.alert-box {
    padding: 14px 16px;
    border-radius: 18px;
    background: linear-gradient(180deg, rgba(64,20,20,0.92), rgba(44,12,12,0.92));
    border: 1px solid rgba(255,107,107,0.30);
}
.stButton > button, .stDownloadButton > button {
    border-radius: 14px !important;
    border: 1px solid rgba(92,200,255,0.25) !important;
    background: linear-gradient(180deg, rgba(24,34,46,1), rgba(16,22,30,1)) !important;
    color: white !important;
    font-weight: 700 !important;
}
</style>
""", unsafe_allow_html=True)

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def run_query(query, params=(), fetch=False):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    data = cur.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return data

def df_from(query, params=()):
    rows = run_query(query, params, fetch=True)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])

def init_db():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        profile TEXT,
        full_name TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS machines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT,
        sector TEXT,
        criticality TEXT,
        active INTEGER DEFAULT 1
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS technicians (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        labor_rate REAL DEFAULT 0,
        phone TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS parts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT,
        stock REAL DEFAULT 0,
        unit_cost REAL DEFAULT 0
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS work_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        notes TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS work_order_parts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wo_id INTEGER,
        part_code TEXT,
        qty REAL,
        unit_cost REAL,
        total_cost REAL
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS preventive_plans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_code TEXT,
        title TEXT,
        frequency_days INTEGER,
        last_done_date TEXT,
        next_due_date TEXT,
        responsible TEXT,
        notes TEXT,
        active INTEGER DEFAULT 1
    )""")

    users_n = cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if users_n == 0:
        cur.executemany(
            "INSERT INTO users (username,password,profile,full_name) VALUES (?,?,?,?)",
            [
                ("operador","1234","Operador","Operador Padrão"),
                ("manutencao","1234","Manutenção","Equipe de Manutenção"),
                ("gestor","1234","Gestor","Gestor Industrial"),
            ],
        )

    machines_n = cur.execute("SELECT COUNT(*) FROM machines").fetchone()[0]
    if machines_n == 0:
        cur.executemany(
            "INSERT INTO machines (code,name,sector,criticality,active) VALUES (?,?,?,?,1)",
            [
                ("SLT-01","Slitter Principal","Conversão","Alta"),
                ("PRS-02","Prensa Hidráulica","Estamparia","Média"),
                ("EMB-03","Embutidora","Montagem","Alta"),
                ("BNC-04","Bancada de Teste","Qualidade","Baixa"),
            ],
        )

    techs_n = cur.execute("SELECT COUNT(*) FROM technicians").fetchone()[0]
    if techs_n == 0:
        cur.executemany(
            "INSERT INTO technicians (name,labor_rate,phone) VALUES (?,?,?)",
            [
                ("Carlos Manutenção", 85.0, ""),
                ("Fernanda Mecânica", 92.0, ""),
                ("Rafael Elétrica", 98.0, ""),
            ],
        )

    parts_n = cur.execute("SELECT COUNT(*) FROM parts").fetchone()[0]
    if parts_n == 0:
        cur.executemany(
            "INSERT INTO parts (code,name,stock,unit_cost) VALUES (?,?,?,?)",
            [
                ("ROL-6204","Rolamento 6204", 20, 28.5),
                ("COR-A36","Correia A36", 10, 65.0),
                ("SEN-PNP","Sensor PNP", 8, 115.0),
                ("CNT-09A","Contator 9A", 12, 54.9),
            ],
        )

    conn.commit()
    conn.close()

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def minutes_between(start, end):
    if not start or not end:
        return 0.0
    try:
        d1 = datetime.fromisoformat(start)
        d2 = datetime.fromisoformat(end)
        return round((d2 - d1).total_seconds() / 60.0, 2)
    except Exception:
        return 0.0

def format_min(mins):
    mins = float(mins or 0)
    h = int(mins // 60)
    m = int(mins % 60)
    return f"{h:02d}h {m:02d}m"

def status_badge(status):
    css = {
        "Aberta": "pill-aberta",
        "Máquina Parada": "pill-parada",
        "Em manutenção": "pill-manutencao",
        "Pausada": "pill-pausada",
        "Finalizada": "pill-finalizada",
    }.get(status, "pill-aberta")
    return f'<span class="status-pill {css}">{status}</span>'

def get_secret_or_env(name):
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass
    return os.getenv(name)

def send_whatsapp_alert(message_text):
    sid = get_secret_or_env("TWILIO_ACCOUNT_SID")
    token = get_secret_or_env("TWILIO_AUTH_TOKEN")
    from_number = get_secret_or_env("TWILIO_WHATSAPP_FROM")
    to_numbers_raw = get_secret_or_env("TWILIO_WHATSAPP_TO")

    if not sid or not token or not from_number or not to_numbers_raw:
        return False, "Credenciais do Twilio não configuradas."

    try:
        client = Client(sid, token)
        to_numbers = [n.strip() for n in str(to_numbers_raw).split(",") if n.strip()]
        sent = []
        for to_number in to_numbers:
            msg = client.messages.create(
                body=message_text,
                from_=from_number,
                to=to_number,
            )
            sent.append(msg.sid)
        return True, f"WhatsApp enviado para {len(sent)} destino(s)."
    except Exception as e:
        return False, f"Falha ao enviar WhatsApp: {e}"

def ensure_session():
    if "auth" not in st.session_state:
        st.session_state.auth = False
    if "user" not in st.session_state:
        st.session_state.user = None

init_db()
ensure_session()

def login_screen():
    st.markdown('<div class="header-box"><h1>🏭 Manutenção V5 Premium</h1><div class="small-muted">WhatsApp integrado • corretiva • preventiva • peças • custos • foto da falha</div></div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1.1, 1.2, 1.1])
    with c2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Entrar no sistema")
        username = st.text_input("Usuário", value="gestor")
        password = st.text_input("Senha", type="password", value="1234")
        if st.button("Acessar", use_container_width=True):
            df = df_from("SELECT * FROM users WHERE username=? AND password=?", (username, password))
            if not df.empty:
                st.session_state.auth = True
                st.session_state.user = df.iloc[0].to_dict()
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")
        st.markdown("</div>", unsafe_allow_html=True)

if not st.session_state.auth:
    login_screen()
    st.stop()

user = st.session_state.user

with st.sidebar:
    st.markdown(f"### 👤 {user['full_name']}")
    st.caption(f"Perfil: {user['profile']}")
    page = st.radio(
        "Módulos",
        ["Dashboard", "Abrir OS", "Painel de OS", "Histórico", "Cadastros", "Preventiva", "Configurações"],
    )
    if st.button("Sair", use_container_width=True):
        st.session_state.auth = False
        st.session_state.user = None
        st.rerun()

st.markdown('<div class="header-box"><h1>⚙️ Centro de Manutenção Industrial</h1><div class="small-muted">Visual premium estilo MES/Tesla com alertas por WhatsApp</div></div>', unsafe_allow_html=True)

wo_df = df_from("SELECT * FROM work_orders ORDER BY id DESC")
machines_df = df_from("SELECT * FROM machines WHERE active=1 ORDER BY code")
tech_df = df_from("SELECT * FROM technicians ORDER BY name")
parts_df = df_from("SELECT * FROM parts ORDER BY code")
prev_df = df_from("SELECT * FROM preventive_plans ORDER BY next_due_date")

if page == "Dashboard":
    total_os = 0 if wo_df.empty else len(wo_df)
    abertas = 0 if wo_df.empty else int(wo_df["status"].isin(["Aberta", "Máquina Parada"]).sum())
    manut = 0 if wo_df.empty else int((wo_df["status"] == "Em manutenção").sum())
    paradas = 0 if wo_df.empty else int((wo_df["status"] == "Máquina Parada").sum())
    finalizadas = 0 if wo_df.empty else int((wo_df["status"] == "Finalizada").sum())
    mttr_series = pd.to_numeric(wo_df["repair_min"], errors="coerce").fillna(0) if not wo_df.empty else pd.Series(dtype=float)
    mttr = round(mttr_series[mttr_series > 0].mean(), 1) if not mttr_series.empty and (mttr_series > 0).any() else 0
    if wo_df.empty:
        mtbf = 0
    else:
        fails = wo_df[wo_df["status"] == "Finalizada"].groupby("machine_code").size()
        mtbf = round(30 * 24 / max(fails.mean(), 1), 1) if len(fails) > 0 else 0
    custo_total = 0 if wo_df.empty else round(pd.to_numeric(wo_df["total_cost"], errors="coerce").fillna(0).sum(), 2)

    cols = st.columns(6)
    card_data = [
        ("OS Totais", total_os, "Visão geral"),
        ("Abertas", abertas, "Necessitam ação"),
        ("Em manutenção", manut, "Equipe atuando"),
        ("Máquinas paradas", paradas, "Impacto produtivo"),
        ("MTTR", f"{mttr} min", "Tempo médio reparo"),
        ("Custo total", f"R$ {custo_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "Peças + mão de obra"),
    ]
    for col, (label, value, sub) in zip(cols, card_data):
        with col:
            st.markdown(f'<div class="kpi"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div><div class="kpi-sub">{sub}</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    left, right = st.columns([1.2, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Ocorrências por máquina")
        if wo_df.empty:
            st.info("Sem OS registradas ainda.")
        else:
            chart_df = wo_df.groupby("machine_code", dropna=False).size().reset_index(name="OS")
            chart_df["machine_code"] = chart_df["machine_code"].fillna("Sem máquina")
            fig = px.bar(chart_df, x="machine_code", y="OS", text="OS")
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#eef5fb",
                margin=dict(l=10, r=10, t=10, b=10),
                height=360
            )
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Indicadores avançados")
        st.write(f"**MTBF estimado:** {mtbf} h")
        st.write(f"**OS finalizadas:** {finalizadas}")
        if not prev_df.empty:
            due_count = (pd.to_datetime(prev_df["next_due_date"], errors="coerce").dt.date <= datetime.now().date()).sum()
        else:
            due_count = 0
        st.write(f"**Preventivas vencidas/hoje:** {int(due_count)}")
        if not wo_df.empty:
            crit = wo_df["criticality"].fillna("Não informado").value_counts().reset_index()
            crit.columns = ["Criticidade", "Qtd"]
            fig2 = px.pie(crit, names="Criticidade", values="Qtd", hole=0.55)
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#eef5fb", margin=dict(l=10, r=10, t=10, b=10), height=330)
            st.plotly_chart(fig2, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("Últimas OS")
    if wo_df.empty:
        st.info("Nenhuma OS cadastrada.")
    else:
        grid = wo_df[["os_number","open_dt","sector","machine_code","criticality","status","assigned_technician","downtime_min","total_cost"]].copy()
        grid["downtime_min"] = grid["downtime_min"].fillna(0).map(format_min)
        st.dataframe(grid, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

elif page == "Abrir OS":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("📋 Abrir ordem de serviço")
    with st.form("form_open_wo"):
        col1, col2, col3 = st.columns(3)
        with col1:
            sector = st.text_input("Setor", value="Produção")
            requester = st.text_input("Solicitante", value=user["full_name"])
            criticality = st.selectbox("Criticidade", ["Baixa", "Média", "Alta", "Crítica"], index=2)
        with col2:
            machine_code = st.selectbox("Máquina", machines_df["code"].tolist() if not machines_df.empty else [])
            status = st.selectbox("Status inicial", ["Máquina Parada", "Aberta"], index=0)
            assigned_technician = st.selectbox("Técnico responsável", [""] + (tech_df["name"].tolist() if not tech_df.empty else []))
        with col3:
            operator_signature = st.text_input("Assinatura operador", value=requester)
            notes = st.text_input("Observações rápidas")
            uploaded_file = st.file_uploader("Foto da falha", type=["png","jpg","jpeg"])
        description = st.text_area("Descrição da falha", height=120)
        submitted = st.form_submit_button("Abrir OS", use_container_width=True)

        if submitted:
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

                run_query(
                    """
                    INSERT INTO work_orders (
                        os_number, open_dt, sector, machine_code, requester, description,
                        criticality, status, stop_start_dt, assigned_technician,
                        operator_signature, photo_path, notes
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        os_number, now_str(), sector, machine_code, requester, description.strip(),
                        criticality, status, now_str() if status == "Máquina Parada" else None, assigned_technician,
                        operator_signature, photo_path, notes
                    )
                )
                st.success(f"OS {os_number} aberta com sucesso.")

                if status == "Máquina Parada":
                    msg = (
                        "🚨 MÁQUINA PARADA\n\n"
                        f"OS: {os_number}\n"
                        f"Máquina: {machine_code}\n"
                        f"Setor: {sector}\n"
                        f"Criticidade: {criticality}\n"
                        f"Solicitante: {requester}\n"
                        f"Problema: {description.strip()}\n\n"
                        "Ação imediata necessária."
                    )
                    ok, detail = send_whatsapp_alert(msg)
                    if ok:
                        st.success(detail)
                    else:
                        st.warning(detail)
                    st.markdown('<div class="alert-box"><strong>🚨 ALERTA:</strong> máquina marcada como parada.</div>', unsafe_allow_html=True)
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

elif page == "Painel de OS":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("⚙️ Painel operacional")
    if wo_df.empty:
        st.info("Nenhuma OS cadastrada.")
    else:
        filt1, filt2, filt3 = st.columns(3)
        with filt1:
            status_filter = st.selectbox("Filtrar status", ["Todos", "Aberta", "Máquina Parada", "Em manutenção", "Pausada", "Finalizada"])
        with filt2:
            machine_filter = st.selectbox("Filtrar máquina", ["Todas"] + wo_df["machine_code"].fillna("Sem máquina").unique().tolist())
        with filt3:
            tech_values = sorted(wo_df["assigned_technician"].fillna("").replace("", "Sem técnico").unique().tolist())
            tech_filter = st.selectbox("Filtrar técnico", ["Todos"] + tech_values)

        panel_df = wo_df.copy()
        if status_filter != "Todos":
            panel_df = panel_df[panel_df["status"] == status_filter]
        if machine_filter != "Todas":
            panel_df = panel_df[panel_df["machine_code"].fillna("Sem máquina") == machine_filter]
        if tech_filter != "Todos":
            value = "" if tech_filter == "Sem técnico" else tech_filter
            panel_df = panel_df[panel_df["assigned_technician"].fillna("") == value]

        for _, row in panel_df.iterrows():
            st.markdown(f'''
            <div class="panel" style="margin-bottom: 14px;">
                <div style="display:flex;justify-content:space-between;gap:16px;align-items:center;flex-wrap:wrap;">
                    <div>
                        <div style="font-size:1.05rem;font-weight:800;">{row["os_number"]} • {row["machine_code"] or "-"}</div>
                        <div class="small-muted">{row["sector"]} • {row["criticality"]} • Solicitante: {row["requester"]}</div>
                    </div>
                    <div>{status_badge(row["status"])}</div>
                </div>
            </div>
            ''', unsafe_allow_html=True)

            desc_col, act_col = st.columns([1.6, 1.2])
            with desc_col:
                st.write(f"**Falha:** {row['description']}")
                st.write(f"**Abertura:** {row['open_dt']}")
                st.write(f"**Técnico:** {row['assigned_technician'] or '-'}")
                st.write(f"**Parada total:** {format_min(row['downtime_min'])}")
                if row.get("photo_path") and os.path.exists(row["photo_path"]):
                    st.image(row["photo_path"], caption="Foto da falha", use_container_width=True)

            with act_col:
                c1, c2 = st.columns(2)
                with c1:
                    if row["status"] in ["Aberta", "Máquina Parada"] and st.button(f"Iniciar {row['id']}", use_container_width=True):
                        start = now_str()
                        response = minutes_between(row["open_dt"], start)
                        run_query("UPDATE work_orders SET service_start_dt=?, status=?, response_min=? WHERE id=?", (start, "Em manutenção", response, int(row["id"])))
                        st.rerun()
                    if row["status"] == "Em manutenção" and st.button(f"Pausar {row['id']}", use_container_width=True):
                        run_query("UPDATE work_orders SET status=? WHERE id=?", ("Pausada", int(row["id"])))
                        st.rerun()

                with c2:
                    if row["status"] == "Pausada" and st.button(f"Retomar {row['id']}", use_container_width=True):
                        run_query("UPDATE work_orders SET status=? WHERE id=?", ("Em manutenção", int(row["id"])))
                        st.rerun()
                    if row["status"] in ["Em manutenção", "Pausada"] and st.button(f"Finalizar {row['id']}", use_container_width=True):
                        end = now_str()
                        repair = minutes_between(row["service_start_dt"], end)
                        downtime = minutes_between(row["stop_start_dt"] or row["open_dt"], end)
                        labor_hours = round(repair / 60.0, 2)
                        tech_rate = 0
                        if row["assigned_technician"]:
                            tdf = df_from("SELECT labor_rate FROM technicians WHERE name=?", (row["assigned_technician"],))
                            if not tdf.empty:
                                tech_rate = float(tdf.iloc[0]["labor_rate"] or 0)
                        labor_cost = round(labor_hours * tech_rate, 2)
                        total = round(labor_cost + float(row["parts_cost"] or 0), 2)
                        run_query(
                            "UPDATE work_orders SET service_end_dt=?, status=?, repair_min=?, downtime_min=?, labor_hours=?, labor_cost=?, total_cost=? WHERE id=?",
                            (end, "Finalizada", repair, downtime, labor_hours, labor_cost, total, int(row["id"]))
                        )
                        st.rerun()

                with st.expander("Registrar causa, ação, assinatura técnica e peças"):
                    root_cause = st.text_input(f"Causa raiz {row['id']}", value=row["root_cause"] or "")
                    action_taken = st.text_area(f"Ação executada {row['id']}", value=row["action_taken"] or "", height=110)
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
                                pdf = df_from("SELECT * FROM parts WHERE code=?", (part_code,))
                                if pdf.empty:
                                    st.error("Peça não encontrada.")
                                else:
                                    stock = float(pdf.iloc[0]["stock"] or 0)
                                    unit = float(pdf.iloc[0]["unit_cost"] or 0)
                                    if qty <= 0:
                                        st.error("Quantidade inválida.")
                                    elif qty > stock:
                                        st.error(f"Estoque insuficiente. Disponível: {stock}")
                                    else:
                                        total_cost = round(qty * unit, 2)
                                        run_query("INSERT INTO work_order_parts (wo_id, part_code, qty, unit_cost, total_cost) VALUES (?,?,?,?,?)", (int(row["id"]), part_code, qty, unit, total_cost))
                                        run_query("UPDATE parts SET stock=stock-? WHERE code=?", (qty, part_code))
                                        run_query("UPDATE work_orders SET parts_cost=COALESCE(parts_cost,0)+?, total_cost=COALESCE(total_cost,0)+? WHERE id=?", (total_cost, total_cost, int(row["id"])))
                                        st.success("Peça baixada do estoque.")
                                        st.rerun()

                    parts_used = df_from("SELECT part_code, qty, unit_cost, total_cost FROM work_order_parts WHERE wo_id=?", (int(row["id"]),))
                    if not parts_used.empty:
                        st.dataframe(parts_used, use_container_width=True, hide_index=True)

                    if st.button(f"Salvar detalhes {row['id']}", key=f"save_{row['id']}", use_container_width=True):
                        run_query("UPDATE work_orders SET root_cause=?, action_taken=?, technician_signature=? WHERE id=?", (root_cause, action_taken, tech_sign, int(row["id"])))
                        st.success("Detalhes atualizados.")
    st.markdown("</div>", unsafe_allow_html=True)

elif page == "Histórico":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("📚 Histórico")
    if wo_df.empty:
        st.info("Sem histórico.")
    else:
        hist = wo_df.copy()
        hist["downtime_fmt"] = hist["downtime_min"].fillna(0).map(format_min)
        hist["repair_fmt"] = hist["repair_min"].fillna(0).map(format_min)
        view = hist[["os_number","open_dt","sector","machine_code","criticality","status","assigned_technician","response_min","repair_fmt","downtime_fmt","total_cost"]].copy()
        st.dataframe(view, use_container_width=True, hide_index=True)
        st.download_button("Exportar CSV", data=hist.to_csv(index=False).encode("utf-8-sig"), file_name="historico_manutencao.csv", mime="text/csv", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

elif page == "Cadastros":
    t1, t2, t3, t4 = st.tabs(["Máquinas", "Técnicos", "Peças", "Usuários"])

    with t1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de máquinas")
        with st.form("machine_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            code = c1.text_input("Código")
            name = c2.text_input("Nome")
            sector = c3.text_input("Setor")
            criticality = c4.selectbox("Criticidade", ["Baixa", "Média", "Alta", "Crítica"])
            if st.form_submit_button("Salvar máquina", use_container_width=True):
                try:
                    run_query("INSERT INTO machines (code,name,sector,criticality,active) VALUES (?,?,?,?,1)", (code, name, sector, criticality))
                    st.success("Máquina cadastrada.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Não foi possível salvar: {e}")
        st.dataframe(machines_df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de técnicos")
        with st.form("tech_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            name = c1.text_input("Nome técnico")
            rate = c2.number_input("Custo hora", min_value=0.0, value=85.0, step=1.0)
            phone = c3.text_input("Telefone")
            if st.form_submit_button("Salvar técnico", use_container_width=True):
                run_query("INSERT INTO technicians (name,labor_rate,phone) VALUES (?,?,?)", (name, rate, phone))
                st.success("Técnico cadastrado.")
                st.rerun()
        st.dataframe(tech_df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de peças")
        with st.form("part_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            pcode = c1.text_input("Código peça")
            pname = c2.text_input("Descrição")
            stock = c3.number_input("Estoque", min_value=0.0, value=0.0, step=1.0)
            cost = c4.number_input("Custo unitário", min_value=0.0, value=0.0, step=0.1)
            if st.form_submit_button("Salvar peça", use_container_width=True):
                try:
                    run_query("INSERT INTO parts (code,name,stock,unit_cost) VALUES (?,?,?,?)", (pcode, pname, stock, cost))
                    st.success("Peça cadastrada.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Não foi possível salvar: {e}")
        st.dataframe(parts_df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t4:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.subheader("Cadastro de usuários")
        users_df = df_from("SELECT username, profile, full_name FROM users ORDER BY username")
        with st.form("user_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            un = c1.text_input("Usuário")
            pw = c2.text_input("Senha")
            pf = c3.selectbox("Perfil", ["Operador", "Manutenção", "Gestor"])
            fn = c4.text_input("Nome completo")
            if st.form_submit_button("Salvar usuário", use_container_width=True):
                try:
                    run_query("INSERT INTO users (username,password,profile,full_name) VALUES (?,?,?,?)", (un, pw, pf, fn))
                    st.success("Usuário cadastrado.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Não foi possível salvar: {e}")
        st.dataframe(users_df, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

elif page == "Preventiva":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🛡️ Plano preventivo")
    with st.form("prev_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        machine_code = c1.selectbox("Máquina", machines_df["code"].tolist() if not machines_df.empty else [])
        title = c2.text_input("Título da preventiva")
        freq = c3.number_input("Frequência (dias)", min_value=1, value=30, step=1)
        responsible = c4.text_input("Responsável")
        c5, c6 = st.columns(2)
        last_done = c5.date_input("Última execução", value=datetime.now().date())
        notes = c6.text_input("Observações")
        if st.form_submit_button("Salvar preventiva", use_container_width=True):
            next_due = last_done + timedelta(days=int(freq))
            run_query("INSERT INTO preventive_plans (machine_code,title,frequency_days,last_done_date,next_due_date,responsible,notes,active) VALUES (?,?,?,?,?,?,?,1)", (machine_code, title, int(freq), str(last_done), str(next_due), responsible, notes))
            st.success("Plano preventivo salvo.")
            st.rerun()

    if prev_df.empty:
        st.info("Nenhuma preventiva cadastrada.")
    else:
        show = prev_df.copy()
        show["status_prev"] = pd.to_datetime(show["next_due_date"], errors="coerce").dt.date.apply(lambda d: "Vencida/Hoje" if d and d <= datetime.now().date() else "Programada")
        st.dataframe(show, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

elif page == "Configurações":
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.subheader("🔌 Configuração do WhatsApp")
    st.write("O envio real usa o Twilio. Você pode configurar pelas variáveis de ambiente ou pelo `secrets.toml` do Streamlit Cloud.")
    st.code("""# .streamlit/secrets.toml
TWILIO_ACCOUNT_SID = "seu_account_sid"
TWILIO_AUTH_TOKEN = "seu_auth_token"
TWILIO_WHATSAPP_FROM = "whatsapp:+14155238886"
TWILIO_WHATSAPP_TO = "whatsapp:+5511999999999,whatsapp:+5549999999999"
""")

    st.write("Também funciona no Windows com:")
    st.code("""set TWILIO_ACCOUNT_SID=seu_account_sid
set TWILIO_AUTH_TOKEN=seu_auth_token
set TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
set TWILIO_WHATSAPP_TO=whatsapp:+5511999999999,whatsapp:+5549999999999
""")

    test_message = st.text_area("Mensagem de teste", value="🚨 Teste de alerta do app de manutenção.")
    if st.button("Enviar teste de WhatsApp", use_container_width=True):
        ok, detail = send_whatsapp_alert(test_message)
        if ok:
            st.success(detail)
        else:
            st.error(detail)

    st.write("Nesta versão, o alerta é disparado automaticamente quando a OS é aberta com status **Máquina Parada**.")
    st.markdown("</div>", unsafe_allow_html=True)

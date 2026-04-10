import os
import sqlite3
import hashlib
from datetime import datetime, timedelta
from contextlib import closing
from typing import Optional, List, Dict, Any

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

try:
    from twilio.rest import Client
except Exception:
    Client = None

DB_PATH = "manutencao_industrial.db"
APP_TITLE = "Sistema de Manutenção Industrial V3"
STATUS_ABERTA = "Máquina Parada"
STATUS_ANDAMENTO = "Em Manutenção"
STATUS_PAUSADA = "Pausada"
STATUS_FINALIZADA = "Finalizada"
STATUS_CANCELADA = "Cancelada"
STATUS_OPTIONS = [STATUS_ABERTA, STATUS_ANDAMENTO, STATUS_PAUSADA, STATUS_FINALIZADA, STATUS_CANCELADA]
PERFIS = ["Operador", "Manutenção", "Gestor"]


# =========================
# Infra / Banco
# =========================

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None


def minutes_between(start: Optional[str], end: Optional[str]) -> Optional[float]:
    dt_start = parse_dt(start)
    dt_end = parse_dt(end)
    if not dt_start or not dt_end:
        return None
    return round((dt_end - dt_start).total_seconds() / 60, 2)


def init_db() -> None:
    conn = get_conn()
    with closing(conn.cursor()) as cur:
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                nome TEXT NOT NULL,
                perfil TEXT NOT NULL,
                ativo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS machines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT UNIQUE NOT NULL,
                nome TEXT NOT NULL,
                setor TEXT NOT NULL,
                fabricante TEXT,
                modelo TEXT,
                criticidade INTEGER NOT NULL DEFAULT 3,
                ativo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS technicians (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                especialidade TEXT,
                custo_hora REAL NOT NULL DEFAULT 0,
                ativo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS parts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT UNIQUE NOT NULL,
                descricao TEXT NOT NULL,
                unidade TEXT NOT NULL DEFAULT 'UN',
                estoque_atual REAL NOT NULL DEFAULT 0,
                estoque_minimo REAL NOT NULL DEFAULT 0,
                custo_unitario REAL NOT NULL DEFAULT 0,
                ativo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS work_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_os TEXT UNIQUE NOT NULL,
                abertura_em TEXT NOT NULL,
                solicitante TEXT NOT NULL,
                setor TEXT NOT NULL,
                machine_id INTEGER NOT NULL,
                descricao_problema TEXT NOT NULL,
                criticidade TEXT NOT NULL,
                prioridade TEXT NOT NULL,
                status TEXT NOT NULL,
                parada_maquina INTEGER NOT NULL DEFAULT 1,
                inicio_reparo_em TEXT,
                ultima_pausa_em TEXT,
                total_pausado_min REAL NOT NULL DEFAULT 0,
                fim_reparo_em TEXT,
                tecnico_id INTEGER,
                causa_raiz TEXT,
                acao_executada TEXT,
                observacoes TEXT,
                sla_resposta_min REAL NOT NULL DEFAULT 30,
                custo_pecas REAL NOT NULL DEFAULT 0,
                custo_mo REAL NOT NULL DEFAULT 0,
                custo_total REAL NOT NULL DEFAULT 0,
                created_by INTEGER,
                FOREIGN KEY(machine_id) REFERENCES machines(id),
                FOREIGN KEY(tecnico_id) REFERENCES technicians(id),
                FOREIGN KEY(created_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS work_order_parts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_order_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                quantidade REAL NOT NULL,
                custo_unitario REAL NOT NULL,
                custo_total REAL NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY(part_id) REFERENCES parts(id)
            );

            CREATE TABLE IF NOT EXISTS stock_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                part_id INTEGER NOT NULL,
                tipo TEXT NOT NULL,
                quantidade REAL NOT NULL,
                referencia TEXT,
                observacao TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(part_id) REFERENCES parts(id)
            );

            CREATE TABLE IF NOT EXISTS notification_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canal TEXT NOT NULL,
                work_order_id INTEGER,
                destinatario TEXT,
                mensagem TEXT NOT NULL,
                status TEXT NOT NULL,
                detalhe TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(work_order_id) REFERENCES work_orders(id)
            );
            """
        )
        conn.commit()

    seed_data(conn)
    conn.close()


def seed_data(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    users = [
        ("operador", hash_password("1234"), "Operador Padrão", "Operador", 1, now_str()),
        ("manutencao", hash_password("1234"), "Técnico Padrão", "Manutenção", 1, now_str()),
        ("gestor", hash_password("1234"), "Gestor Padrão", "Gestor", 1, now_str()),
    ]
    for user in users:
        cur.execute(
            "INSERT OR IGNORE INTO users (username, password_hash, nome, perfil, ativo, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            user,
        )

    machines = [
        ("PRD-001", "Prensa Hidráulica 01", "Estamparia", "Schuler", "PH-300", 5, 1, now_str()),
        ("SLT-002", "Slitter 02", "Corte", "Custom", "SL-200", 4, 1, now_str()),
        ("CNC-003", "Centro de Usinagem 03", "Usinagem", "Romi", "D-800", 5, 1, now_str()),
    ]
    for machine in machines:
        cur.execute(
            "INSERT OR IGNORE INTO machines (codigo, nome, setor, fabricante, modelo, criticidade, ativo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            machine,
        )

    techs = [
        ("Carlos Silva", "Mecânica", 85.0, 1, now_str()),
        ("Ana Souza", "Elétrica", 95.0, 1, now_str()),
        ("João Lima", "Automação", 110.0, 1, now_str()),
    ]
    for tech in techs:
        cur.execute(
            "INSERT OR IGNORE INTO technicians (nome, especialidade, custo_hora, ativo, created_at) VALUES (?, ?, ?, ?, ?)",
            tech,
        )

    parts = [
        ("ROL-6205", "Rolamento 6205", "UN", 12, 4, 18.5, 1, now_str()),
        ("COR-A13", "Correia A13", "UN", 8, 2, 32.0, 1, now_str()),
        ("SEN-PNP", "Sensor PNP 24V", "UN", 6, 2, 95.0, 1, now_str()),
        ("FUS-10A", "Fusível 10A", "UN", 30, 10, 3.5, 1, now_str()),
    ]
    for part in parts:
        cur.execute(
            "INSERT OR IGNORE INTO parts (codigo, descricao, unidade, estoque_atual, estoque_minimo, custo_unitario, ativo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            part,
        )

    conn.commit()


# =========================
# Consultas
# =========================

def fetch_df(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def fetch_one(query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchone()
    finally:
        conn.close()


def execute(query: str, params: tuple = ()) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
    finally:
        conn.close()


def execute_returning_id(query: str, params: tuple = ()) -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


# =========================
# Notificações
# =========================

def log_notification(canal: str, work_order_id: Optional[int], destinatario: str, mensagem: str, status: str, detalhe: str = "") -> None:
    execute(
        "INSERT INTO notification_log (canal, work_order_id, destinatario, mensagem, status, detalhe, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (canal, work_order_id, destinatario, mensagem, status, detalhe, now_str()),
    )


def send_whatsapp_message(message: str, work_order_id: Optional[int] = None) -> Dict[str, str]:
    sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    token = os.getenv("TWILIO_AUTH_TOKEN", "")
    sender = os.getenv("TWILIO_WHATSAPP_FROM", "")
    target = os.getenv("TWILIO_WHATSAPP_TO", "")

    if not all([sid, token, sender, target]):
        detalhe = "Credenciais do Twilio não configuradas nas variáveis de ambiente."
        log_notification("WhatsApp", work_order_id, target or "não configurado", message, "pendente", detalhe)
        return {"status": "pendente", "detail": detalhe}

    if Client is None:
        detalhe = "Biblioteca twilio não disponível no ambiente."
        log_notification("WhatsApp", work_order_id, target, message, "erro", detalhe)
        return {"status": "erro", "detail": detalhe}

    try:
        client = Client(sid, token)
        client.messages.create(body=message, from_=sender, to=target)
        log_notification("WhatsApp", work_order_id, target, message, "enviado", "Mensagem enviada com sucesso")
        return {"status": "enviado", "detail": "Mensagem enviada com sucesso"}
    except Exception as exc:
        detalhe = str(exc)
        log_notification("WhatsApp", work_order_id, target, message, "erro", detalhe)
        return {"status": "erro", "detail": detalhe}


# =========================
# Regras de negócio
# =========================

def next_os_number() -> str:
    row = fetch_one("SELECT numero_os FROM work_orders ORDER BY id DESC LIMIT 1")
    if not row:
        return "OS-000001"
    last = row[0]
    try:
        num = int(str(last).split("-")[-1]) + 1
    except Exception:
        num = 1
    return f"OS-{num:06d}"


def authenticate(username: str, password: str) -> Optional[Dict[str, Any]]:
    row = fetch_one("SELECT * FROM users WHERE username = ? AND ativo = 1", (username,))
    if row and row["password_hash"] == hash_password(password):
        return dict(row)
    return None


def get_open_work_orders() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT wo.*, m.codigo AS maquina_codigo, m.nome AS maquina_nome, t.nome AS tecnico_nome
        FROM work_orders wo
        JOIN machines m ON m.id = wo.machine_id
        LEFT JOIN technicians t ON t.id = wo.tecnico_id
        WHERE wo.status IN (?, ?, ?)
        ORDER BY wo.id DESC
        """,
        (STATUS_ABERTA, STATUS_ANDAMENTO, STATUS_PAUSADA),
    )


def get_all_work_orders() -> pd.DataFrame:
    return fetch_df(
        """
        SELECT wo.*, m.codigo AS maquina_codigo, m.nome AS maquina_nome, t.nome AS tecnico_nome
        FROM work_orders wo
        JOIN machines m ON m.id = wo.machine_id
        LEFT JOIN technicians t ON t.id = wo.tecnico_id
        ORDER BY wo.id DESC
        """
    )


def work_order_metrics(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "abertas": 0,
            "paradas": 0,
            "em_manutencao": 0,
            "finalizadas": 0,
            "mttr": 0,
            "mtbf_h": 0,
            "custo_total": 0,
        }

    df = df.copy()
    df["tempo_reparo_min_calc"] = df.apply(
        lambda r: calculate_repair_minutes(r["inicio_reparo_em"], r["fim_reparo_em"], r["total_pausado_min"]), axis=1
    )
    finalizadas = df[df["status"] == STATUS_FINALIZADA]

    mttr = round(finalizadas["tempo_reparo_min_calc"].dropna().mean(), 2) if not finalizadas.empty else 0

    mtbf_h = 0
    if len(finalizadas) >= 2:
        finalizadas = finalizadas.sort_values("abertura_em")
        gaps = []
        previous_end = None
        for _, row in finalizadas.iterrows():
            current_start = parse_dt(row["abertura_em"])
            if previous_end and current_start:
                gaps.append((current_start - previous_end).total_seconds() / 3600)
            previous_end = parse_dt(row["fim_reparo_em"])
        if gaps:
            mtbf_h = round(sum(gaps) / len(gaps), 2)

    return {
        "abertas": int((df["status"].isin([STATUS_ABERTA, STATUS_PAUSADA, STATUS_ANDAMENTO])).sum()),
        "paradas": int((df["status"] == STATUS_ABERTA).sum()),
        "em_manutencao": int((df["status"] == STATUS_ANDAMENTO).sum()),
        "finalizadas": int((df["status"] == STATUS_FINALIZADA).sum()),
        "mttr": mttr,
        "mtbf_h": mtbf_h,
        "custo_total": round(df["custo_total"].fillna(0).sum(), 2),
    }


def calculate_response_minutes(abertura_em: Optional[str], inicio_reparo_em: Optional[str]) -> Optional[float]:
    return minutes_between(abertura_em, inicio_reparo_em)


def calculate_repair_minutes(inicio_reparo_em: Optional[str], fim_reparo_em: Optional[str], total_pausado_min: float = 0) -> Optional[float]:
    total = minutes_between(inicio_reparo_em, fim_reparo_em)
    if total is None:
        return None
    return round(max(total - (total_pausado_min or 0), 0), 2)


def calculate_downtime_minutes(abertura_em: Optional[str], fim_reparo_em: Optional[str]) -> Optional[float]:
    return minutes_between(abertura_em, fim_reparo_em)


def create_work_order(
    solicitante: str,
    setor: str,
    machine_id: int,
    descricao_problema: str,
    criticidade: str,
    prioridade: str,
    parada_maquina: bool,
    sla_resposta_min: float,
    created_by: int,
) -> int:
    numero = next_os_number()
    wo_id = execute_returning_id(
        """
        INSERT INTO work_orders (
            numero_os, abertura_em, solicitante, setor, machine_id, descricao_problema,
            criticidade, prioridade, status, parada_maquina, sla_resposta_min, created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            numero,
            now_str(),
            solicitante,
            setor,
            machine_id,
            descricao_problema,
            criticidade,
            prioridade,
            STATUS_ABERTA if parada_maquina else STATUS_ANDAMENTO,
            1 if parada_maquina else 0,
            sla_resposta_min,
            created_by,
        ),
    )

    machine = fetch_one("SELECT codigo, nome FROM machines WHERE id = ?", (machine_id,))
    msg = f"🚨 {numero} | Máquina parada: {machine['codigo']} - {machine['nome']} | Setor: {setor} | Problema: {descricao_problema}"
    send_whatsapp_message(msg, wo_id)
    return wo_id


def start_work_order(wo_id: int, technician_id: int) -> None:
    row = fetch_one("SELECT status, inicio_reparo_em FROM work_orders WHERE id = ?", (wo_id,))
    if not row or row["status"] in [STATUS_FINALIZADA, STATUS_CANCELADA]:
        return

    execute(
        "UPDATE work_orders SET tecnico_id = ?, status = ?, inicio_reparo_em = COALESCE(inicio_reparo_em, ?) WHERE id = ?",
        (technician_id, STATUS_ANDAMENTO, now_str(), wo_id),
    )


def pause_work_order(wo_id: int) -> None:
    row = fetch_one("SELECT status FROM work_orders WHERE id = ?", (wo_id,))
    if not row or row["status"] != STATUS_ANDAMENTO:
        return
    execute(
        "UPDATE work_orders SET status = ?, ultima_pausa_em = ? WHERE id = ?",
        (STATUS_PAUSADA, now_str(), wo_id),
    )


def resume_work_order(wo_id: int) -> None:
    row = fetch_one("SELECT status, ultima_pausa_em, total_pausado_min FROM work_orders WHERE id = ?", (wo_id,))
    if not row or row["status"] != STATUS_PAUSADA:
        return
    pausa = parse_dt(row["ultima_pausa_em"])
    total_pausado = row["total_pausado_min"] or 0
    if pausa:
        total_pausado += round((datetime.now() - pausa).total_seconds() / 60, 2)
    execute(
        "UPDATE work_orders SET status = ?, ultima_pausa_em = NULL, total_pausado_min = ? WHERE id = ?",
        (STATUS_ANDAMENTO, total_pausado, wo_id),
    )


def consume_part(part_id: int, quantity: float, reference: str, note: str) -> Dict[str, str]:
    part = fetch_one("SELECT * FROM parts WHERE id = ?", (part_id,))
    if not part:
        return {"status": "erro", "detail": "Peça não encontrada"}
    if part["estoque_atual"] < quantity:
        return {"status": "erro", "detail": f"Estoque insuficiente para {part['descricao']}"}

    new_stock = part["estoque_atual"] - quantity
    execute("UPDATE parts SET estoque_atual = ? WHERE id = ?", (new_stock, part_id))
    execute(
        "INSERT INTO stock_movements (part_id, tipo, quantidade, referencia, observacao, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (part_id, "SAÍDA", quantity, reference, note, now_str()),
    )
    return {"status": "ok", "detail": "Estoque atualizado"}


def finalize_work_order(
    wo_id: int,
    tecnico_id: int,
    causa_raiz: str,
    acao_executada: str,
    observacoes: str,
    horas_mo: float,
    part_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    row = fetch_one("SELECT * FROM work_orders WHERE id = ?", (wo_id,))
    if not row:
        return {"status": "erro", "detail": "OS não encontrada"}

    if row["status"] == STATUS_PAUSADA:
        resume_work_order(wo_id)
        row = fetch_one("SELECT * FROM work_orders WHERE id = ?", (wo_id,))

    finish_time = now_str()
    tech = fetch_one("SELECT * FROM technicians WHERE id = ?", (tecnico_id,))
    if not tech:
        return {"status": "erro", "detail": "Técnico não encontrado"}

    custo_mo = round((horas_mo or 0) * float(tech["custo_hora"] or 0), 2)
    custo_pecas = 0.0
    os_numero = row["numero_os"]

    for item in part_items:
        if item["quantidade"] <= 0:
            continue
        part = fetch_one("SELECT * FROM parts WHERE id = ?", (item["part_id"],))
        if not part:
            return {"status": "erro", "detail": "Peça inválida"}
        result = consume_part(item["part_id"], item["quantidade"], os_numero, f"Consumo na {os_numero}")
        if result["status"] != "ok":
            return result
        custo_total_item = round(item["quantidade"] * float(part["custo_unitario"]), 2)
        custo_pecas += custo_total_item
        execute(
            "INSERT INTO work_order_parts (work_order_id, part_id, quantidade, custo_unitario, custo_total, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (wo_id, item["part_id"], item["quantidade"], float(part["custo_unitario"]), custo_total_item, now_str()),
        )

    custo_total = round(custo_mo + custo_pecas, 2)
    execute(
        """
        UPDATE work_orders
        SET tecnico_id = ?, fim_reparo_em = ?, status = ?, causa_raiz = ?, acao_executada = ?,
            observacoes = ?, custo_pecas = ?, custo_mo = ?, custo_total = ?
        WHERE id = ?
        """,
        (tecnico_id, finish_time, STATUS_FINALIZADA, causa_raiz, acao_executada, observacoes, custo_pecas, custo_mo, custo_total, wo_id),
    )

    response = calculate_response_minutes(row["abertura_em"], row["inicio_reparo_em"])
    repair = calculate_repair_minutes(row["inicio_reparo_em"], finish_time, row["total_pausado_min"])
    downtime = calculate_downtime_minutes(row["abertura_em"], finish_time)

    machine = fetch_one("SELECT codigo, nome FROM machines WHERE id = ?", (row["machine_id"],))
    msg = (
        f"✅ {os_numero} finalizada | Máquina: {machine['codigo']} - {machine['nome']} | "
        f"Resposta: {response or 0:.1f} min | Reparo: {repair or 0:.1f} min | Parada: {downtime or 0:.1f} min"
    )
    send_whatsapp_message(msg, wo_id)

    return {
        "status": "ok",
        "response_min": response,
        "repair_min": repair,
        "downtime_min": downtime,
        "custo_total": custo_total,
    }


# =========================
# UI helpers
# =========================

def ensure_session() -> None:
    if "user" not in st.session_state:
        st.session_state.user = None


def login_screen() -> None:
    st.markdown(
        """
        <style>
        .big-card {background: linear-gradient(135deg, #0f172a, #1e293b); padding: 28px; border-radius: 18px; color: white; border: 1px solid #334155;}
        .metric-card {background: #111827; padding: 14px; border-radius: 14px; border: 1px solid #374151;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title(APP_TITLE)
    st.caption("Controle de OS corretiva com alerta de máquina parada, crono de manutenção, peças, custos e indicadores.")

    col1, col2 = st.columns([1.2, 1])
    with col1:
        st.markdown(
            """
            <div class='big-card'>
                <h3>Pronto para operação interna e externa</h3>
                <p>Fluxo de OS, painel do manutentor, estoque, custos, MTTR, MTBF e integração preparada para WhatsApp via Twilio.</p>
                <p><b>Usuários de teste:</b><br>operador / 1234<br>manutencao / 1234<br>gestor / 1234</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        with st.form("login_form"):
            username = st.text_input("Usuário")
            password = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar", use_container_width=True)
            if submitted:
                user = authenticate(username.strip(), password)
                if user:
                    st.session_state.user = user
                    st.rerun()
                else:
                    st.error("Usuário ou senha inválidos.")


def sidebar() -> str:
    user = st.session_state.user
    with st.sidebar:
        st.markdown(f"### 👤 {user['nome']}")
        st.caption(f"Perfil: {user['perfil']}")
        st.divider()
        menu_options = ["Dashboard", "Abrir OS", "Painel de OS", "Histórico", "Cadastros", "Estoque", "Configurações"]
        choice = st.radio("Menu", menu_options)
        st.divider()
        if st.button("Sair", use_container_width=True):
            st.session_state.user = None
            st.rerun()
    return choice


def can_manage_catalogs() -> bool:
    return st.session_state.user and st.session_state.user["perfil"] in ["Gestor", "Manutenção"]


def style_app() -> None:
    st.markdown(
        """
        <style>
        .stApp {background: linear-gradient(180deg, #0b1220 0%, #111827 100%); color: #e5e7eb;}
        [data-testid="stSidebar"] {background: #0f172a;}
        .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
        .stMetric {background: rgba(15,23,42,.65); padding: 10px; border-radius: 14px; border: 1px solid #334155;}
        .status-chip {padding: 6px 10px; border-radius: 999px; display:inline-block; font-weight:600;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_status_badge(status: str) -> str:
    color_map = {
        STATUS_ABERTA: "#dc2626",
        STATUS_ANDAMENTO: "#d97706",
        STATUS_PAUSADA: "#2563eb",
        STATUS_FINALIZADA: "#16a34a",
        STATUS_CANCELADA: "#6b7280",
    }
    color = color_map.get(status, "#6b7280")
    return f"<span class='status-chip' style='background:{color}; color:white'>{status}</span>"


# =========================
# Views
# =========================

def view_dashboard() -> None:
    st.title("🏭 Dashboard de Manutenção")
    df = get_all_work_orders()
    metrics = work_order_metrics(df)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("OS abertas", metrics["abertas"])
    col2.metric("Máquinas paradas", metrics["paradas"])
    col3.metric("Em manutenção", metrics["em_manutencao"])
    col4.metric("Finalizadas", metrics["finalizadas"])

    col5, col6, col7 = st.columns(3)
    col5.metric("MTTR (min)", metrics["mttr"])
    col6.metric("MTBF (h)", metrics["mtbf_h"])
    col7.metric("Custo total (R$)", f"{metrics['custo_total']:.2f}")

    if df.empty:
        st.info("Ainda não há ordens de serviço cadastradas.")
        return

    df_plot = df.copy()
    status_count = df_plot.groupby("status").size().reset_index(name="total")
    fig_status = px.bar(status_count, x="status", y="total", title="OS por status", text_auto=True)
    st.plotly_chart(fig_status, use_container_width=True)

    machine_rank = (
        df_plot.groupby(["maquina_codigo", "maquina_nome"]).size().reset_index(name="ocorrencias").sort_values("ocorrencias", ascending=False)
    )
    if not machine_rank.empty:
        fig_machine = px.bar(
            machine_rank.head(10),
            x="maquina_codigo",
            y="ocorrencias",
            hover_data=["maquina_nome"],
            title="Máquinas com mais ocorrências",
            text_auto=True,
        )
        st.plotly_chart(fig_machine, use_container_width=True)

    finished = df_plot[df_plot["status"] == STATUS_FINALIZADA].copy()
    if not finished.empty:
        finished["tempo_reparo_min"] = finished.apply(
            lambda r: calculate_repair_minutes(r["inicio_reparo_em"], r["fim_reparo_em"], r["total_pausado_min"]), axis=1
        )
        finished["tempo_parada_min"] = finished.apply(
            lambda r: calculate_downtime_minutes(r["abertura_em"], r["fim_reparo_em"]), axis=1
        )
        fig_times = go.Figure()
        fig_times.add_bar(x=finished["numero_os"], y=finished["tempo_reparo_min"], name="Reparo (min)")
        fig_times.add_bar(x=finished["numero_os"], y=finished["tempo_parada_min"], name="Parada (min)")
        fig_times.update_layout(title="Tempos por OS finalizada", barmode="group")
        st.plotly_chart(fig_times, use_container_width=True)

    stock_alert = fetch_df(
        "SELECT codigo, descricao, estoque_atual, estoque_minimo FROM parts WHERE ativo = 1 AND estoque_atual <= estoque_minimo ORDER BY estoque_atual ASC"
    )
    if not stock_alert.empty:
        st.warning("Peças em estoque mínimo ou abaixo do mínimo")
        st.dataframe(stock_alert, use_container_width=True, hide_index=True)


def view_open_os_form() -> None:
    st.title("📝 Abertura de Ordem de Serviço")
    machines_df = fetch_df("SELECT * FROM machines WHERE ativo = 1 ORDER BY codigo")
    if machines_df.empty:
        st.warning("Cadastre ao menos uma máquina antes de abrir uma OS.")
        return

    with st.form("open_os_form"):
        col1, col2 = st.columns(2)
        with col1:
            solicitante = st.text_input("Solicitante", value=st.session_state.user["nome"])
            setor = st.text_input("Setor")
            machine_code = st.selectbox(
                "Máquina",
                options=machines_df["codigo"].tolist(),
                format_func=lambda x: f"{x} - {machines_df.loc[machines_df['codigo'] == x, 'nome'].iloc[0]}",
            )
            criticidade = st.selectbox("Criticidade", ["Baixa", "Média", "Alta", "Crítica"], index=2)
        with col2:
            prioridade = st.selectbox("Prioridade", ["Baixa", "Média", "Alta", "Urgente"], index=2)
            parada_maquina = st.toggle("Máquina parada", value=True)
            sla_resposta_min = st.number_input("SLA de resposta (min)", min_value=5, max_value=600, value=30, step=5)
        descricao = st.text_area("Descrição do problema", placeholder="Descreva o defeito, sintoma e contexto da parada.")
        submitted = st.form_submit_button("Abrir OS", use_container_width=True)

        if submitted:
            if not solicitante.strip() or not setor.strip() or not descricao.strip():
                st.error("Preencha solicitante, setor e descrição do problema.")
                return
            machine_id = int(machines_df.loc[machines_df["codigo"] == machine_code, "id"].iloc[0])
            wo_id = create_work_order(
                solicitante=solicitante.strip(),
                setor=setor.strip(),
                machine_id=machine_id,
                descricao_problema=descricao.strip(),
                criticidade=criticidade,
                prioridade=prioridade,
                parada_maquina=parada_maquina,
                sla_resposta_min=float(sla_resposta_min),
                created_by=int(st.session_state.user["id"]),
            )
            wo = fetch_one("SELECT numero_os FROM work_orders WHERE id = ?", (wo_id,))
            st.success(f"OS {wo['numero_os']} aberta com sucesso.")
            st.info("O alerta de WhatsApp foi disparado ou registrado para envio, conforme a configuração do ambiente.")


def _display_os_card(row: pd.Series, techs_df: pd.DataFrame, parts_df: pd.DataFrame) -> None:
    response_min = calculate_response_minutes(row["abertura_em"], row["inicio_reparo_em"])
    current_end = row["fim_reparo_em"] if row["fim_reparo_em"] else now_str()
    live_repair = calculate_repair_minutes(row["inicio_reparo_em"], current_end, row["total_pausado_min"]) if row["inicio_reparo_em"] else None

    with st.expander(f"{row['numero_os']} | {row['maquina_codigo']} - {row['maquina_nome']} | {row['status']}", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(format_status_badge(row["status"]), unsafe_allow_html=True)
        col2.metric("Resposta (min)", f"{response_min:.1f}" if response_min is not None else "-")
        col3.metric("Reparo atual (min)", f"{live_repair:.1f}" if live_repair is not None else "-")
        col4.metric("Pausado (min)", f"{float(row['total_pausado_min'] or 0):.1f}")

        st.write(f"**Solicitante:** {row['solicitante']}  ")
        st.write(f"**Setor:** {row['setor']}  ")
        st.write(f"**Problema:** {row['descricao_problema']}")

        cols = st.columns(4)
        with cols[0]:
            if row["status"] in [STATUS_ABERTA, STATUS_PAUSADA] and can_manage_catalogs():
                tecnico_start = st.selectbox(
                    f"Técnico para iniciar - {row['id']}",
                    options=techs_df["id"].tolist(),
                    format_func=lambda x: techs_df.loc[techs_df["id"] == x, "nome"].iloc[0],
                    key=f"start_tech_{row['id']}",
                )
                if st.button("Iniciar", key=f"btn_start_{row['id']}", use_container_width=True):
                    start_work_order(int(row["id"]), int(tecnico_start))
                    st.rerun()
        with cols[1]:
            if row["status"] == STATUS_ANDAMENTO and can_manage_catalogs():
                if st.button("Pausar", key=f"btn_pause_{row['id']}", use_container_width=True):
                    pause_work_order(int(row["id"]))
                    st.rerun()
            elif row["status"] == STATUS_PAUSADA and can_manage_catalogs():
                if st.button("Retomar", key=f"btn_resume_{row['id']}", use_container_width=True):
                    resume_work_order(int(row["id"]))
                    st.rerun()
        with cols[2]:
            if row["status"] in [STATUS_ABERTA, STATUS_ANDAMENTO, STATUS_PAUSADA] and can_manage_catalogs():
                if st.button("Cancelar", key=f"btn_cancel_{row['id']}", use_container_width=True):
                    execute("UPDATE work_orders SET status = ? WHERE id = ?", (STATUS_CANCELADA, int(row["id"])))
                    st.rerun()

        if row["status"] in [STATUS_ANDAMENTO, STATUS_PAUSADA] and can_manage_catalogs():
            st.divider()
            st.subheader("Finalização")
            with st.form(f"finish_form_{row['id']}"):
                tecnico_id = st.selectbox(
                    "Técnico responsável",
                    options=techs_df["id"].tolist(),
                    index=0 if row["tecnico_id"] is None else max(0, techs_df.index[techs_df["id"] == row["tecnico_id"]].tolist()[0]),
                    format_func=lambda x: techs_df.loc[techs_df["id"] == x, "nome"].iloc[0],
                )
                horas_mo = st.number_input("Horas de mão de obra", min_value=0.0, max_value=48.0, value=1.0, step=0.5)
                causa_raiz = st.text_area("Causa raiz")
                acao_executada = st.text_area("Ação executada")
                observacoes = st.text_area("Observações")
                st.markdown("**Peças consumidas**")
                selected_parts = st.multiselect(
                    "Selecione as peças utilizadas",
                    options=parts_df["id"].tolist(),
                    format_func=lambda x: f"{parts_df.loc[parts_df['id'] == x, 'codigo'].iloc[0]} - {parts_df.loc[parts_df['id'] == x, 'descricao'].iloc[0]}",
                    key=f"parts_multi_{row['id']}",
                )
                part_items = []
                for pid in selected_parts:
                    part_label = parts_df.loc[parts_df["id"] == pid].iloc[0]
                    qty = st.number_input(
                        f"Qtd. {part_label['codigo']} ({part_label['estoque_atual']} em estoque)",
                        min_value=0.0,
                        max_value=float(part_label["estoque_atual"]),
                        value=1.0,
                        step=1.0,
                        key=f"qty_{row['id']}_{pid}",
                    )
                    part_items.append({"part_id": int(pid), "quantidade": float(qty)})
                submitted = st.form_submit_button("Finalizar OS", use_container_width=True)
                if submitted:
                    if not acao_executada.strip():
                        st.error("Descreva a ação executada antes de finalizar.")
                    else:
                        result = finalize_work_order(
                            int(row["id"]), int(tecnico_id), causa_raiz.strip(), acao_executada.strip(), observacoes.strip(), float(horas_mo), part_items
                        )
                        if result["status"] == "ok":
                            st.success(
                                f"OS finalizada. Resposta: {result['response_min'] or 0:.1f} min | Reparo: {result['repair_min'] or 0:.1f} min | "
                                f"Parada: {result['downtime_min'] or 0:.1f} min | Custo: R$ {result['custo_total']:.2f}"
                            )
                            st.rerun()
                        else:
                            st.error(result["detail"])


def view_os_panel() -> None:
    st.title("🛠️ Painel Operacional de OS")
    df = get_open_work_orders()
    techs_df = fetch_df("SELECT * FROM technicians WHERE ativo = 1 ORDER BY nome")
    parts_df = fetch_df("SELECT * FROM parts WHERE ativo = 1 ORDER BY codigo")

    if df.empty:
        st.success("Nenhuma OS aberta no momento.")
        return

    filtro_status = st.multiselect("Filtrar status", STATUS_OPTIONS, default=[STATUS_ABERTA, STATUS_ANDAMENTO, STATUS_PAUSADA])
    filtro_setor = st.multiselect("Filtrar setor", sorted(df["setor"].dropna().unique().tolist()), default=sorted(df["setor"].dropna().unique().tolist()))
    filtered = df[df["status"].isin(filtro_status) & df["setor"].isin(filtro_setor)]

    for _, row in filtered.iterrows():
        _display_os_card(row, techs_df, parts_df)


def view_history() -> None:
    st.title("📚 Histórico de Ordens de Serviço")
    df = get_all_work_orders()
    if df.empty:
        st.info("Sem histórico ainda.")
        return

    df = df.copy()
    df["tempo_resposta_min"] = df.apply(lambda r: calculate_response_minutes(r["abertura_em"], r["inicio_reparo_em"]), axis=1)
    df["tempo_reparo_min"] = df.apply(lambda r: calculate_repair_minutes(r["inicio_reparo_em"], r["fim_reparo_em"], r["total_pausado_min"]), axis=1)
    df["tempo_parada_min"] = df.apply(lambda r: calculate_downtime_minutes(r["abertura_em"], r["fim_reparo_em"]), axis=1)

    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.multiselect("Status", STATUS_OPTIONS, default=STATUS_OPTIONS)
    with col2:
        setor_filter = st.multiselect("Setor", sorted(df["setor"].dropna().unique().tolist()), default=sorted(df["setor"].dropna().unique().tolist()))
    with col3:
        machine_filter = st.multiselect("Máquina", sorted(df["maquina_codigo"].dropna().unique().tolist()), default=sorted(df["maquina_codigo"].dropna().unique().tolist()))

    filtered = df[df["status"].isin(status_filter) & df["setor"].isin(setor_filter) & df["maquina_codigo"].isin(machine_filter)]
    st.dataframe(filtered, use_container_width=True, hide_index=True)
    csv = filtered.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Exportar CSV", csv, file_name="historico_os.csv", mime="text/csv")

    notif_df = fetch_df("SELECT * FROM notification_log ORDER BY id DESC LIMIT 50")
    if not notif_df.empty:
        st.subheader("Log de notificações")
        st.dataframe(notif_df, use_container_width=True, hide_index=True)


def view_catalogs() -> None:
    st.title("🗂️ Cadastros")
    if not can_manage_catalogs():
        st.warning("Seu perfil não possui permissão para alterar cadastros.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["Máquinas", "Técnicos", "Peças", "Usuários"])

    with tab1:
        df = fetch_df("SELECT * FROM machines ORDER BY codigo")
        st.dataframe(df, use_container_width=True, hide_index=True)
        with st.form("machine_form"):
            col1, col2, col3 = st.columns(3)
            codigo = col1.text_input("Código")
            nome = col2.text_input("Nome")
            setor = col3.text_input("Setor")
            col4, col5, col6 = st.columns(3)
            fabricante = col4.text_input("Fabricante")
            modelo = col5.text_input("Modelo")
            criticidade = col6.number_input("Criticidade", min_value=1, max_value=5, value=3, step=1)
            if st.form_submit_button("Salvar máquina"):
                execute(
                    "INSERT INTO machines (codigo, nome, setor, fabricante, modelo, criticidade, ativo, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                    (codigo.strip(), nome.strip(), setor.strip(), fabricante.strip(), modelo.strip(), int(criticidade), now_str()),
                )
                st.success("Máquina cadastrada.")
                st.rerun()

    with tab2:
        df = fetch_df("SELECT * FROM technicians ORDER BY nome")
        st.dataframe(df, use_container_width=True, hide_index=True)
        with st.form("tech_form"):
            col1, col2, col3 = st.columns(3)
            nome = col1.text_input("Nome do técnico")
            especialidade = col2.text_input("Especialidade")
            custo_hora = col3.number_input("Custo/hora", min_value=0.0, value=80.0, step=5.0)
            if st.form_submit_button("Salvar técnico"):
                execute(
                    "INSERT INTO technicians (nome, especialidade, custo_hora, ativo, created_at) VALUES (?, ?, ?, 1, ?)",
                    (nome.strip(), especialidade.strip(), float(custo_hora), now_str()),
                )
                st.success("Técnico cadastrado.")
                st.rerun()

    with tab3:
        df = fetch_df("SELECT * FROM parts ORDER BY codigo")
        st.dataframe(df, use_container_width=True, hide_index=True)
        with st.form("part_form"):
            c1, c2, c3, c4 = st.columns(4)
            codigo = c1.text_input("Código da peça")
            descricao = c2.text_input("Descrição")
            unidade = c3.text_input("Unidade", value="UN")
            estoque = c4.number_input("Estoque inicial", min_value=0.0, value=0.0, step=1.0)
            c5, c6 = st.columns(2)
            estoque_minimo = c5.number_input("Estoque mínimo", min_value=0.0, value=0.0, step=1.0)
            custo_unitario = c6.number_input("Custo unitário", min_value=0.0, value=0.0, step=0.5)
            if st.form_submit_button("Salvar peça"):
                execute(
                    "INSERT INTO parts (codigo, descricao, unidade, estoque_atual, estoque_minimo, custo_unitario, ativo, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                    (codigo.strip(), descricao.strip(), unidade.strip(), float(estoque), float(estoque_minimo), float(custo_unitario), now_str()),
                )
                st.success("Peça cadastrada.")
                st.rerun()

    with tab4:
        df = fetch_df("SELECT id, username, nome, perfil, ativo, created_at FROM users ORDER BY username")
        st.dataframe(df, use_container_width=True, hide_index=True)
        with st.form("user_form"):
            c1, c2, c3, c4 = st.columns(4)
            username = c1.text_input("Usuário")
            nome = c2.text_input("Nome")
            perfil = c3.selectbox("Perfil", PERFIS)
            senha = c4.text_input("Senha", type="password")
            if st.form_submit_button("Salvar usuário"):
                execute(
                    "INSERT INTO users (username, password_hash, nome, perfil, ativo, created_at) VALUES (?, ?, ?, ?, 1, ?)",
                    (username.strip(), hash_password(senha), nome.strip(), perfil, now_str()),
                )
                st.success("Usuário cadastrado.")
                st.rerun()


def view_stock() -> None:
    st.title("📦 Estoque e Movimentações")
    parts_df = fetch_df("SELECT * FROM parts ORDER BY codigo")
    st.dataframe(parts_df, use_container_width=True, hide_index=True)

    if can_manage_catalogs() and not parts_df.empty:
        with st.form("stock_input_form"):
            part_id = st.selectbox(
                "Peça",
                options=parts_df["id"].tolist(),
                format_func=lambda x: f"{parts_df.loc[parts_df['id'] == x, 'codigo'].iloc[0]} - {parts_df.loc[parts_df['id'] == x, 'descricao'].iloc[0]}",
            )
            quantidade = st.number_input("Quantidade de entrada", min_value=0.0, value=1.0, step=1.0)
            observacao = st.text_input("Observação", value="Reposição manual")
            if st.form_submit_button("Registrar entrada"):
                part = fetch_one("SELECT estoque_atual FROM parts WHERE id = ?", (int(part_id),))
                novo = float(part["estoque_atual"]) + float(quantidade)
                execute("UPDATE parts SET estoque_atual = ? WHERE id = ?", (novo, int(part_id)))
                execute(
                    "INSERT INTO stock_movements (part_id, tipo, quantidade, referencia, observacao, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (int(part_id), "ENTRADA", float(quantidade), "AJUSTE", observacao.strip(), now_str()),
                )
                st.success("Entrada registrada.")
                st.rerun()

    mov_df = fetch_df(
        """
        SELECT sm.id, p.codigo, p.descricao, sm.tipo, sm.quantidade, sm.referencia, sm.observacao, sm.created_at
        FROM stock_movements sm
        JOIN parts p ON p.id = sm.part_id
        ORDER BY sm.id DESC
        LIMIT 200
        """
    )
    if not mov_df.empty:
        st.subheader("Movimentações")
        st.dataframe(mov_df, use_container_width=True, hide_index=True)


def view_settings() -> None:
    st.title("⚙️ Configurações e Implantação")
    st.subheader("Integração de WhatsApp")
    st.code(
        """
TWILIO_ACCOUNT_SID=seu_sid
TWILIO_AUTH_TOKEN=seu_token
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
TWILIO_WHATSAPP_TO=whatsapp:+55SEUNUMERO
        """.strip(),
        language="bash",
    )
    st.info("Sem essas variáveis, o sistema registra o alerta como pendente no log, sem bloquear a operação.")

    st.subheader("Acesso interno e externo")
    st.markdown(
        """
- **Interno:** rode em um PC/servidor local com `streamlit run app.py --server.port 8501`.
- **Externo:** publique em um VPS, VM ou Streamlit Community Cloud/Render. Para produção, use proxy reverso e HTTPS.
- **Banco atual:** SQLite. Para muitos usuários simultâneos, migrar depois para PostgreSQL.
        """
    )

    st.subheader("Recomendações para produção")
    st.markdown(
        """
- Criar backup diário do arquivo `manutencao_industrial.db`.
- Restringir acesso externo com autenticação forte.
- Trocar senhas padrão imediatamente.
- Integrar o cadastro de máquinas com sua base real.
- Migrar para PostgreSQL quando houver alto volume e acesso concorrente.
        """
    )


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="🏭", layout="wide")
    style_app()
    init_db()
    ensure_session()

    if not st.session_state.user:
        login_screen()
        return

    choice = sidebar()

    if choice == "Dashboard":
        view_dashboard()
    elif choice == "Abrir OS":
        view_open_os_form()
    elif choice == "Painel de OS":
        view_os_panel()
    elif choice == "Histórico":
        view_history()
    elif choice == "Cadastros":
        view_catalogs()
    elif choice == "Estoque":
        view_stock()
    elif choice == "Configurações":
        view_settings()


if __name__ == "__main__":
    main()

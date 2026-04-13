# Manutenção V8 Fábrica

Novidades da V8:
- cadastro restrito somente ao Gestor da Manutenção
- pronto para PostgreSQL com fallback para SQLite
- senha criptografada com SHA-256
- troca de senha
- logo da empresa
- tela TV industrial
- alertas por WhatsApp
- escalonamento automático

## Rodar localmente
pip install -r requirements.txt
streamlit run app.py

## Usuários iniciais
operador / 1234
manutencao / 1234
gestor / 1234

## Banco
Por padrão usa SQLite.
Para PostgreSQL, configure nos Secrets:
DB_MODE = "postgres"
POSTGRES_URL = "postgresql://usuario:senha@host:5432/banco"

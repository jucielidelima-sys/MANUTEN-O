# Manutenção V10 Fábrica

Novidades da V10:
- gestão completa de máquinas
- editar, desativar, reativar e excluir máquinas com segurança
- gestão completa de técnicos
- editar, desativar, reativar e excluir técnicos com segurança
- máquinas e técnicos inativos somem dos formulários operacionais
- cadastros restritos ao Gestor
- PostgreSQL pronto com fallback para SQLite
- senha criptografada, troca de senha, logo da empresa
- WhatsApp, escalonamento e tela TV

## Rodar localmente
pip install -r requirements.txt
streamlit run app.py

## Usuários iniciais
operador / 1234
manutencao / 1234
gestor / 1234

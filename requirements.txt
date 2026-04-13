# Manutenção V12 Industrial

Novidades da V12:
- mantém compatibilidade com o banco da V10
- usa `manutencao_v10_fabrica.db` por padrão para preservar máquinas e técnicos
- preventiva com geração automática de OS preventiva
- tipo de manutenção: corretiva e preventiva
- painel Tesla com velocímetros e termômetro
- painel de OS para Gestor e Manutenção
- Preventiva, Segurança e Configurações só para Gestor

## Importante sobre manter os cadastros da V10
Para manter máquinas e técnicos já cadastrados:
- use esta V12 no mesmo ambiente onde está o arquivo `manutencao_v10_fabrica.db`
- ou copie esse arquivo para a pasta do app antes de rodar

## Rodar localmente
pip install -r requirements.txt
streamlit run app.py

## Usuários iniciais
operador / 1234
manutencao / 1234
gestor / 1234

# Sistema de Manutenção Industrial V3

Aplicação em Streamlit para controle de ordens de serviço corretivas com:

- login por perfil
- abertura de OS com alerta de máquina parada
- painel de atendimento com iniciar, pausar, retomar e finalizar
- cálculo de tempo de resposta, tempo de reparo e tempo total de parada
- cadastro de máquinas, técnicos, usuários e peças
- baixa automática de peças no encerramento da OS
- custo de mão de obra e peças
- dashboard com MTTR, MTBF estimado e ranking de ocorrências
- integração preparada para WhatsApp via Twilio

## Como rodar

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Usuários iniciais

- operador / 1234
- manutencao / 1234
- gestor / 1234

## Variáveis de ambiente para WhatsApp

```bash
TWILIO_ACCOUNT_SID=seu_sid
TWILIO_AUTH_TOKEN=seu_token
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
TWILIO_WHATSAPP_TO=whatsapp:+55SEUNUMERO
```

## Observações

- O banco local é `manutencao_industrial.db`.
- Sem Twilio configurado, o sistema registra o envio como pendente no log.
- Para produção com muitos acessos simultâneos, migrar para PostgreSQL.

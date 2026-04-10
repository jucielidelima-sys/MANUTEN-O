# Manutenção V5 Premium com WhatsApp

Versão do app já integrada com envio de alertas por WhatsApp via Twilio.

## Como rodar
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Usuários iniciais
- operador / 1234
- manutencao / 1234
- gestor / 1234

## Como configurar o WhatsApp no Streamlit Cloud
Crie o arquivo `.streamlit/secrets.toml` com:

```toml
TWILIO_ACCOUNT_SID = "seu_account_sid"
TWILIO_AUTH_TOKEN = "seu_auth_token"
TWILIO_WHATSAPP_FROM = "whatsapp:+14155238886"
TWILIO_WHATSAPP_TO = "whatsapp:+5511999999999,whatsapp:+5549999999999"
```

## Fluxo do alerta
Quando a OS for aberta com status `Máquina Parada`, o app tenta enviar o WhatsApp automaticamente.

## Observação
No sandbox do Twilio, os números de destino precisam ter entrado no sandbox antes.

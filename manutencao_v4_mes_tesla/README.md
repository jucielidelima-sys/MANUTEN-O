# MES Maintenance V4 - Visual MES/Tesla

Aplicação em Streamlit para controle de manutenção industrial com foco em:

- abertura e acompanhamento de OS
- alerta de máquina parada
- início, pausa, retomada e finalização de reparo
- controle de peças com baixa automática de estoque
- cálculo de tempo de resposta, reparo e parada
- custos de mão de obra e peças
- dashboard com MTTR, MTBF estimado, ranking de ocorrências e histórico
- visual escuro estilo sala de controle / MES

## Como executar

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Usuários iniciais

- operador / 1234
- manutencao / 1234
- gestor / 1234

## Banco de dados

O sistema cria automaticamente o arquivo `manutencao_v4.db` na primeira execução.

## WhatsApp

A integração real com WhatsApp está preparada para Twilio.
Use as variáveis de ambiente descritas na tela "Configuração WhatsApp".

## Observação

Para múltiplos acessos simultâneos em ambiente externo, o próximo passo recomendado é migrar o banco para PostgreSQL.

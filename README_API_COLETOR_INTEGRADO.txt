SISTEMA CH CONTESTADO - COMPLETO COM API DO COLETOR

Incluído:
- Endpoint POST /api/coletor/xml
- Recebe XML enviado pelo coletor local
- Identifica cliente por CNPJ
- Classifica EMITIDA/RECEBIDA
- Ignora XML duplicado por chave
- Mantém painel Storage
- Mantém geração de ZIP/e-mail

No Railway, configure:
COLETOR_API_TOKEN=um_token_grande_e_seguro

O mesmo token deve ser colocado no config.json do coletor.

VERSÃO PRONTA PARA RAILWAY + SQLITE PERSISTENTE

Configuração do volume no Railway:
- Attach volume ao serviço
- Mount path: /data

O sistema agora salva automaticamente em:
- Banco: /data/database.db
- Uploads: /data/uploads
- Configuração de e-mail: /data/email_config.json

Localmente:
- Se /data não existir, continua usando a pasta do projeto normalmente.

Arquivos importantes para deploy:
- Procfile: web: gunicorn app:app
- requirements.txt com gunicorn

Depois de subir no GitHub:
1. Commit changes
2. Railway faz deploy automático
3. Teste login, cadastro, upload e download

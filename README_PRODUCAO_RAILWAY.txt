SISTEMA CH CONTESTADO - VERSÃO LIMPA PROFISSIONAL

Esta versão mantém os dados existentes no Railway porque usa:
- Banco: /data/database.db
- Uploads: /data/uploads
- Configuração de e-mail: /data/email_config.json

IMPORTANTE:
No Railway, mantenha o volume anexado com mount path:
/data

E configure as Variables:
RESEND_API_KEY=sua_chave_resend
RESEND_FROM=CH Contestado <onboarding@resend.dev>

Para produção com domínio validado no Resend:
RESEND_FROM=CH Contestado <noreply@chcontestato.com>

Como atualizar sem perder dados:
1. Não apague o volume do Railway.
2. Suba estes arquivos no GitHub.
3. Aguarde o redeploy.
4. Acesse /status-data logado como admin para confirmar DATA_DIR=/data.
5. Faça cadastro teste e redeploy para validar persistência.

Backups:
- /backup/database baixa apenas o database.db.
- /backup/completo baixa banco + uploads em ZIP.

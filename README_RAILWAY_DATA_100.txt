VERSÃO AJUSTADA PARA SALVAR 100% NO VOLUME /data

Antes de subir:
1. No Railway, mantenha o volume anexado ao serviço.
2. Mount path precisa estar como:
/data

Onde os dados serão salvos:
- Banco SQLite: /data/database.db
- Uploads: /data/uploads
- Documentos: /data/uploads/documentos
- Certificados: /data/uploads/certificados
- Zips: /data/uploads/zips
- Configuração de e-mail: /data/email_config.json

Teste obrigatório:
1. Suba essa versão no GitHub.
2. Cadastre 1 cliente de teste.
3. Faça um redeploy no Railway.
4. Confira se o cliente continuou.
Se continuar, está persistente corretamente.

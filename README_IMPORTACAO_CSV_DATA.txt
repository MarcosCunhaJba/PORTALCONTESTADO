VERSÃO COM /data + IMPORTAÇÃO DE CLIENTES CSV

Incluído:
- Salvamento persistente em /data no Railway.
- Importar clientes por CSV.
- Baixar modelo de CSV.
- Rota /status-data para confirmar se está usando /data.

Teste:
1. Suba no GitHub.
2. Acesse /status-data logado como admin.
3. Confirme DATA_DIR=/data e db_existe=true.
4. Importe um CSV de teste.
5. Faça redeploy e veja se permaneceu.

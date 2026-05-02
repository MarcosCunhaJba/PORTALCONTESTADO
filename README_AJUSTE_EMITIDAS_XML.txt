AJUSTE EMITIDAS XML

Removido o filtro que descartava XML quando o destinatário era diferente do cliente.
Agora o sistema salva/classifica:
- emit_cnpj == CNPJ cliente => NFs emitidas pela empresa
- dest_cnpj == CNPJ cliente => NFs recebidas pela empresa

Observação: a SEFAZ pode não retornar 100% das emitidas, mas esta versão não descarta mais as emitidas que vierem.

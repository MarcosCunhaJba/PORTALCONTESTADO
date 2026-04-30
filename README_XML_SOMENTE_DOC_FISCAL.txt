CORREÇÃO XML:
- Não salva mais eventos: ciência da operação, manifestação, cancelamento/eventos etc.
- Salva somente XML de documento fiscal: NFE, NFCE, CTE, MDFE/OUTROS.
- Separa corretamente:
  - NFs emitidas pela empresa = emitente é o CNPJ do cliente.
  - NFs recebidas pela empresa = destinatário é o CNPJ do cliente ou resumo de documento de terceiro.
- Mantém abas emitidas/recebidas.
- ZIP organiza por EMITIDA/RECEBIDA e tipo fiscal.
IMPORTANTE: XMLs/eventos já salvos anteriormente continuam no banco. Para limpar os antigos, use limpeza manual ou peça a rotina de limpeza.

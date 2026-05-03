CORREÇÕES:
- Corrigido crash: NameError app is not defined causado por @app.context_processor antes do app = Flask.
- Mantida tela de upload de logo/aparência.
- Envio/ZIP da contabilidade agora inclui documentos + XMLs do mês dos clientes vinculados.
- SQL do ZIP usa aliases explícitos para evitar ambiguous column name.

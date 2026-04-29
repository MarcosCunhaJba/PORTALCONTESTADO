AJUSTE XML / DF-e MANUAL

Incluído:
- Botão "XMLs" na tela de clientes.
- Tela para buscar XMLs via Distribuição DF-e usando o certificado A1 do cliente.
- Salva XMLs em /data/uploads/xmls/<cliente_id>/.
- Lista e permite baixar XMLs encontrados.

Requisitos:
- Cliente precisa ter CNPJ válido.
- Cliente precisa ter certificado A1 .pfx cadastrado.
- Cliente precisa ter senha do certificado cadastrada.
- Railway precisa instalar cryptography via requirements.txt.

Observação:
- É consulta manual, 1 clique por vez.
- O serviço retorna DF-e de interesse do CNPJ conforme regras SEFAZ.

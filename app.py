import os
import csv
import io
import json
import uuid
import zipfile
import sqlite3
import requests
from datetime import datetime
from functools import wraps
from flask import (
    Flask, render_template, request, redirect, session, url_for,
    send_from_directory, flash, Response, jsonify
)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# =========================================================
# CONFIGURAÇÃO PRINCIPAL
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Railway: se existir volume /data, salva tudo lá.
# Local: se /data não existir, salva na pasta do projeto.
DATA_DIR = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") else BASE_DIR)

DB_PATH = os.path.join(DATA_DIR, "database.db")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
DOC_DIR = os.path.join(UPLOAD_DIR, "documentos")
CERT_DIR = os.path.join(UPLOAD_DIR, "certificados")
ZIP_DIR = os.path.join(UPLOAD_DIR, "zips")

for pasta in (DATA_DIR, UPLOAD_DIR, DOC_DIR, CERT_DIR, ZIP_DIR):
    os.makedirs(pasta, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "ch_contestado_secret_key")

MESES = [
    ("01", "Janeiro"), ("02", "Fevereiro"), ("03", "Março"),
    ("04", "Abril"), ("05", "Maio"), ("06", "Junho"),
    ("07", "Julho"), ("08", "Agosto"), ("09", "Setembro"),
    ("10", "Outubro"), ("11", "Novembro"), ("12", "Dezembro"),
]
ANOS = list(range(2026, 2036))


# =========================================================
# BANCO DE DADOS
# =========================================================

def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def column_exists(cur, table, column):
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def ensure_column(cur, table, column, definition):
    if not column_exists(cur, table, column):
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db():
    con = db()
    cur = con.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        email TEXT UNIQUE,
        senha_hash TEXT,
        tipo TEXT,
        contabilidade_id INTEGER,
        criado_em TEXT
    );

    CREATE TABLE IF NOT EXISTS contabilidades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        cnpj TEXT,
        telefone TEXT,
        email TEXT,
        ativo INTEGER DEFAULT 1,
        criado_em TEXT
    );

    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        razao TEXT NOT NULL,
        cnpj TEXT,
        contabilidade_id INTEGER,
        ano_certificado INTEGER,
        senha_certificado TEXT,
        arquivo_certificado TEXT,
        criado_em TEXT,
        ativo INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS documentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER,
        mes TEXT,
        ano INTEGER,
        descricao TEXT,
        nome_original TEXT,
        arquivo TEXT,
        token TEXT UNIQUE,
        enviado_em TEXT
    );

    CREATE TABLE IF NOT EXISTS envios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contabilidade_id INTEGER,
        mes TEXT,
        ano INTEGER,
        arquivo_zip TEXT,
        token TEXT UNIQUE,
        email_destino TEXT,
        enviado_email INTEGER DEFAULT 0,
        criado_em TEXT
    );

    CREATE TABLE IF NOT EXISTS interesses_ch (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        usuario_nome TEXT,
        usuario_email TEXT,
        contabilidade_id INTEGER,
        contabilidade_nome TEXT,
        criado_em TEXT,
        ip TEXT,
        email_enviado INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS export_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        senha TEXT NOT NULL,
        criado_em TEXT,
        usado INTEGER DEFAULT 0
    );
    """)

    # Autorreparo de colunas para bancos antigos
    ensure_column(cur, "contabilidades", "ativo", "INTEGER DEFAULT 1")
    ensure_column(cur, "clientes", "ativo", "INTEGER DEFAULT 1")
    ensure_column(cur, "documentos", "descricao", "TEXT")
    ensure_column(cur, "users", "criado_em", "TEXT")

    # Índices para performance
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clientes_contabilidade ON clientes(contabilidade_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clientes_cnpj ON clientes(cnpj)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_docs_cliente_mes_ano ON documentos(cliente_id, mes, ano)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")

    # Admin inicial
    admin = cur.execute("SELECT id FROM users WHERE email=?", ("admin@admin.com",)).fetchone()
    if not admin:
        cur.execute("""
            INSERT INTO users (nome, email, senha_hash, tipo, contabilidade_id, criado_em)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            "Administrador",
            "admin@admin.com",
            generate_password_hash("admin123"),
            "admin",
            None,
            datetime.now().isoformat()
        ))

    con.commit()
    con.close()


init_db()


# =========================================================
# HELPERS
# =========================================================

def nome_mes(codigo):
    for cod, nome in MESES:
        if str(cod) == str(codigo):
            return nome
    return str(codigo)


def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    con = db()
    user = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    con.close()
    return user


@app.context_processor
def inject_global():
    return {
        "MESES": MESES,
        "ANOS": ANOS,
        "nome_mes": nome_mes,
        "user": current_user()
    }


def login_required(tipo=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not session.get("user_id"):
                return redirect(url_for("login"))
            if tipo and session.get("tipo") != tipo:
                return redirect(url_for("dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def enviar_email_resend(destino, assunto, corpo):
    """Envio via Resend API. Funciona no Railway, diferente de SMTP."""
    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    remetente = os.environ.get("RESEND_FROM", "CH Contestado <onboarding@resend.dev>").strip()

    if not api_key:
        return False, "RESEND_API_KEY não configurada no Railway."

    if not destino:
        return False, "E-mail de destino não informado."

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "from": remetente,
                "to": [destino],
                "subject": assunto,
                "text": corpo
            },
            timeout=15
        )

        if resp.status_code in (200, 202):
            return True, "E-mail enviado com sucesso."

        return False, f"Erro Resend {resp.status_code}: {resp.text[:500]}"

    except Exception as e:
        return False, f"Falha Resend: {str(e)}"


def enviar_email_link(destino, assunto, corpo):
    return enviar_email_resend(destino, assunto, corpo)


def enviar_email_simples(destino, assunto, corpo):
    return enviar_email_resend(destino, assunto, corpo)


# =========================================================
# LOGIN
# =========================================================

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        senha = request.form.get("senha", "")

        con = db()
        user = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        con.close()

        if user and check_password_hash(user["senha_hash"], senha):
            session["user_id"] = user["id"]
            session["nome"] = user["nome"]
            session["email"] = user["email"]
            session["tipo"] = user["tipo"]
            session["contabilidade_id"] = user["contabilidade_id"]

            if user["tipo"] == "admin":
                return redirect(url_for("dashboard_admin"))
            return redirect(url_for("area_contabilidade"))

        flash("E-mail ou senha inválidos.")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required()
def dashboard():
    if session.get("tipo") == "admin":
        return redirect(url_for("dashboard_admin"))
    return redirect(url_for("area_contabilidade"))


# =========================================================
# ADMIN DASHBOARD
# =========================================================

@app.route("/admin")
@login_required("admin")
def dashboard_admin():
    mes = request.args.get("mes", datetime.now().strftime("%m"))
    ano = int(request.args.get("ano", datetime.now().year if datetime.now().year >= 2026 else 2026))
    status = request.args.get("status", "todos")
    q = request.args.get("q", "").strip()

    con = db()
    clientes = con.execute("""
        SELECT c.*, co.nome AS contabilidade_nome
        FROM clientes c
        LEFT JOIN contabilidades co ON co.id = c.contabilidade_id
        WHERE COALESCE(c.ativo,1)=1
        ORDER BY c.razao
    """).fetchall()

    linhas = []
    enviados = 0
    pendentes = 0

    for c in clientes:
        doc = con.execute("""
            SELECT * FROM documentos
            WHERE cliente_id=? AND mes=? AND ano=?
            ORDER BY id DESC LIMIT 1
        """, (c["id"], mes, ano)).fetchone()

        tem_doc = doc is not None
        if tem_doc:
            enviados += 1
        else:
            pendentes += 1

        if status == "enviado" and not tem_doc:
            continue
        if status == "pendente" and tem_doc:
            continue
        if q and q.lower() not in (c["razao"] or "").lower() and q not in (c["cnpj"] or ""):
            continue

        linhas.append({"cliente": c, "doc": doc, "tem_doc": tem_doc})

    con.close()

    return render_template(
        "dashboard.html",
        mes=mes,
        ano=ano,
        status=status,
        q=q,
        linhas=linhas,
        total_clientes=len(clientes),
        enviados=enviados,
        pendentes=pendentes
    )


# =========================================================
# CONTABILIDADES
# =========================================================

@app.route("/contabilidades", methods=["GET", "POST"])
@login_required("admin")
def contabilidades():
    con = db()

    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        cnpj = request.form.get("cnpj", "").strip()
        email = request.form.get("email", "").strip().lower()
        telefone = request.form.get("telefone", "").strip()
        senha = request.form.get("senha", "123456").strip() or "123456"

        if nome and email:
            cur = con.cursor()
            cur.execute("""
                INSERT INTO contabilidades (nome, cnpj, telefone, email, ativo, criado_em)
                VALUES (?, ?, ?, ?, 1, ?)
            """, (nome, cnpj, telefone, email, datetime.now().isoformat()))
            contabilidade_id = cur.lastrowid

            existente = con.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
            if not existente:
                cur.execute("""
                    INSERT INTO users (nome, email, senha_hash, tipo, contabilidade_id, criado_em)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (nome, email, generate_password_hash(senha), "contabilidade", contabilidade_id, datetime.now().isoformat()))

            con.commit()
            flash("Contabilidade cadastrada.")

    lista = con.execute("""
        SELECT * FROM contabilidades
        WHERE COALESCE(ativo,1)=1
        ORDER BY nome
    """).fetchall()
    con.close()

    return render_template("contabilidades.html", contabilidades=lista)


@app.route("/contabilidades/excluir/<int:id>")
@login_required("admin")
def excluir_contabilidade(id):
    con = db()
    con.execute("UPDATE contabilidades SET ativo=0 WHERE id=?", (id,))
    con.execute("UPDATE clientes SET contabilidade_id=NULL WHERE contabilidade_id=?", (id,))
    con.commit()
    con.close()
    flash("Contabilidade excluída.")
    return redirect(url_for("contabilidades"))


# =========================================================
# CLIENTES
# =========================================================

@app.route("/clientes", methods=["GET", "POST"])
@login_required("admin")
def clientes():
    con = db()

    if request.method == "POST":
        razao = request.form.get("razao", "").strip()
        cnpj = request.form.get("cnpj", "").strip()
        contabilidade_id = request.form.get("contabilidade_id") or None
        ano_certificado = request.form.get("ano_certificado") or None
        senha_certificado = request.form.get("senha_certificado", "").strip()

        arquivo_certificado = None
        f = request.files.get("certificado")
        if f and f.filename:
            arquivo_certificado = f"{uuid.uuid4().hex}_{secure_filename(f.filename)}"
            f.save(os.path.join(CERT_DIR, arquivo_certificado))

        if razao:
            con.execute("""
                INSERT INTO clientes
                (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, arquivo_certificado, criado_em, ativo)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                razao, cnpj, contabilidade_id, ano_certificado,
                senha_certificado, arquivo_certificado, datetime.now().isoformat()
            ))
            con.commit()
            flash("Cliente cadastrado.")

    filtro = request.args.get("filtro", "todos")
    cnpj_busca = request.args.get("cnpj", "").strip()

    sql = """
        SELECT c.*, co.nome AS contabilidade_nome
        FROM clientes c
        LEFT JOIN contabilidades co ON co.id = c.contabilidade_id
        WHERE COALESCE(c.ativo,1)=1
    """
    params = []

    if filtro == "sem_contabilidade":
        sql += " AND (c.contabilidade_id IS NULL OR c.contabilidade_id='')"

    if cnpj_busca:
        sql += " AND c.cnpj LIKE ?"
        params.append(f"%{cnpj_busca}%")

    sql += " ORDER BY COALESCE(c.ano_certificado, 9999), c.razao"

    itens = con.execute(sql, params).fetchall()
    contabs = con.execute("SELECT * FROM contabilidades WHERE COALESCE(ativo,1)=1 ORDER BY nome").fetchall()
    con.close()

    return render_template(
        "clientes.html",
        itens=itens,
        contabs=contabs,
        anos=ANOS,
        filtro=filtro,
        cnpj=cnpj_busca
    )


@app.route("/clientes/editar/<int:id>", methods=["GET", "POST"])
@login_required("admin")
def editar_cliente(id):
    con = db()
    cliente = con.execute("SELECT * FROM clientes WHERE id=?", (id,)).fetchone()

    if not cliente:
        con.close()
        flash("Cliente não encontrado.")
        return redirect(url_for("clientes"))

    if request.method == "POST":
        razao = request.form.get("razao", "").strip()
        cnpj = request.form.get("cnpj", "").strip()
        contabilidade_id = request.form.get("contabilidade_id") or None
        ano_certificado = request.form.get("ano_certificado") or None
        senha_certificado = request.form.get("senha_certificado", "").strip()
        arquivo_certificado = cliente["arquivo_certificado"]

        f = request.files.get("certificado")
        if f and f.filename:
            arquivo_certificado = f"{uuid.uuid4().hex}_{secure_filename(f.filename)}"
            f.save(os.path.join(CERT_DIR, arquivo_certificado))

        con.execute("""
            UPDATE clientes
            SET razao=?, cnpj=?, contabilidade_id=?, ano_certificado=?,
                senha_certificado=?, arquivo_certificado=?
            WHERE id=?
        """, (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, arquivo_certificado, id))
        con.commit()
        con.close()
        flash("Cliente atualizado.")
        return redirect(url_for("clientes"))

    contabs = con.execute("SELECT * FROM contabilidades WHERE COALESCE(ativo,1)=1 ORDER BY nome").fetchall()
    con.close()
    return render_template("editar_cliente.html", cliente=cliente, contabs=contabs, anos=ANOS)


@app.route("/clientes/excluir/<int:id>")
@login_required("admin")
def excluir_cliente(id):
    con = db()
    con.execute("UPDATE clientes SET ativo=0 WHERE id=?", (id,))
    con.commit()
    con.close()
    flash("Cliente excluído.")
    return redirect(url_for("clientes"))


@app.route("/clientes/importar-csv", methods=["GET", "POST"])
@login_required("admin")
def importar_clientes_csv():
    con = db()
    contabs = con.execute("SELECT * FROM contabilidades WHERE COALESCE(ativo,1)=1 ORDER BY nome").fetchall()

    if request.method == "POST":
        arquivo = request.files.get("arquivo_csv")
        contabilidade_padrao = request.form.get("contabilidade_id") or None

        if not arquivo or not arquivo.filename:
            flash("Selecione um arquivo CSV.")
            con.close()
            return redirect(url_for("importar_clientes_csv"))

        try:
            conteudo = arquivo.read().decode("utf-8-sig")
        except UnicodeDecodeError:
            arquivo.stream.seek(0)
            conteudo = arquivo.read().decode("latin-1")

        linhas = conteudo.splitlines()
        if not linhas:
            flash("CSV vazio.")
            con.close()
            return redirect(url_for("importar_clientes_csv"))

        delimitador = ";" if linhas[0].count(";") >= linhas[0].count(",") else ","
        leitor = csv.DictReader(io.StringIO(conteudo), delimiter=delimitador)

        inseridos = atualizados = ignorados = 0

        for linha in leitor:
            row = {str(k).strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in linha.items() if k}
            razao = row.get("razao") or row.get("razão") or row.get("cliente") or row.get("nome") or row.get("razao social") or row.get("razão social")
            cnpj = row.get("cnpj") or ""
            contabilidade_id = row.get("contabilidade_id") or contabilidade_padrao
            ano_certificado = row.get("ano_certificado") or row.get("ano certificado") or None
            senha_certificado = row.get("senha_certificado") or row.get("senha certificado") or ""

            if not razao:
                ignorados += 1
                continue

            existente = con.execute("SELECT id FROM clientes WHERE cnpj=? AND cnpj<>''", (cnpj,)).fetchone() if cnpj else None

            if existente:
                con.execute("""
                    UPDATE clientes
                    SET razao=?, contabilidade_id=COALESCE(?, contabilidade_id),
                        ano_certificado=COALESCE(?, ano_certificado),
                        senha_certificado=COALESCE(?, senha_certificado),
                        ativo=1
                    WHERE id=?
                """, (razao, contabilidade_id, ano_certificado, senha_certificado, existente["id"]))
                atualizados += 1
            else:
                con.execute("""
                    INSERT INTO clientes
                    (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, criado_em, ativo)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                """, (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, datetime.now().isoformat()))
                inseridos += 1

        con.commit()
        con.close()
        flash(f"Importação concluída: {inseridos} inseridos, {atualizados} atualizados, {ignorados} ignorados.")
        return redirect(url_for("clientes"))

    con.close()
    return render_template("importar_clientes_csv.html", contabs=contabs)


@app.route("/clientes/modelo-csv")
@login_required("admin")
def modelo_clientes_csv():
    conteudo = "razao;cnpj;contabilidade_id;ano_certificado;senha_certificado\nEmpresa Exemplo LTDA;00.000.000/0001-00;;2026;senha123\n"
    return Response(
        conteudo,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=modelo_importacao_clientes.csv"}
    )


@app.route("/clientes/exportar-csv")
@login_required("admin")
def exportar_clientes_csv():
    con = db()
    rows = con.execute("""
        SELECT c.id, c.razao, c.cnpj, COALESCE(co.nome,'') AS contabilidade,
               COALESCE(c.ano_certificado,'') AS ano_certificado,
               COALESCE(c.senha_certificado,'') AS senha_certificado,
               COALESCE(c.arquivo_certificado,'') AS arquivo_certificado,
               COALESCE(c.criado_em,'') AS criado_em
        FROM clientes c
        LEFT JOIN contabilidades co ON co.id = c.contabilidade_id
        WHERE COALESCE(c.ativo,1)=1
        ORDER BY c.razao
    """).fetchall()
    con.close()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["ID", "Cliente", "CNPJ", "Contabilidade", "Ano Certificado", "Senha Certificado", "Arquivo Certificado", "Criado em"])

    for r in rows:
        writer.writerow([r["id"], r["razao"], r["cnpj"], r["contabilidade"], r["ano_certificado"], r["senha_certificado"], r["arquivo_certificado"], r["criado_em"]])

    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=clientes_ch_contestado.csv"}
    )


# =========================================================
# DOCUMENTOS
# =========================================================

@app.route("/upload/<int:cliente_id>", methods=["GET", "POST"])
@login_required("admin")
def upload(cliente_id):
    mes = request.args.get("mes") or request.form.get("mes") or datetime.now().strftime("%m")
    ano = int(request.args.get("ano") or request.form.get("ano") or 2026)

    con = db()
    cliente = con.execute("SELECT * FROM clientes WHERE id=?", (cliente_id,)).fetchone()

    if not cliente:
        con.close()
        flash("Cliente não encontrado.")
        return redirect(url_for("dashboard_admin"))

    if request.method == "POST":
        f = request.files.get("arquivo")
        descricao = request.form.get("descricao", "").strip()

        if f and f.filename:
            nome_original = f.filename
            arquivo_salvo = f"{uuid.uuid4().hex}_{secure_filename(nome_original)}"
            f.save(os.path.join(DOC_DIR, arquivo_salvo))
            token = uuid.uuid4().hex

            con.execute("""
                INSERT INTO documentos (cliente_id, mes, ano, descricao, nome_original, arquivo, token, enviado_em)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (cliente_id, mes, ano, descricao, nome_original, arquivo_salvo, token, datetime.now().strftime("%d/%m/%Y %H:%M")))
            con.commit()
            flash("Documento enviado.")

        con.close()
        return redirect(url_for("dashboard_admin", mes=mes, ano=ano))

    con.close()
    return render_template("upload.html", cliente=cliente, mes=mes, ano=ano)


@app.route("/download/<arquivo>")
@login_required()
def download(arquivo):
    # download de documento ou certificado por nome
    for pasta in (DOC_DIR, CERT_DIR, ZIP_DIR):
        caminho = os.path.join(pasta, arquivo)
        if os.path.exists(caminho):
            return send_from_directory(pasta, arquivo, as_attachment=True)
    flash("Arquivo não encontrado.")
    return redirect(url_for("dashboard"))


@app.route("/certificado/<arquivo>")
@login_required("admin")
def certificado(arquivo):
    return send_from_directory(CERT_DIR, arquivo, as_attachment=True)


# =========================================================
# ÁREA DA CONTABILIDADE
# =========================================================

@app.route("/contabilidade")
@login_required("contabilidade")
def area_contabilidade():
    mes = request.args.get("mes", datetime.now().strftime("%m"))
    ano = int(request.args.get("ano", 2026))
    cnpj = request.args.get("cnpj", "").strip()

    con = db()
    query = """
        SELECT c.*, d.nome_original, d.arquivo, d.token
        FROM clientes c
        LEFT JOIN documentos d ON d.cliente_id=c.id AND d.mes=? AND d.ano=?
        WHERE COALESCE(c.ativo,1)=1 AND c.contabilidade_id=?
    """
    params = [mes, ano, session.get("contabilidade_id")]

    if cnpj:
        query += " AND c.cnpj LIKE ?"
        params.append(f"%{cnpj}%")

    rows = con.execute(query + " ORDER BY c.razao", params).fetchall()
    con.close()

    return render_template("contabilidade_area.html", rows=rows, meses=MESES, anos=ANOS, mes=mes, ano=ano, cnpj=cnpj)


@app.route("/registrar-interesse-ch")
@login_required("contabilidade")
def registrar_interesse_ch():
    con = db()
    user = con.execute("SELECT * FROM users WHERE id=?", (session.get("user_id"),)).fetchone()
    cont = con.execute("SELECT * FROM contabilidades WHERE id=?", (session.get("contabilidade_id"),)).fetchone()

    usuario_nome = user["nome"] if user else "Usuário não identificado"
    usuario_email = user["email"] if user else ""
    contabilidade_nome = cont["nome"] if cont else ""
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)

    assunto = "Novo interesse de contabilidade - CH Contestado"
    corpo = f"""Uma contabilidade clicou no botão "Falar com a CH".

Usuário: {usuario_nome}
E-mail do usuário: {usuario_email}
Contabilidade: {contabilidade_nome}
Data/Hora: {agora}
IP: {ip}

Entrar em contato para apresentar o sistema CH Contestado.
"""

    ok, msg = enviar_email_simples("chcontestado@gmail.com", assunto, corpo)

    con.execute("""
        INSERT INTO interesses_ch
        (user_id, usuario_nome, usuario_email, contabilidade_id, contabilidade_nome, criado_em, ip, email_enviado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (session.get("user_id"), usuario_nome, usuario_email, session.get("contabilidade_id"), contabilidade_nome, agora, ip, 1 if ok else 0))
    con.commit()
    con.close()

    return jsonify({"ok": True, "email_enviado": bool(ok), "mensagem": msg})


@app.route("/interesses-ch")
@login_required("admin")
def interesses_ch():
    con = db()
    rows = con.execute("SELECT * FROM interesses_ch ORDER BY id DESC").fetchall()
    con.close()
    return render_template("interesses_ch.html", rows=rows)


# =========================================================
# ZIPS E E-MAILS
# =========================================================

def gerar_zip_contabilidade(contabilidade_id, mes, ano):
    con = db()
    cont = con.execute("SELECT * FROM contabilidades WHERE id=?", (contabilidade_id,)).fetchone()

    docs = con.execute("""
        SELECT d.*, c.razao
        FROM documentos d
        JOIN clientes c ON c.id=d.cliente_id
        WHERE c.contabilidade_id=? AND d.mes=? AND d.ano=?
        ORDER BY c.razao
    """, (contabilidade_id, mes, ano)).fetchall()

    if not cont or not docs:
        con.close()
        return None, cont, 0

    token = uuid.uuid4().hex
    nome_zip = f"contabilidade_{contabilidade_id}_{ano}_{mes}_{token[:8]}.zip"
    zip_path = os.path.join(ZIP_DIR, nome_zip)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for d in docs:
            origem = os.path.join(DOC_DIR, d["arquivo"])
            if os.path.exists(origem):
                pasta_cliente = secure_filename(d["razao"]) or f"cliente_{d['cliente_id']}"
                z.write(origem, f"{pasta_cliente}/{d['nome_original']}")

    con.execute("""
        INSERT INTO envios
        (contabilidade_id, mes, ano, arquivo_zip, token, email_destino, enviado_email, criado_em)
        VALUES (?, ?, ?, ?, ?, ?, 0, ?)
    """, (contabilidade_id, mes, ano, nome_zip, token, cont["email"], datetime.now().strftime("%d/%m/%Y %H:%M")))
    con.commit()
    con.close()

    return token, cont, len(docs)


@app.route("/enviar-documentos/<int:contabilidade_id>")
@login_required("admin")
def enviar_documentos(contabilidade_id):
    mes = request.args.get("mes", "01")
    ano = int(request.args.get("ano", 2026))
    modo = request.args.get("modo", "link")

    token, cont, qtd = gerar_zip_contabilidade(contabilidade_id, mes, ano)

    if not token:
        flash("Não há documentos para essa contabilidade no mês selecionado.")
        return redirect(url_for("contabilidades"))

    link = request.host_url.rstrip("/") + url_for("zip_publico", token=token)

    if modo == "email":
        ok, msg = enviar_email_link(
            cont["email"],
            f"Documentos contábeis {nome_mes(mes)}/{ano}",
            f"Olá, segue o link para baixar os documentos de {nome_mes(mes)}/{ano}:\n\n{link}"
        )
        con = db()
        con.execute("UPDATE envios SET enviado_email=? WHERE token=?", (1 if ok else 0, token))
        con.commit()
        con.close()
        flash(msg)
    else:
        flash("ZIP gerado. Link: " + link)

    return redirect(url_for("historico_envios"))


@app.route("/zip/<token>")
def zip_publico(token):
    con = db()
    envio = con.execute("SELECT * FROM envios WHERE token=?", (token,)).fetchone()
    con.close()

    if not envio:
        return "Link inválido.", 404

    return send_from_directory(ZIP_DIR, envio["arquivo_zip"], as_attachment=True)


@app.route("/historico-envios")
@login_required("admin")
def historico_envios():
    con = db()
    envios = con.execute("""
        SELECT e.*, co.nome AS contabilidade_nome
        FROM envios e
        LEFT JOIN contabilidades co ON co.id=e.contabilidade_id
        ORDER BY e.id DESC
    """).fetchall()
    con.close()
    return render_template("historico_envios.html", envios=envios)


# =========================================================
# STATUS E BACKUPS
# =========================================================

@app.route("/status-data")
@login_required("admin")
def status_data():
    info = {
        "DATA_DIR": DATA_DIR,
        "DB_PATH": DB_PATH,
        "UPLOAD_DIR": UPLOAD_DIR,
        "data_existe": os.path.isdir(DATA_DIR),
        "db_existe": os.path.exists(DB_PATH),
        "doc_dir_existe": os.path.isdir(DOC_DIR),
        "cert_dir_existe": os.path.isdir(CERT_DIR),
        "zip_dir_existe": os.path.isdir(ZIP_DIR)
    }
    return "<pre>" + json.dumps(info, indent=2, ensure_ascii=False) + "</pre>"


@app.route("/backup/database")
@login_required("admin")
def backup_database():
    return send_from_directory(os.path.dirname(DB_PATH), os.path.basename(DB_PATH), as_attachment=True)


@app.route("/backup/completo")
@login_required("admin")
def backup_completo():
    nome_zip = f"backup_ch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    caminho_zip = os.path.join(ZIP_DIR, nome_zip)

    with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as z:
        if os.path.exists(DB_PATH):
            z.write(DB_PATH, "database.db")

        if os.path.exists(UPLOAD_DIR):
            for raiz, pastas, arquivos in os.walk(UPLOAD_DIR):
                for arquivo in arquivos:
                    caminho = os.path.join(raiz, arquivo)
                    rel = os.path.relpath(caminho, DATA_DIR)
                    z.write(caminho, rel)

    return send_from_directory(ZIP_DIR, nome_zip, as_attachment=True)


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8090)))

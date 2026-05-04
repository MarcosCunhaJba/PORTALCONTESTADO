
# =========================================================
# CONFIGURAÇÕES VISUAIS / LOGO
# =========================================================

def get_config(chave, padrao=""):
    try:
        con = db()
        row = con.execute("SELECT valor FROM configuracoes WHERE chave=?", (chave,)).fetchone()
        con.close()
        return row["valor"] if row and row["valor"] is not None else padrao
    except Exception:
        return padrao


def set_config(chave, valor):
    con = db()
    con.execute("""
        INSERT INTO configuracoes (chave, valor)
        VALUES (?, ?)
        ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor
    """, (chave, valor))
    con.commit()
    con.close()


@app.context_processor
def inject_configuracoes_visuais():
    logo_topo = get_config("logo_topo", "")
    logo_login = get_config("logo_login", "")

    return {
        "LOGO_TOPO": logo_topo if logo_topo else "logo_ch_contestado.png",
        "LOGO_LOGIN": logo_login if logo_login else (logo_topo if logo_topo else "logo_ch_contestado.png")
    }


def dias_sem_contato(data_txt):
    if not data_txt:
        return None
    try:
        dt = datetime.strptime(data_txt, "%d/%m/%Y %H:%M")
        return (datetime.now() - dt).days
    except Exception:
        return None


import os
import base64
import gzip
import tempfile
import xml.etree.ElementTree as ET
import csv
import io
import json
import uuid
import zipfile
import sqlite3
import requests
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, redirect, session, url_for, send_from_directory, flash, Response, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") else BASE_DIR)

DB_PATH = os.path.join(DATA_DIR, "database.db")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
DOC_DIR = os.path.join(UPLOAD_DIR, "documentos")
CERT_DIR = os.path.join(UPLOAD_DIR, "certificados")
ZIP_DIR = os.path.join(UPLOAD_DIR, "zips")
XML_DIR = os.path.join(UPLOAD_DIR, "xmls")

for pasta in (DATA_DIR, UPLOAD_DIR, DOC_DIR, CERT_DIR, ZIP_DIR, XML_DIR):
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

def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con

def column_exists(cur, table, column):
    return any(r[1] == column for r in cur.execute(f"PRAGMA table_info({table})").fetchall())

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

    CREATE TABLE IF NOT EXISTS configuracoes (
        chave TEXT PRIMARY KEY,
        valor TEXT
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
        CREATE TABLE IF NOT EXISTS xmls_dfe (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER,
        nsu TEXT,
        schema_xml TEXT,
        chave TEXT,
        arquivo TEXT,
        criado_em TEXT,
        numero_nf TEXT,
        valor_nf TEXT,
        mes_ref TEXT,
        ano_ref INTEGER,
        tipo_doc TEXT,
        emit_cnpj TEXT,
        dest_cnpj TEXT
    );

    CREATE TABLE IF NOT EXISTS dfe_status (
        cliente_id INTEGER PRIMARY KEY,
        ult_nsu TEXT,
        max_nsu TEXT,
        cstat TEXT,
        xmotivo TEXT,
        atualizado_em TEXT
    );
    """)
    # Autorreparo completo para bancos antigos que já estavam no Railway
    # Isso evita erro 500 quando uma tabela antiga não tem alguma coluna nova.
    ensure_column(cur, "users", "nome", "TEXT")
    ensure_column(cur, "users", "email", "TEXT")
    ensure_column(cur, "users", "senha_hash", "TEXT")
    ensure_column(cur, "users", "tipo", "TEXT")
    ensure_column(cur, "users", "contabilidade_id", "INTEGER")
    ensure_column(cur, "users", "criado_em", "TEXT")

    ensure_column(cur, "contabilidades", "nome", "TEXT")
    ensure_column(cur, "contabilidades", "cnpj", "TEXT")
    ensure_column(cur, "contabilidades", "telefone", "TEXT")
    ensure_column(cur, "contabilidades", "email", "TEXT")
    ensure_column(cur, "contabilidades", "ativo", "INTEGER DEFAULT 1")
    ensure_column(cur, "contabilidades", "criado_em", "TEXT")

    ensure_column(cur, "clientes", "razao", "TEXT")
    ensure_column(cur, "clientes", "cnpj", "TEXT")
    ensure_column(cur, "clientes", "contabilidade_id", "INTEGER")
    ensure_column(cur, "clientes", "ano_certificado", "INTEGER")
    ensure_column(cur, "clientes", "senha_certificado", "TEXT")
    ensure_column(cur, "clientes", "arquivo_certificado", "TEXT")
    ensure_column(cur, "clientes", "criado_em", "TEXT")
    ensure_column(cur, "clientes", "ativo", "INTEGER DEFAULT 1")
    ensure_column(cur, "clientes", "coletor_ultimo_contato", "TEXT")
    ensure_column(cur, "clientes", "coletor_ultimo_status", "TEXT")

    ensure_column(cur, "documentos", "cliente_id", "INTEGER")
    ensure_column(cur, "documentos", "mes", "TEXT")
    ensure_column(cur, "documentos", "ano", "INTEGER")
    ensure_column(cur, "documentos", "descricao", "TEXT")
    ensure_column(cur, "documentos", "nome_original", "TEXT")
    ensure_column(cur, "documentos", "arquivo", "TEXT")
    ensure_column(cur, "documentos", "token", "TEXT")
    ensure_column(cur, "documentos", "enviado_em", "TEXT")

    ensure_column(cur, "envios", "contabilidade_id", "INTEGER")
    ensure_column(cur, "envios", "mes", "TEXT")
    ensure_column(cur, "envios", "ano", "INTEGER")
    ensure_column(cur, "envios", "arquivo_zip", "TEXT")
    ensure_column(cur, "envios", "token", "TEXT")
    ensure_column(cur, "envios", "email_destino", "TEXT")
    ensure_column(cur, "envios", "enviado_email", "INTEGER DEFAULT 0")
    ensure_column(cur, "envios", "criado_em", "TEXT")

    ensure_column(cur, "interesses_ch", "user_id", "INTEGER")
    ensure_column(cur, "interesses_ch", "usuario_nome", "TEXT")
    ensure_column(cur, "interesses_ch", "usuario_email", "TEXT")
    ensure_column(cur, "interesses_ch", "contabilidade_id", "INTEGER")
    ensure_column(cur, "interesses_ch", "contabilidade_nome", "TEXT")
    ensure_column(cur, "interesses_ch", "criado_em", "TEXT")
    ensure_column(cur, "interesses_ch", "ip", "TEXT")
    ensure_column(cur, "interesses_ch", "email_enviado", "INTEGER DEFAULT 0")
    ensure_column(cur, "xmls_dfe", "cliente_id", "INTEGER")
    ensure_column(cur, "xmls_dfe", "nsu", "TEXT")
    ensure_column(cur, "xmls_dfe", "schema_xml", "TEXT")
    ensure_column(cur, "xmls_dfe", "chave", "TEXT")
    ensure_column(cur, "xmls_dfe", "arquivo", "TEXT")
    ensure_column(cur, "xmls_dfe", "criado_em", "TEXT")
    ensure_column(cur, "xmls_dfe", "numero_nf", "TEXT")
    ensure_column(cur, "xmls_dfe", "valor_nf", "TEXT")
    ensure_column(cur, "xmls_dfe", "mes_ref", "TEXT")
    ensure_column(cur, "xmls_dfe", "ano_ref", "INTEGER")
    ensure_column(cur, "xmls_dfe", "tipo_doc", "TEXT")
    ensure_column(cur, "xmls_dfe", "emit_cnpj", "TEXT")
    ensure_column(cur, "xmls_dfe", "dest_cnpj", "TEXT")
    ensure_column(cur, "xmls_dfe", "direcao", "TEXT")
    ensure_column(cur, "xmls_dfe", "modelo_doc", "TEXT")
    ensure_column(cur, "dfe_status", "cliente_id", "INTEGER")
    ensure_column(cur, "dfe_status", "ult_nsu", "TEXT")
    ensure_column(cur, "dfe_status", "max_nsu", "TEXT")
    ensure_column(cur, "dfe_status", "cstat", "TEXT")
    ensure_column(cur, "dfe_status", "xmotivo", "TEXT")
    ensure_column(cur, "dfe_status", "atualizado_em", "TEXT")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_clientes_contabilidade ON clientes(contabilidade_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clientes_cnpj ON clientes(cnpj)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_docs_cliente_mes_ano ON documentos(cliente_id, mes, ano)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_xmls_cliente ON xmls_dfe(cliente_id, nsu)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")

    if not cur.execute("SELECT id FROM users WHERE email=?", ("admin@admin.com",)).fetchone():
        cur.execute("""
            INSERT INTO users (nome, email, senha_hash, tipo, contabilidade_id, criado_em)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("Administrador", "admin@admin.com", generate_password_hash("admin123"), "admin", None, datetime.now().isoformat()))
    con.commit()
    con.close()

init_db()

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
    return {"MESES": MESES, "ANOS": ANOS, "nome_mes": nome_mes, "user": current_user()}

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

def enviar_email_brevo(destino, assunto, corpo):
    api_key = os.environ.get("BREVO_API_KEY", "").strip()
    remetente = os.environ.get("BREVO_FROM", "").strip()

    if not api_key:
        return False, "BREVO_API_KEY não configurada no Railway."
    if not remetente or "<" not in remetente or ">" not in remetente:
        return False, "BREVO_FROM inválido. Use: CH Contestado <seuemail@gmail.com>"
    if not destino:
        return False, "E-mail de destino não informado."

    nome_remetente = remetente.split("<")[0].strip() or "CH Contestado"
    email_remetente = remetente.split("<")[1].replace(">", "").strip()

    try:
        resp = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={"accept": "application/json", "api-key": api_key, "content-type": "application/json"},
            json={
                "sender": {"name": nome_remetente, "email": email_remetente},
                "to": [{"email": destino}],
                "subject": assunto,
                "htmlContent": str(corpo).replace("\n", "<br>")
            },
            timeout=20
        )
        if resp.status_code == 201:
            return True, "E-mail enviado com sucesso via Brevo."
        return False, f"Erro Brevo {resp.status_code}: {resp.text[:500]}"
    except Exception as e:
        return False, f"Falha Brevo: {str(e)}"

def enviar_email_link(destino, assunto, corpo):
    return enviar_email_brevo(destino, assunto, corpo)

def enviar_email_simples(destino, assunto, corpo):
    return enviar_email_brevo(destino, assunto, corpo)

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

@app.route("/admin")
@login_required("admin")
def dashboard_admin():
    mes = request.args.get("mes", datetime.now().strftime("%m"))
    ano = int(request.args.get("ano", 2026))
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
        enviados += 1 if tem_doc else 0
        pendentes += 0 if tem_doc else 1
        if status == "enviado" and not tem_doc:
            continue
        if status == "pendente" and tem_doc:
            continue
        if q and q.lower() not in (c["razao"] or "").lower() and q not in (c["cnpj"] or ""):
            continue
        linhas.append({"cliente": c, "doc": doc, "tem_doc": tem_doc})
    con.close()
    return render_template("dashboard.html", mes=mes, ano=ano, status=status, q=q, linhas=linhas, total_clientes=len(clientes), enviados=enviados, pendentes=pendentes)

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

        if not nome or not email:
            flash("Informe pelo menos nome e e-mail da contabilidade.")
        else:
            try:
                cur = con.cursor()
                cont_existente = con.execute("SELECT id FROM contabilidades WHERE lower(email)=lower(?) AND COALESCE(ativo,1)=1", (email,)).fetchone()
                if cont_existente:
                    flash("Já existe uma contabilidade ativa cadastrada com esse e-mail.")
                else:
                    cur.execute("""
                        INSERT INTO contabilidades (nome, cnpj, telefone, email, ativo, criado_em)
                        VALUES (?, ?, ?, ?, 1, ?)
                    """, (nome, cnpj, telefone, email, datetime.now().isoformat()))
                    contabilidade_id = cur.lastrowid
                    user_existente = con.execute("SELECT id FROM users WHERE lower(email)=lower(?)", (email,)).fetchone()
                    if user_existente:
                        cur.execute("""
                            UPDATE users
                            SET nome=?, senha_hash=?, tipo='contabilidade', contabilidade_id=?
                            WHERE id=?
                        """, (nome, generate_password_hash(senha), contabilidade_id, user_existente["id"]))
                    else:
                        cur.execute("""
                            INSERT INTO users (nome, email, senha_hash, tipo, contabilidade_id, criado_em)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (nome, email, generate_password_hash(senha), "contabilidade", contabilidade_id, datetime.now().isoformat()))
                    con.commit()
                    flash("Contabilidade cadastrada com sucesso.")
            except Exception as e:
                con.rollback()
                flash(f"Erro ao cadastrar contabilidade: {str(e)}")

    lista = con.execute("SELECT * FROM contabilidades WHERE COALESCE(ativo,1)=1 ORDER BY nome").fetchall()
    con.close()
    return render_template("contabilidades.html", contabilidades=lista)


@app.route("/contabilidades/editar/<int:id>", methods=["GET", "POST"])
@login_required("admin")
def editar_contabilidade(id):
    con = db()
    cont = con.execute("SELECT * FROM contabilidades WHERE id=?", (id,)).fetchone()

    if not cont:
        con.close()
        flash("Contabilidade não encontrada.")
        return redirect(url_for("contabilidades"))

    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        cnpj = request.form.get("cnpj", "").strip()
        email = request.form.get("email", "").strip().lower()
        telefone = request.form.get("telefone", "").strip()
        nova_senha = request.form.get("senha", "").strip()

        if not nome or not email:
            flash("Informe nome e e-mail.")
        else:
            con.execute("UPDATE contabilidades SET nome=?, cnpj=?, email=?, telefone=? WHERE id=?",
                        (nome, cnpj, email, telefone, id))
            user = con.execute("SELECT * FROM users WHERE contabilidade_id=?", (id,)).fetchone()
            if user:
                if nova_senha:
                    con.execute("UPDATE users SET nome=?, email=?, senha_hash=? WHERE id=?",
                                (nome, email, generate_password_hash(nova_senha), user["id"]))
                else:
                    con.execute("UPDATE users SET nome=?, email=? WHERE id=?",
                                (nome, email, user["id"]))
            else:
                senha = nova_senha or "123456"
                con.execute("""
                    INSERT INTO users (nome, email, senha_hash, tipo, contabilidade_id, criado_em)
                    VALUES (?, ?, ?, 'contabilidade', ?, ?)
                """, (nome, email, generate_password_hash(senha), id, datetime.now().isoformat()))
            con.commit()
            con.close()
            flash("Contabilidade atualizada.")
            return redirect(url_for("contabilidades"))

    con.close()
    return render_template("editar_contabilidade.html", cont=cont)

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
                INSERT INTO clientes (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, arquivo_certificado, criado_em, ativo)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """, (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, arquivo_certificado, datetime.now().isoformat()))
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
    return render_template("clientes.html", itens=itens, contabs=contabs, anos=ANOS, filtro=filtro, cnpj=cnpj_busca)

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
            UPDATE clientes SET razao=?, cnpj=?, contabilidade_id=?, ano_certificado=?, senha_certificado=?, arquivo_certificado=? WHERE id=?
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
                    UPDATE clientes SET razao=?, contabilidade_id=COALESCE(?, contabilidade_id), ano_certificado=COALESCE(?, ano_certificado), senha_certificado=COALESCE(?, senha_certificado), ativo=1 WHERE id=?
                """, (razao, contabilidade_id, ano_certificado, senha_certificado, existente["id"]))
                atualizados += 1
            else:
                con.execute("""
                    INSERT INTO clientes (razao, cnpj, contabilidade_id, ano_certificado, senha_certificado, criado_em, ativo)
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
    return Response(conteudo, mimetype="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=modelo_importacao_clientes.csv"})

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
    return Response(output.getvalue(), mimetype="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=clientes_ch_contestado.csv"})

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
        INSERT INTO interesses_ch (user_id, usuario_nome, usuario_email, contabilidade_id, contabilidade_nome, criado_em, ip, email_enviado)
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

def gerar_zip_contabilidade(contabilidade_id, mes, ano):
    con = db()

    cont = con.execute("""
        SELECT co.id, co.nome, co.email
        FROM contabilidades co
        WHERE co.id=?
    """, (contabilidade_id,)).fetchone()

    docs = con.execute("""
        SELECT 
            d.id AS documento_id,
            d.cliente_id AS cliente_id,
            d.nome_original AS nome_original,
            d.arquivo AS arquivo,
            c.razao AS razao
        FROM documentos d
        JOIN clientes c ON c.id = d.cliente_id
        WHERE c.contabilidade_id = ?
          AND d.mes = ?
          AND d.ano = ?
        ORDER BY c.razao ASC, d.id ASC
    """, (contabilidade_id, mes, ano)).fetchall()

    xmls = []
    try:
        filtro_sql = filtro_xml_mes_sql()
        xmls = con.execute(f"""
            SELECT
                x.id AS xml_id,
                x.cliente_id AS cliente_id,
                x.arquivo AS arquivo,
                x.tipo_doc AS tipo_doc,
                x.direcao AS direcao,
                x.numero_nf AS numero_nf,
                x.chave AS chave,
                c.razao AS razao
            FROM xmls_dfe x
            JOIN clientes c ON c.id = x.cliente_id
            WHERE c.contabilidade_id = ?
              AND {filtro_sql}
            ORDER BY c.razao ASC, x.direcao ASC, x.tipo_doc ASC, x.numero_nf ASC, x.id ASC
        """, (contabilidade_id, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")).fetchall()
    except Exception as e:
        print("Erro ao buscar XMLs para ZIP:", str(e))
        xmls = []

    if not cont or (not docs and not xmls):
        con.close()
        return None, cont, 0

    token = uuid.uuid4().hex
    nome_zip = f"contabilidade_{contabilidade_id}_{ano}_{mes}_{token[:8]}.zip"
    zip_path = os.path.join(ZIP_DIR, nome_zip)
    os.makedirs(ZIP_DIR, exist_ok=True)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for d in docs:
            origem = os.path.join(DOC_DIR, d["arquivo"])
            if os.path.exists(origem):
                pasta_cliente = secure_filename(d["razao"] or f"cliente_{d['cliente_id']}") or f"cliente_{d['cliente_id']}"
                nome_arquivo = d["nome_original"] or d["arquivo"]
                z.write(origem, f"DOCUMENTOS/{pasta_cliente}/{nome_arquivo}")

        for x in xmls:
            origem_xml = os.path.join(XML_DIR, str(x["cliente_id"]), x["arquivo"])
            if os.path.exists(origem_xml):
                pasta_cliente_xml = secure_filename(x["razao"] or f"cliente_{x['cliente_id']}") or f"cliente_{x['cliente_id']}"
                direcao = x["direcao"] or "RECEBIDA"
                tipo = x["tipo_doc"] or "OUTROS"
                z.write(origem_xml, f"XMLS/{pasta_cliente_xml}/{direcao}/{tipo}/{x['arquivo']}")

    con.execute("""
        INSERT INTO envios (
            contabilidade_id, mes, ano, arquivo_zip, token, email_destino, enviado_email, criado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, 0, ?)
    """, (contabilidade_id, mes, ano, nome_zip, token, cont["email"], datetime.now().strftime("%d/%m/%Y %H:%M")))

    con.commit()
    con.close()

    return token, cont, len(docs) + len(xmls)


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
# XML / DF-e MANUAL
# =========================================================

def somente_digitos(valor):
    return "".join(ch for ch in str(valor or "") if ch.isdigit())


def salvar_certificado_pfx_temporario(caminho_pfx, senha):
    with open(caminho_pfx, "rb") as f:
        pfx_data = f.read()

    senha_bytes = senha.encode("utf-8") if senha else None
    private_key, certificate, additional_certs = pkcs12.load_key_and_certificates(pfx_data, senha_bytes)

    if not private_key or not certificate:
        raise Exception("Certificado PFX inválido ou senha incorreta.")

    cert_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")

    cert_temp.write(certificate.public_bytes(Encoding.PEM))
    if additional_certs:
        for cert in additional_certs:
            cert_temp.write(cert.public_bytes(Encoding.PEM))
    cert_temp.close()

    key_temp.write(private_key.private_bytes(
        Encoding.PEM,
        PrivateFormat.TraditionalOpenSSL,
        NoEncryption()
    ))
    key_temp.close()

    return cert_temp.name, key_temp.name




def extrair_dados_nfe(xml_conteudo, cnpj_cliente):
    def local(tag):
        return tag.split("}", 1)[-1] if "}" in tag else tag

    cnpj_cliente = somente_digitos(cnpj_cliente)
    chave = ""
    numero = ""
    valor = ""
    emit_cnpj = ""
    dest_cnpj = ""
    mes_ref = ""
    ano_ref = None
    tipo_doc = "OUTROS"
    direcao = ""
    eh_documento_fiscal = False

    try:
        xml_doc = ET.fromstring(xml_conteudo)
        root_name = local(xml_doc.tag)

        nomes_evento = {"procEventoNFe", "resEvento", "evento", "retEvento", "procEventoCTe", "procEventoMDFe"}
        if root_name in nomes_evento or "Evento" in root_name:
            return chave, numero, valor, emit_cnpj, dest_cnpj, mes_ref, ano_ref, tipo_doc, direcao, False

        if root_name in {"nfeProc", "NFe", "resNFe", "cteProc", "CTe", "resCTe", "mdfeProc", "MDFe", "resMDFe"}:
            eh_documento_fiscal = True

        for el in xml_doc.iter():
            nome = local(el.tag)
            if nome in ("chNFe", "chCTe", "chMDFe") and el.text and not chave:
                chave = el.text.strip()
                eh_documento_fiscal = True
            elif nome == "nNF" and el.text and not numero:
                numero = el.text.strip().lstrip("0") or "0"
            elif nome == "vNF" and el.text and not valor:
                valor = el.text.strip()

        for emit in xml_doc.iter():
            if local(emit.tag) == "emit":
                for filho in emit:
                    if local(filho.tag) == "CNPJ" and filho.text:
                        emit_cnpj = somente_digitos(filho.text)
                        break
                break

        for dest in xml_doc.iter():
            if local(dest.tag) == "dest":
                for filho in dest:
                    if local(filho.tag) == "CNPJ" and filho.text:
                        dest_cnpj = somente_digitos(filho.text)
                        break
                break

        if not emit_cnpj and root_name in {"resNFe", "resCTe", "resMDFe"}:
            for el in xml_doc.iter():
                if local(el.tag) == "CNPJ" and el.text:
                    emit_cnpj = somente_digitos(el.text)
                    break

        if chave and len(chave) >= 34:
            aa = chave[2:4]
            mm = chave[4:6]
            modelo = chave[20:22]
            mes_ref = mm
            ano_ref = int("20" + aa) if aa.isdigit() else None
            if not numero:
                numero = chave[25:34].lstrip("0") or "0"
            tipo_doc = {"55": "NFE", "65": "NFCE", "57": "CTE", "58": "MDFE"}.get(modelo, "OUTROS")

        if emit_cnpj == cnpj_cliente:
            direcao = "EMITIDA"
        elif dest_cnpj == cnpj_cliente:
            direcao = "RECEBIDA"
        elif emit_cnpj and emit_cnpj != cnpj_cliente and root_name in {"resNFe", "resCTe", "resMDFe"}:
            direcao = "RECEBIDA"

        return chave, numero, valor, emit_cnpj, dest_cnpj, mes_ref, ano_ref, tipo_doc, direcao, eh_documento_fiscal

    except Exception:
        return chave, numero, valor, emit_cnpj, dest_cnpj, mes_ref, ano_ref, tipo_doc, direcao, False


def atualizar_status_dfe(cliente_id, ult_nsu, max_nsu, cstat, xmotivo):
    con = db()
    con.execute("""
        INSERT INTO dfe_status (cliente_id, ult_nsu, max_nsu, cstat, xmotivo, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(cliente_id) DO UPDATE SET
            ult_nsu=excluded.ult_nsu,
            max_nsu=excluded.max_nsu,
            cstat=excluded.cstat,
            xmotivo=excluded.xmotivo,
            atualizado_em=excluded.atualizado_em
    """, (cliente_id, ult_nsu, max_nsu, cstat, xmotivo, datetime.now().strftime("%d/%m/%Y %H:%M")))
    con.commit()
    con.close()


def filtro_xml_mes_sql():
    return """(
        (mes_ref=? AND ano_ref=?)
        OR (
            (mes_ref IS NULL OR mes_ref='')
            AND chave IS NOT NULL
            AND length(chave) >= 6
            AND substr(chave, 5, 2)=?
            AND ('20' || substr(chave, 3, 2))=?
        )
        OR (
            (mes_ref IS NULL OR mes_ref='')
            AND (chave IS NULL OR chave='')
            AND criado_em LIKE ?
        )
    )"""

def consultar_dfe_sefaz(cliente, cuf_autor="42", tp_amb="1"):
    cnpj = somente_digitos(cliente["cnpj"])
    if len(cnpj) != 14:
        return False, "CNPJ do cliente inválido.", 0

    if not cliente["arquivo_certificado"]:
        return False, "Cliente sem certificado digital A1 cadastrado.", 0

    if not cliente["senha_certificado"]:
        return False, "Cliente sem senha do certificado cadastrada.", 0

    caminho_pfx = os.path.join(CERT_DIR, cliente["arquivo_certificado"])
    if not os.path.exists(caminho_pfx):
        return False, "Arquivo do certificado não encontrado no servidor.", 0

    con = db()
    status = con.execute("SELECT ult_nsu FROM dfe_status WHERE cliente_id=?", (cliente["id"],)).fetchone()
    if status and status["ult_nsu"]:
        ult_nsu = str(status["ult_nsu"]).zfill(15)
    else:
        ultimo = con.execute(
            "SELECT nsu FROM xmls_dfe WHERE cliente_id=? ORDER BY CAST(nsu AS INTEGER) DESC LIMIT 1",
            (cliente["id"],)
        ).fetchone()
        ult_nsu = (ultimo["nsu"] if ultimo and ultimo["nsu"] else "0").zfill(15)
    con.close()
    cert_pem = key_pem = None

    try:
        cert_pem, key_pem = salvar_certificado_pfx_temporario(caminho_pfx, cliente["senha_certificado"])

        endpoint = "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx"

        dist_xml = f"""<distDFeInt versao="1.01" xmlns="http://www.portalfiscal.inf.br/nfe">
  <tpAmb>{tp_amb}</tpAmb>
  <cUFAutor>{cuf_autor}</cUFAutor>
  <CNPJ>{cnpj}</CNPJ>
  <distNSU>
    <ultNSU>{ult_nsu}</ultNSU>
  </distNSU>
</distDFeInt>"""

        envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">
      <nfeDadosMsg>{dist_xml}</nfeDadosMsg>
    </nfeDistDFeInteresse>
  </soap12:Body>
</soap12:Envelope>"""

        headers = {
            "Content-Type": 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe/nfeDistDFeInteresse"'
        }

        resp = requests.post(
            endpoint,
            data=envelope.encode("utf-8"),
            headers=headers,
            cert=(cert_pem, key_pem),
            timeout=60
        )

        if resp.status_code != 200:
            return False, f"SEFAZ retornou HTTP {resp.status_code}: {resp.text[:500]}", 0

        root_xml = ET.fromstring(resp.content)

        def local(tag):
            return tag.split("}", 1)[-1] if "}" in tag else tag

        ret = None
        for el in root_xml.iter():
            if local(el.tag) == "retDistDFeInt":
                ret = el
                break

        if ret is None:
            return False, "Retorno da SEFAZ sem retDistDFeInt.", 0

        dados = {}
        for el in ret.iter():
            dados[local(el.tag)] = el.text

        cstat = dados.get("cStat", "")
        xmotivo = dados.get("xMotivo", "")
        ret_ult_nsu = dados.get("ultNSU", ult_nsu) or ult_nsu
        ret_max_nsu = dados.get("maxNSU", "") or ""

        atualizar_status_dfe(cliente["id"], ret_ult_nsu, ret_max_nsu, cstat, xmotivo)

        if cstat == "656":
            return False, "SEFAZ: 656 - Consumo indevido. Aguarde 1 hora para nova consulta deste cliente. O sistema manteve o último NSU para a próxima tentativa.", 0

        if cstat not in ("137", "138"):
            return False, f"SEFAZ: {cstat} - {xmotivo}", 0

        salvos = 0
        pasta_cliente = os.path.join(XML_DIR, str(cliente["id"]))
        os.makedirs(pasta_cliente, exist_ok=True)

        con = db()

        for doczip in ret.iter():
            if local(doczip.tag) != "docZip":
                continue

            nsu = doczip.attrib.get("NSU", "")
            schema_xml = doczip.attrib.get("schema", "")

            if not doczip.text:
                continue

            existente = con.execute(
                "SELECT id FROM xmls_dfe WHERE cliente_id=? AND nsu=?",
                (cliente["id"], nsu)
            ).fetchone()
            if existente:
                continue

            xml_gzip = base64.b64decode(doczip.text)
            xml_conteudo = gzip.decompress(xml_gzip)

            nome_arquivo = f"{nsu}_{secure_filename(schema_xml or 'dfe')}.xml"
            caminho_xml = os.path.join(pasta_cliente, nome_arquivo)

            with open(caminho_xml, "wb") as f:
                f.write(xml_conteudo)

            chave, numero_nf, valor_nf, emit_cnpj, dest_cnpj, mes_ref, ano_ref, tipo_doc, direcao, eh_documento_fiscal = extrair_dados_nfe(xml_conteudo, cnpj)

            # Salva somente documento fiscal, nunca evento.
            if not eh_documento_fiscal:
                continue

            # Salva XML quando o CNPJ do cliente aparece como EMITENTE ou DESTINATÁRIO.
            # Isso permite capturar notas emitidas pela empresa quando a SEFAZ retornar.
            if emit_cnpj and emit_cnpj == cnpj:
                direcao = "EMITIDA"
            elif dest_cnpj and dest_cnpj == cnpj:
                direcao = "RECEBIDA"

            if not eh_documento_fiscal:
                continue

            if (emit_cnpj or dest_cnpj) and emit_cnpj != cnpj and dest_cnpj != cnpj:
                continue

            if not direcao:
                direcao = "RECEBIDA"

            con.execute(
                "INSERT INTO xmls_dfe (cliente_id, nsu, schema_xml, chave, arquivo, criado_em, numero_nf, valor_nf, mes_ref, ano_ref, tipo_doc, emit_cnpj, dest_cnpj, direcao, modelo_doc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (cliente["id"], nsu, schema_xml, chave, nome_arquivo, datetime.now().strftime("%d/%m/%Y %H:%M"), numero_nf, valor_nf, mes_ref, ano_ref, tipo_doc, emit_cnpj, dest_cnpj, direcao, tipo_doc)
            )
            salvos += 1

        con.commit()
        con.close()

        if salvos == 0:
            return True, f"Consulta concluída. SEFAZ: {cstat} - {xmotivo}. Nenhum XML novo encontrado.", 0

        return True, f"Consulta concluída. {salvos} XML(s) novo(s) salvo(s).", salvos

    except Exception as e:
        return False, f"Erro na consulta DF-e: {str(e)}", 0

    finally:
        for tmp in (cert_pem, key_pem):
            if tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass



# =========================================================
# DOWNLOAD DO COLETOR CONFIGURADO POR CLIENTE
# =========================================================

def codigo_coletor_cliente():
    return r"""import os
import json
import time
import hashlib
import requests
from pathlib import Path
from datetime import datetime

CONFIG_FILE = "config.json"
LOG_FILE = "log.txt"
ENVIADOS_FILE = "enviados.json"

def log(msg):
    txt = f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] {msg}"
    print(txt)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(txt + "\\n")

def carregar_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def carregar_enviados():
    if not os.path.exists(ENVIADOS_FILE):
        return {}
    try:
        with open(ENVIADOS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def salvar_enviados(data):
    with open(ENVIADOS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def hash_arquivo(caminho):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest()

def montar_paths(cfg):
    cnpj = "".join(ch for ch in str(cfg["cnpj"]) if ch.isdigit())
    disco = cfg.get("disco", "C").replace(":", "").upper()
    ano_mes = datetime.now().strftime("%Y%m")

    bases = [
        f"{disco}:/CHSISTEMAS/{cnpj}",
        f"{disco}:/CH Sistemas/{cnpj}"
    ]

    tipos = cfg.get("tipos", ["NFE", "NFCE"])
    paths = []

    for base in bases:
        for tipo in tipos:
            paths.append(f"{base}/{tipo}/{cnpj}/{ano_mes}/AUTORIZADOS")

    return paths

def enviar_xml(xml_path, cfg):
    url = cfg["portal_url"].rstrip("/") + "/api/coletor/xml"
    headers = {"X-API-TOKEN": cfg["api_token"]}
    data = {"cnpj": cfg["cnpj"]}

    with open(xml_path, "rb") as f:
        files = {"arquivo": (os.path.basename(xml_path), f, "application/xml")}
        r = requests.post(url, headers=headers, files=files, data=data, timeout=60)

    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"ok": False, "erro": r.text}

def executar_uma_vez(cfg, enviados):
    paths = montar_paths(cfg)
    enviados_novos = 0

    for p in paths:
        pasta = Path(p)
        log(f"Verificando: {p}")

        if not pasta.exists():
            log("Pasta não existe.")
            continue

        xmls = list(pasta.rglob("*.xml"))
        log(f"XMLs encontrados: {len(xmls)}")

        for xml in xmls:
            caminho = str(xml)

            try:
                h = hash_arquivo(caminho)

                if enviados.get(caminho) == h:
                    continue

                status, retorno = enviar_xml(caminho, cfg)

                if status in (200, 201) and retorno.get("ok"):
                    enviados[caminho] = h
                    enviados_novos += 1
                    log(f"OK: {caminho} | {retorno.get('status')} | {retorno.get('direcao')} | {retorno.get('chave')}")
                else:
                    log(f"ERRO: {caminho} | HTTP {status} | {retorno}")

            except Exception as e:
                log(f"FALHA: {caminho} | {e}")

    return enviados_novos

def main():
    cfg = carregar_config()
    enviados = carregar_enviados()

    log("Coletor CH iniciado.")
    log(f"Portal: {cfg['portal_url']}")
    log(f"CNPJ: {cfg['cnpj']}")
    log(f"Disco: {cfg.get('disco', 'C')}")
    log(f"Intervalo: {cfg.get('intervalo_segundos', 60)}s")

    modo_continuo = cfg.get("modo_continuo", True)

    while True:
        novos = executar_uma_vez(cfg, enviados)
        salvar_enviados(enviados)
        log(f"Ciclo finalizado. Novos enviados: {novos}")

        if not modo_continuo:
            break

        time.sleep(int(cfg.get("intervalo_segundos", 60)))

if __name__ == "__main__":
    main()
"""


@app.route("/clientes/<int:id>/baixar-coletor")
@login_required("admin")
def baixar_coletor_cliente(id):
    con = db()
    cliente = con.execute("SELECT * FROM clientes WHERE id=?", (id,)).fetchone()
    con.close()

    if not cliente:
        flash("Cliente não encontrado.")
        return redirect(url_for("clientes"))

    token = os.environ.get("COLETOR_API_TOKEN", "").strip()
    if not token:
        flash("Configure a variável COLETOR_API_TOKEN no Railway antes de baixar o coletor.")
        return redirect(url_for("clientes"))

    cnpj = somente_digitos(cliente["cnpj"])
    if not cnpj:
        flash("Cliente sem CNPJ cadastrado. Informe o CNPJ antes de baixar o coletor.")
        return redirect(url_for("clientes"))

    portal_url = request.host_url.rstrip("/")

    config = {
        "portal_url": portal_url,
        "api_token": token,
        "cnpj": cnpj,
        "disco": "C",
        "tipos": ["NFE", "NFCE"],
        "intervalo_segundos": 60,
        "modo_continuo": True
    }

    nome_cliente = secure_filename(cliente["razao"] or f"cliente_{id}") or f"cliente_{id}"

    memoria = io.BytesIO()
    with zipfile.ZipFile(memoria, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("coletor.py", codigo_coletor_cliente())
        z.writestr("config.json", json.dumps(config, indent=2, ensure_ascii=False))
        z.writestr("requirements.txt", "requests\n")
        z.writestr("instalar_dependencias.bat", "@echo off\ncd /d %~dp0\npython -m pip install -r requirements.txt\npause\n")
        z.writestr("rodar_coletor.bat", "@echo off\ncd /d %~dp0\npython coletor.py\npause\n")
        z.writestr("README.txt", f"""COLETOR XML CH CONTESTADO

Cliente: {cliente['razao']}
CNPJ: {cnpj}

Este coletor já está configurado para este cliente.

Como usar:
1) Extraia este ZIP no computador do cliente.
2) Rode instalar_dependencias.bat uma vez.
3) Rode rodar_coletor.bat.

Por padrão, o coletor procura em:
C:\\CHSISTEMAS\\{cnpj}\\NFE\\{cnpj}\\ANOMES\\AUTORIZADOS
C:\\CHSISTEMAS\\{cnpj}\\NFCE\\{cnpj}\\ANOMES\\AUTORIZADOS

Também tenta:
C:\\CH Sistemas\\...

Se o XML estiver em outro disco, edite config.json e troque:
"disco": "C"
para D, E etc.
""")

    memoria.seek(0)
    nome_zip = f"coletor_{nome_cliente}_{cnpj}.zip"
    return send_file(memoria, as_attachment=True, download_name=nome_zip, mimetype="application/zip")


@app.route("/clientes/xmls/<int:id>")
@login_required("admin")
def xmls_cliente(id):
    mes = request.args.get("mes") or datetime.now().strftime("%m")
    ano = int(request.args.get("ano") or datetime.now().year)

    con = db()
    cliente = con.execute("SELECT * FROM clientes WHERE id=?", (id,)).fetchone()
    if not cliente:
        con.close()
        flash("Cliente não encontrado.")
        return redirect(url_for("clientes"))

    cnpj_cliente = somente_digitos(cliente["cnpj"])
    filtro_sql = filtro_xml_mes_sql()

    emitidas = con.execute(
        f"""SELECT * FROM xmls_dfe x
            WHERE cliente_id=?
              AND (direcao='EMITIDA' OR emit_cnpj=?)
              AND {filtro_sql}
            ORDER BY x.tipo_doc, x.numero_nf, x.id DESC""",
        (id, cnpj_cliente, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")
    ).fetchall()

    recebidas = con.execute(
        f"""SELECT * FROM xmls_dfe x
            WHERE cliente_id=?
              AND (
                    direcao='RECEBIDA'
                    OR dest_cnpj=?
                    OR (COALESCE(direcao,'')='' AND COALESCE(emit_cnpj,'')<>? AND COALESCE(dest_cnpj,'')='')
                  )
              AND {filtro_sql}
            ORDER BY x.tipo_doc, x.numero_nf, x.id DESC""",
        (id, cnpj_cliente, cnpj_cliente, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")
    ).fetchall()

    con.close()

    return render_template("xmls_cliente.html", cliente=cliente, emitidas=emitidas, recebidas=recebidas, mes=mes, ano=ano)


@app.route("/clientes/xmls/<int:id>/buscar", methods=["POST"])
@login_required("admin")
def buscar_xmls_cliente(id):
    cuf_autor = request.form.get("cuf_autor", "42").strip() or "42"
    tp_amb = request.form.get("tp_amb", "1").strip() or "1"

    con = db()
    cliente = con.execute("SELECT * FROM clientes WHERE id=?", (id,)).fetchone()
    con.close()

    if not cliente:
        flash("Cliente não encontrado.")
        return redirect(url_for("clientes"))

    ok, msg, qtd = consultar_dfe_sefaz(cliente, cuf_autor=cuf_autor, tp_amb=tp_amb)
    flash(msg)
    return redirect(url_for("xmls_cliente", id=id))


@app.route("/clientes/xmls/<int:id>/baixar/<arquivo>")
@login_required("admin")
def baixar_xml_cliente(id, arquivo):
    pasta_cliente = os.path.join(XML_DIR, str(id))
    return send_from_directory(pasta_cliente, arquivo, as_attachment=True)



@app.route("/clientes/xmls/<int:id>/baixar-todos")
@login_required("admin")
def baixar_todos_xml_cliente(id):
    mes = request.args.get("mes") or datetime.now().strftime("%m")
    ano = int(request.args.get("ano") or datetime.now().year)

    con = db()
    cliente = con.execute("SELECT * FROM clientes WHERE id=?", (id,)).fetchone()
    filtro_sql = filtro_xml_mes_sql()
    xmls = con.execute(f"SELECT * FROM xmls_dfe x WHERE cliente_id=? AND {filtro_sql} ORDER BY x.tipo_doc, x.numero_nf, x.id DESC",
                       (id, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")).fetchall()
    con.close()

    if not cliente:
        flash("Cliente não encontrado.")
        return redirect(url_for("clientes"))

    if not xmls:
        flash("Nenhum XML disponível para zipar neste mês.")
        return redirect(url_for("xmls_cliente", id=id))

    pasta_cliente = os.path.join(XML_DIR, str(id))
    pasta_zip = os.path.join(ZIP_DIR, "xmls")
    os.makedirs(pasta_zip, exist_ok=True)

    nome_base = secure_filename(cliente["razao"] or f"cliente_{id}") or f"cliente_{id}"
    nome_zip = f"xmls_{nome_base}_{ano}_{mes}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    caminho_zip = os.path.join(pasta_zip, nome_zip)

    with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for x in xmls:
            caminho_xml = os.path.join(pasta_cliente, x["arquivo"])
            if os.path.exists(caminho_xml):
                tipo = x["tipo_doc"] or "OUTROS"
                z.write(caminho_xml, os.path.join(nome_base, (x["direcao"] or "RECEBIDA"), tipo, x["arquivo"]))

    return send_from_directory(pasta_zip, nome_zip, as_attachment=True)


@app.route("/contabilidade/xmls/<int:id>")
@login_required("contabilidade")
def xmls_cliente_contabilidade(id):
    mes = request.args.get("mes") or datetime.now().strftime("%m")
    ano = int(request.args.get("ano") or datetime.now().year)

    con = db()
    cliente = con.execute(
        "SELECT * FROM clientes WHERE id=? AND contabilidade_id=? AND COALESCE(ativo,1)=1",
        (id, session.get("contabilidade_id"))
    ).fetchone()

    if not cliente:
        con.close()
        flash("Cliente não encontrado ou sem permissão.")
        return redirect(url_for("area_contabilidade"))

    cnpj_cliente = somente_digitos(cliente["cnpj"])
    filtro_sql = filtro_xml_mes_sql()

    emitidas = con.execute(
        f"""SELECT * FROM xmls_dfe x
            WHERE cliente_id=?
              AND (direcao='EMITIDA' OR emit_cnpj=?)
              AND {filtro_sql}
            ORDER BY x.tipo_doc, x.numero_nf, x.id DESC""",
        (id, cnpj_cliente, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")
    ).fetchall()

    recebidas = con.execute(
        f"""SELECT * FROM xmls_dfe x
            WHERE cliente_id=?
              AND (
                    direcao='RECEBIDA'
                    OR (COALESCE(direcao,'')='' AND dest_cnpj=?)
                    OR (COALESCE(direcao,'')='' AND COALESCE(emit_cnpj,'')<>? AND COALESCE(dest_cnpj,'')='')
                  )
              AND {filtro_sql}
            ORDER BY x.tipo_doc, x.numero_nf, x.id DESC""",
        (id, cnpj_cliente, cnpj_cliente, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")
    ).fetchall()

    con.close()

    return render_template("xmls_cliente_contabilidade.html", cliente=cliente, emitidas=emitidas, recebidas=recebidas, mes=mes, ano=ano)


@app.route("/contabilidade/xmls/<int:id>/baixar/<arquivo>")
@login_required("contabilidade")
def baixar_xml_cliente_contabilidade(id, arquivo):
    con = db()
    cliente = con.execute(
        "SELECT id FROM clientes WHERE id=? AND contabilidade_id=? AND COALESCE(ativo,1)=1",
        (id, session.get("contabilidade_id"))
    ).fetchone()
    con.close()

    if not cliente:
        flash("Sem permissão para baixar este XML.")
        return redirect(url_for("area_contabilidade"))

    return send_from_directory(os.path.join(XML_DIR, str(id)), arquivo, as_attachment=True)


@app.route("/contabilidade/xmls/<int:id>/baixar-todos")
@login_required("contabilidade")
def baixar_todos_xml_cliente_contabilidade(id):
    mes = request.args.get("mes") or datetime.now().strftime("%m")
    ano = int(request.args.get("ano") or datetime.now().year)

    con = db()
    cliente = con.execute(
        "SELECT * FROM clientes WHERE id=? AND contabilidade_id=? AND COALESCE(ativo,1)=1",
        (id, session.get("contabilidade_id"))
    ).fetchone()

    filtro_sql = filtro_xml_mes_sql()
    xmls = con.execute(f"SELECT * FROM xmls_dfe x WHERE cliente_id=? AND {filtro_sql} ORDER BY x.tipo_doc, x.numero_nf, x.id DESC",
                       (id, mes, ano, mes, str(ano), f"%/{mes}/{ano}%")).fetchall()
    con.close()

    if not cliente:
        flash("Cliente não encontrado ou sem permissão.")
        return redirect(url_for("area_contabilidade"))

    if not xmls:
        flash("Nenhum XML disponível para zipar neste mês.")
        return redirect(url_for("xmls_cliente_contabilidade", id=id, mes=mes, ano=ano))

    pasta_cliente = os.path.join(XML_DIR, str(id))
    pasta_zip = os.path.join(ZIP_DIR, "xmls")
    os.makedirs(pasta_zip, exist_ok=True)

    nome_base = secure_filename(cliente["razao"] or f"cliente_{id}") or f"cliente_{id}"
    nome_zip = f"xmls_{nome_base}_{ano}_{mes}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    caminho_zip = os.path.join(pasta_zip, nome_zip)

    with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for x in xmls:
            caminho_xml = os.path.join(pasta_cliente, x["arquivo"])
            if os.path.exists(caminho_xml):
                tipo = x["tipo_doc"] or "OUTROS"
                z.write(caminho_xml, os.path.join(nome_base, (x["direcao"] or "RECEBIDA"), tipo, x["arquivo"]))

    return send_from_directory(pasta_zip, nome_zip, as_attachment=True)


# =========================================================
# PAINEL DE STORAGE
# =========================================================

def tamanho_bytes(caminho):
    total = 0
    if os.path.isfile(caminho):
        return os.path.getsize(caminho)
    for raiz, pastas, arquivos in os.walk(caminho):
        for arquivo in arquivos:
            try:
                total += os.path.getsize(os.path.join(raiz, arquivo))
            except Exception:
                pass
    return total


def formatar_tamanho(bytes_total):
    for unidade in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_total < 1024:
            return f"{bytes_total:.2f} {unidade}"
        bytes_total /= 1024
    return f"{bytes_total:.2f} PB"



# =========================================================
# API DO COLETOR DE XML LOCAL
# =========================================================

@app.route("/api/coletor/xml", methods=["POST"])
def api_coletor_xml():
    token_config = os.environ.get("COLETOR_API_TOKEN", "").strip()
    token_recebido = request.headers.get("X-API-TOKEN", "").strip()

    if not token_config:
        return jsonify({"ok": False, "erro": "COLETOR_API_TOKEN não configurado no Railway."}), 500

    if token_recebido != token_config:
        return jsonify({"ok": False, "erro": "Token inválido."}), 401

    arquivo = request.files.get("arquivo")
    cnpj_informado = somente_digitos(request.form.get("cnpj", ""))

    if not arquivo or not arquivo.filename:
        return jsonify({"ok": False, "erro": "Arquivo XML não enviado."}), 400

    try:
        xml_conteudo = arquivo.read()

        chave, numero_nf, valor_nf, emit_cnpj, dest_cnpj, mes_ref, ano_ref, tipo_doc, direcao, eh_documento_fiscal = extrair_dados_nfe(xml_conteudo, cnpj_informado)

        if not eh_documento_fiscal:
            return jsonify({"ok": False, "erro": "Arquivo ignorado: não é XML de documento fiscal."}), 400

        cnpjs_possiveis = []
        if cnpj_informado:
            cnpjs_possiveis.append(cnpj_informado)
        if emit_cnpj:
            cnpjs_possiveis.append(emit_cnpj)
        if dest_cnpj:
            cnpjs_possiveis.append(dest_cnpj)

        cnpjs_possiveis = list(dict.fromkeys([c for c in cnpjs_possiveis if c]))

        con = db()
        cliente = None

        for cnpj_busca in cnpjs_possiveis:
            cliente = con.execute("""
                SELECT * FROM clientes
                WHERE REPLACE(REPLACE(REPLACE(cnpj,'.',''),'/',''),'-','')=?
                  AND COALESCE(ativo,1)=1
                LIMIT 1
            """, (cnpj_busca,)).fetchone()
            if cliente:
                break

        if not cliente:
            con.close()
            return jsonify({"ok": False, "erro": "Cliente não encontrado para o CNPJ do XML."}), 404

        cnpj_cliente = somente_digitos(cliente["cnpj"])

        if emit_cnpj == cnpj_cliente:
            direcao = "EMITIDA"
        elif dest_cnpj == cnpj_cliente:
            direcao = "RECEBIDA"
        elif not direcao:
            direcao = "RECEBIDA"

        if chave:
            existente = con.execute(
                "SELECT id FROM xmls_dfe WHERE cliente_id=? AND chave=?",
                (cliente["id"], chave)
            ).fetchone()
            if existente:
                con.execute("""
                    UPDATE clientes
                    SET coletor_ultimo_contato=?, coletor_ultimo_status=?
                    WHERE id=?
                """, (datetime.now().strftime("%d/%m/%Y %H:%M"), "DUPLICADO", cliente["id"]))
                con.commit()
                con.close()
                return jsonify({
                    "ok": True,
                    "status": "duplicado",
                    "mensagem": "XML já existia.",
                    "chave": chave
                })

        pasta_cliente = os.path.join(XML_DIR, str(cliente["id"]))
        os.makedirs(pasta_cliente, exist_ok=True)

        nome_original = secure_filename(arquivo.filename)
        if not nome_original.lower().endswith(".xml"):
            nome_original += ".xml"

        nome_salvo = f"{uuid.uuid4().hex}_{nome_original}"
        caminho_xml = os.path.join(pasta_cliente, nome_salvo)

        with open(caminho_xml, "wb") as f:
            f.write(xml_conteudo)

        nsu = f"COLETOR_{uuid.uuid4().hex[:12]}"

        con.execute("""
            INSERT INTO xmls_dfe (
                cliente_id, nsu, schema_xml, chave, arquivo, criado_em,
                numero_nf, valor_nf, mes_ref, ano_ref, tipo_doc,
                emit_cnpj, dest_cnpj, direcao, modelo_doc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cliente["id"],
            nsu,
            "coletor_xml",
            chave,
            nome_salvo,
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            numero_nf,
            valor_nf,
            mes_ref,
            ano_ref,
            tipo_doc,
            emit_cnpj,
            dest_cnpj,
            direcao,
            tipo_doc
        ))

        con.execute("""
            UPDATE clientes
            SET coletor_ultimo_contato=?, coletor_ultimo_status=?
            WHERE id=?
        """, (datetime.now().strftime("%d/%m/%Y %H:%M"), "OK", cliente["id"]))

        con.commit()
        con.close()

        return jsonify({
            "ok": True,
            "status": "salvo",
            "cliente_id": cliente["id"],
            "cliente": cliente["razao"],
            "direcao": direcao,
            "tipo_doc": tipo_doc,
            "numero_nf": numero_nf,
            "chave": chave
        })

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500




@app.route("/admin/coletor-status")
@login_required("admin")
def coletor_status():
    con = db()
    clientes = con.execute("""
        SELECT id, razao, cnpj, coletor_ultimo_contato, coletor_ultimo_status
        FROM clientes
        WHERE COALESCE(ativo,1)=1
        ORDER BY razao
    """).fetchall()
    con.close()
    return render_template("coletor_status.html", clientes=clientes)


@app.route("/admin/aparencia", methods=["GET", "POST"])
@login_required("admin")
def configurar_aparencia():
    if request.method == "POST":
        os.makedirs(os.path.join("static", "uploads"), exist_ok=True)

        logo_topo = request.files.get("logo_topo")
        logo_login = request.files.get("logo_login")

        if logo_topo and logo_topo.filename:
            ext = os.path.splitext(logo_topo.filename)[1].lower()
            if ext not in [".png", ".jpg", ".jpeg", ".webp"]:
                flash("Logo do topo precisa ser PNG, JPG ou WEBP.")
                return redirect(url_for("configurar_aparencia"))

            nome = f"uploads/logo_topo_{uuid.uuid4().hex}{ext}"
            caminho = os.path.join("static", nome)
            logo_topo.save(caminho)
            set_config("logo_topo", nome)

        if logo_login and logo_login.filename:
            ext = os.path.splitext(logo_login.filename)[1].lower()
            if ext not in [".png", ".jpg", ".jpeg", ".webp"]:
                flash("Logo do login precisa ser PNG, JPG ou WEBP.")
                return redirect(url_for("configurar_aparencia"))

            nome = f"uploads/logo_login_{uuid.uuid4().hex}{ext}"
            caminho = os.path.join("static", nome)
            logo_login.save(caminho)
            set_config("logo_login", nome)

        flash("Aparência atualizada com sucesso.")
        return redirect(url_for("configurar_aparencia"))

    return render_template("aparencia.html")


@app.route("/admin/storage")
@login_required("admin")
def painel_storage():
    caminhos = [
        ("Banco SQLite", DB_PATH),
        ("Uploads - Documentos", DOC_DIR),
        ("Uploads - Certificados", CERT_DIR),
        ("XMLs", XML_DIR if "XML_DIR" in globals() else os.path.join(UPLOAD_DIR, "xmls")),
        ("ZIPs temporários", ZIP_DIR),
        ("Uploads total", UPLOAD_DIR),
        ("Data total", DATA_DIR),
    ]

    itens = []
    for nome, caminho in caminhos:
        tamanho = tamanho_bytes(caminho) if os.path.exists(caminho) else 0
        qtd_arquivos = 0
        if os.path.isdir(caminho):
            for _, _, arquivos in os.walk(caminho):
                qtd_arquivos += len(arquivos)
        elif os.path.isfile(caminho):
            qtd_arquivos = 1

        itens.append({
            "nome": nome,
            "caminho": caminho,
            "bytes": tamanho,
            "tamanho": formatar_tamanho(tamanho),
            "arquivos": qtd_arquivos
        })

    maiores = []
    for raiz, pastas, arquivos in os.walk(DATA_DIR):
        for arquivo in arquivos:
            caminho = os.path.join(raiz, arquivo)
            try:
                tamanho = os.path.getsize(caminho)
                maiores.append({
                    "arquivo": os.path.relpath(caminho, DATA_DIR),
                    "bytes": tamanho,
                    "tamanho": formatar_tamanho(tamanho)
                })
            except Exception:
                pass

    maiores = sorted(maiores, key=lambda x: x["bytes"], reverse=True)[:30]

    return render_template("storage.html", itens=itens, maiores=maiores)


@app.route("/admin/storage/limpar-zips", methods=["POST"])
@login_required("admin")
def limpar_zips():
    apagados = 0
    liberado = 0

    if os.path.exists(ZIP_DIR):
        for raiz, pastas, arquivos in os.walk(ZIP_DIR):
            for arquivo in arquivos:
                caminho = os.path.join(raiz, arquivo)
                try:
                    liberado += os.path.getsize(caminho)
                    os.remove(caminho)
                    apagados += 1
                except Exception:
                    pass

    flash(f"ZIPs temporários apagados: {apagados}. Espaço liberado: {formatar_tamanho(liberado)}.")
    return redirect(url_for("painel_storage"))


@app.route("/admin/storage/limpar-zips-antigos", methods=["POST"])
@login_required("admin")
def limpar_zips_antigos():
    import time
    dias = int(request.form.get("dias", 1))
    limite = time.time() - (dias * 86400)
    apagados = 0
    liberado = 0

    if os.path.exists(ZIP_DIR):
        for raiz, pastas, arquivos in os.walk(ZIP_DIR):
            for arquivo in arquivos:
                caminho = os.path.join(raiz, arquivo)
                try:
                    if os.path.getmtime(caminho) < limite:
                        liberado += os.path.getsize(caminho)
                        os.remove(caminho)
                        apagados += 1
                except Exception:
                    pass

    flash(f"ZIPs com mais de {dias} dia(s) apagados: {apagados}. Espaço liberado: {formatar_tamanho(liberado)}.")
    return redirect(url_for("painel_storage"))


@app.route("/admin/storage/vacuum-db", methods=["POST"])
@login_required("admin")
def vacuum_db():
    antes = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    con = db()
    con.execute("VACUUM")
    con.close()
    depois = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0

    flash(f"Banco otimizado. Antes: {formatar_tamanho(antes)} | Depois: {formatar_tamanho(depois)}.")
    return redirect(url_for("painel_storage"))


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


@app.errorhandler(Exception)
def tratar_erro_geral(e):
    # Mostra erro amigável e grava no log do Railway.
    print("ERRO GERAL:", repr(e))
    try:
        flash(f"Erro interno: {str(e)}")
        return redirect(url_for("dashboard"))
    except Exception:
        return f"Erro interno: {str(e)}", 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8090)))

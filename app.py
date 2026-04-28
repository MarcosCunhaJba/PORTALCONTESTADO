import os, sqlite3, uuid, zipfile, smtplib, json, csv, io
from datetime import datetime
from email.message import EmailMessage
from flask import Flask, render_template, request, redirect, session, url_for, send_from_directory, flash, Response
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from config import SECRET_KEY

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Railway + SQLite persistente:
# Se existir /data, o sistema salva banco, uploads e configurações dentro do volume.
# Localmente, continua salvando dentro da pasta do projeto.
DATA_DIR = os.environ.get('DATA_DIR')
if not DATA_DIR:
    DATA_DIR = '/data' if os.path.isdir('/data') else BASE_DIR

DB_PATH = os.path.join(DATA_DIR, 'database.db')
UPLOAD_DIR = os.path.join(DATA_DIR, 'uploads')
DOC_DIR = os.path.join(UPLOAD_DIR, 'documentos')
CERT_DIR = os.path.join(UPLOAD_DIR, 'certificados')
ZIP_DIR = os.path.join(UPLOAD_DIR, 'zips')
CONFIG_EMAIL = os.path.join(DATA_DIR, 'email_config.json')

for p in (DATA_DIR, UPLOAD_DIR, DOC_DIR, CERT_DIR, ZIP_DIR):
    os.makedirs(p, exist_ok=True)

app = Flask(__name__)
app.secret_key = SECRET_KEY
MESES = [('01','Janeiro'),('02','Fevereiro'),('03','Março'),('04','Abril'),('05','Maio'),('06','Junho'),('07','Julho'),('08','Agosto'),('09','Setembro'),('10','Outubro'),('11','Novembro'),('12','Dezembro')]
ANOS = list(range(2026, 2036))

def db():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.row_factory = sqlite3.Row
    return con

def column_exists(cur, table, col):
    return any(r[1] == col for r in cur.execute(f"PRAGMA table_info({table})").fetchall())

def init_db():
    con = db(); cur = con.cursor()
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT, email TEXT UNIQUE, senha_hash TEXT, tipo TEXT, contabilidade_id INTEGER);
    CREATE TABLE IF NOT EXISTS contabilidades(id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL, cnpj TEXT, telefone TEXT, email TEXT);
    CREATE TABLE IF NOT EXISTS clientes(id INTEGER PRIMARY KEY AUTOINCREMENT, razao TEXT NOT NULL, cnpj TEXT, contabilidade_id INTEGER, ano_certificado INTEGER, senha_certificado TEXT, arquivo_certificado TEXT, criado_em TEXT);
    CREATE TABLE IF NOT EXISTS documentos(id INTEGER PRIMARY KEY AUTOINCREMENT, cliente_id INTEGER, mes TEXT, ano INTEGER, nome_original TEXT, arquivo TEXT, token TEXT UNIQUE, enviado_em TEXT);
    CREATE TABLE IF NOT EXISTS envios(id INTEGER PRIMARY KEY AUTOINCREMENT, contabilidade_id INTEGER, mes TEXT, ano INTEGER, arquivo_zip TEXT, token TEXT UNIQUE, email_destino TEXT, enviado_email INTEGER DEFAULT 0, criado_em TEXT);
    CREATE TABLE IF NOT EXISTS interesses_ch(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, usuario_nome TEXT, usuario_email TEXT, contabilidade_id INTEGER, contabilidade_nome TEXT, criado_em TEXT, ip TEXT, email_enviado INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS export_tokens(id INTEGER PRIMARY KEY AUTOINCREMENT, senha TEXT NOT NULL, criado_em TEXT, usado INTEGER DEFAULT 0);
    ''')
    if not cur.execute('SELECT id FROM users WHERE email=?', ('admin@admin.com',)).fetchone():
        cur.execute('INSERT INTO users(nome,email,senha_hash,tipo) VALUES(?,?,?,?)', ('Administrador','admin@admin.com',generate_password_hash('admin123'),'admin'))
    con.commit(); con.close()
init_db()

def login_required(tipo=None):
    def deco(fn):
        def wrapper(*args, **kwargs):
            if 'user_id' not in session: return redirect(url_for('login'))
            if tipo and session.get('tipo') != tipo: return redirect(url_for('dashboard'))
            return fn(*args, **kwargs)
        wrapper.__name__ = fn.__name__
        return wrapper
    return deco

def email_config():
    if os.path.exists(CONFIG_EMAIL):
        with open(CONFIG_EMAIL, 'r', encoding='utf-8') as f: return json.load(f)
    return {"smtp_host":"smtp.gmail.com","smtp_port":587,"smtp_user":"","smtp_password":"","remetente_nome":"CH Contestado"}

def enviar_email_link(destino, assunto, corpo):
    cfg = email_config()
    if not cfg.get('smtp_user') or not cfg.get('smtp_password') or not destino:
        return False, 'SMTP não configurado ou contabilidade sem e-mail.'
    msg = EmailMessage(); msg['Subject']=assunto; msg['From']=cfg.get('smtp_user'); msg['To']=destino; msg.set_content(corpo)
    with smtplib.SMTP(cfg.get('smtp_host','smtp.gmail.com'), int(cfg.get('smtp_port',587))) as s:
        s.starttls(); s.login(cfg['smtp_user'], cfg['smtp_password']); s.send_message(msg)
    return True, 'E-mail enviado.'


def enviar_email_simples(destino, assunto, corpo):
    cfg = email_config()
    if not cfg.get('smtp_user') or not cfg.get('smtp_password') or not destino:
        return False, 'SMTP não configurado.'
    msg = EmailMessage()
    msg['Subject'] = assunto
    msg['From'] = cfg.get('smtp_user')
    msg['To'] = destino
    msg.set_content(corpo)
    with smtplib.SMTP(cfg.get('smtp_host','smtp.gmail.com'), int(cfg.get('smtp_port',587))) as s:
        s.starttls()
        s.login(cfg['smtp_user'], cfg['smtp_password'])
        s.send_message(msg)
    return True, 'E-mail enviado.'

@app.route('/', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email','').strip().lower(); senha=request.form.get('senha','')
        con=db(); u=con.execute('SELECT * FROM users WHERE email=?',(email,)).fetchone(); con.close()
        if u and check_password_hash(u['senha_hash'], senha):
            session.update({'user_id':u['id'],'nome':u['nome'],'tipo':u['tipo'],'contabilidade_id':u['contabilidade_id']})
            return redirect(url_for('dashboard'))
        flash('Login inválido')
    return render_template('login.html')

@app.route('/logout')
def logout(): session.clear(); return redirect(url_for('login'))

@app.route('/dashboard')
@login_required()
def dashboard():
    if session.get('tipo')=='contabilidade': return redirect(url_for('area_contabilidade'))
    mes=request.args.get('mes', datetime.now().strftime('%m')); ano=int(request.args.get('ano', max(2026, datetime.now().year)))
    status=request.args.get('status','todos')
    con=db()
    rows=con.execute('''SELECT c.*, co.nome contabilidade_nome, d.arquivo, d.nome_original, d.token, d.enviado_em
                        FROM clientes c LEFT JOIN contabilidades co ON co.id=c.contabilidade_id
                        LEFT JOIN documentos d ON d.cliente_id=c.id AND d.mes=? AND d.ano=?
                        ORDER BY c.razao''',(mes,ano)).fetchall()
    if status == 'enviado': rows=[r for r in rows if r['arquivo']]
    if status == 'pendente': rows=[r for r in rows if not r['arquivo']]
    stats={'clientes':con.execute('SELECT COUNT(*) n FROM clientes').fetchone()['n'], 'contabilidades':con.execute('SELECT COUNT(*) n FROM contabilidades').fetchone()['n'], 'pendentes':sum(1 for r in rows if not r['arquivo']), 'enviados':sum(1 for r in rows if r['arquivo'])}
    con.close()
    return render_template('dashboard.html', rows=rows, stats=stats, meses=MESES, anos=ANOS, mes=mes, ano=ano, status=status)

@app.route('/contabilidades', methods=['GET','POST'])
@login_required('admin')
def contabilidades():
    con=db()
    if request.method=='POST':
        nome=request.form['nome']; email=request.form.get('email','').strip().lower(); senha=request.form.get('senha') or '123456'
        cur=con.cursor(); cur.execute('INSERT INTO contabilidades(nome,cnpj,telefone,email) VALUES(?,?,?,?)',(nome,request.form.get('cnpj'),request.form.get('telefone'),email)); cid=cur.lastrowid
        if email: cur.execute('INSERT OR IGNORE INTO users(nome,email,senha_hash,tipo,contabilidade_id) VALUES(?,?,?,?,?)',(nome,email,generate_password_hash(senha),'contabilidade',cid))
        con.commit(); flash('Contabilidade cadastrada')
    itens=con.execute('SELECT * FROM contabilidades ORDER BY nome').fetchall(); con.close()
    return render_template('contabilidades.html', itens=itens, meses=MESES, anos=ANOS)

@app.route('/contabilidades/excluir/<int:id>')
@login_required('admin')
def excluir_contabilidade(id):
    con=db(); con.execute('UPDATE clientes SET contabilidade_id=NULL WHERE contabilidade_id=?',(id,)); con.execute('DELETE FROM users WHERE contabilidade_id=?',(id,)); con.execute('DELETE FROM contabilidades WHERE id=?',(id,)); con.commit(); con.close(); flash('Contabilidade excluída'); return redirect(url_for('contabilidades'))

@app.route('/clientes', methods=['GET','POST'])
@login_required('admin')
def clientes():
    con=db()
    if request.method=='POST':
        cert=None; f=request.files.get('certificado')
        if f and f.filename:
            cert=f"{uuid.uuid4().hex}_{secure_filename(f.filename)}"; f.save(os.path.join(CERT_DIR, cert))
        contab = request.form.get('contabilidade_id') or None
        con.execute('''INSERT INTO clientes(razao,cnpj,contabilidade_id,ano_certificado,senha_certificado,arquivo_certificado,criado_em) VALUES(?,?,?,?,?,?,?)''',
                    (request.form['razao'], request.form.get('cnpj'), contab, request.form.get('ano_certificado') or None, request.form.get('senha_certificado'), cert, datetime.now().isoformat()))
        con.commit(); flash('Cliente cadastrado')
    filtro=request.args.get('filtro','todos')
    cnpj=request.args.get('cnpj','').strip()
    contabs=con.execute('SELECT * FROM contabilidades ORDER BY nome').fetchall()
    sql='''SELECT c.*, co.nome contabilidade_nome FROM clientes c LEFT JOIN contabilidades co ON co.id=c.contabilidade_id'''
    params=[]
    condicoes=[]
    if filtro=='sem_contabilidade':
        condicoes.append('(c.contabilidade_id IS NULL OR c.contabilidade_id="")')
    if cnpj:
        condicoes.append('c.cnpj LIKE ?')
        params.append('%'+cnpj+'%')
    if condicoes:
        sql += ' WHERE ' + ' AND '.join(condicoes)
    sql += ' ORDER BY COALESCE(c.ano_certificado,9999), c.razao'
    itens=con.execute(sql, params).fetchall(); con.close()
    return render_template('clientes.html', itens=itens, contabs=contabs, anos=ANOS, filtro=filtro, cnpj=cnpj)

@app.route('/clientes/editar/<int:id>', methods=['GET','POST'])
@login_required('admin')
def editar_cliente(id):
    con=db(); cliente=con.execute('SELECT * FROM clientes WHERE id=?',(id,)).fetchone()
    if request.method=='POST':
        cert=cliente['arquivo_certificado']; f=request.files.get('certificado')
        if f and f.filename:
            cert=f"{uuid.uuid4().hex}_{secure_filename(f.filename)}"; f.save(os.path.join(CERT_DIR, cert))
        con.execute('''UPDATE clientes SET razao=?, cnpj=?, contabilidade_id=?, ano_certificado=?, senha_certificado=?, arquivo_certificado=? WHERE id=?''',
                    (request.form['razao'], request.form.get('cnpj'), request.form.get('contabilidade_id') or None, request.form.get('ano_certificado') or None, request.form.get('senha_certificado'), cert, id))
        con.commit(); con.close(); flash('Cliente atualizado'); return redirect(url_for('clientes'))
    contabs=con.execute('SELECT * FROM contabilidades ORDER BY nome').fetchall(); con.close()
    return render_template('editar_cliente.html', cliente=cliente, contabs=contabs, anos=ANOS)

@app.route('/clientes/excluir/<int:id>')
@login_required('admin')
def excluir_cliente(id):
    con=db(); docs=con.execute('SELECT arquivo FROM documentos WHERE cliente_id=?',(id,)).fetchall()
    for d in docs:
        try: os.remove(os.path.join(DOC_DIR,d['arquivo']))
        except Exception: pass
    con.execute('DELETE FROM documentos WHERE cliente_id=?',(id,)); con.execute('DELETE FROM clientes WHERE id=?',(id,)); con.commit(); con.close(); flash('Cliente excluído'); return redirect(url_for('clientes'))

@app.route('/upload/<int:cliente_id>', methods=['GET','POST'])
@login_required('admin')
def upload(cliente_id):
    mes=request.args.get('mes','01'); ano=int(request.args.get('ano',2026)); con=db(); cliente=con.execute('SELECT * FROM clientes WHERE id=?',(cliente_id,)).fetchone()
    if request.method=='POST':
        f=request.files.get('arquivo')
        if f and f.filename:
            filename=f"{cliente_id}_{ano}_{mes}_{uuid.uuid4().hex}_{secure_filename(f.filename)}"; f.save(os.path.join(DOC_DIR, filename))
            con.execute('DELETE FROM documentos WHERE cliente_id=? AND mes=? AND ano=?',(cliente_id,mes,ano))
            con.execute('INSERT INTO documentos(cliente_id,mes,ano,nome_original,arquivo,token,enviado_em) VALUES(?,?,?,?,?,?,?)',(cliente_id,mes,ano,f.filename,filename,uuid.uuid4().hex,datetime.now().strftime('%d/%m/%Y %H:%M')))
            con.commit(); con.close(); return redirect(url_for('dashboard', mes=mes, ano=ano))
    con.close(); return render_template('upload.html', cliente=cliente, mes=mes, ano=ano, meses=MESES)

@app.route('/contabilidade')
@login_required('contabilidade')
def area_contabilidade():
    mes=request.args.get('mes','01'); ano=int(request.args.get('ano',2026)); cnpj=request.args.get('cnpj','').strip()
    con=db(); q='''SELECT c.*, d.nome_original, d.arquivo, d.token FROM clientes c LEFT JOIN documentos d ON d.cliente_id=c.id AND d.mes=? AND d.ano=? WHERE c.contabilidade_id=?'''; params=[mes,ano,session.get('contabilidade_id')]
    if cnpj: q += ' AND c.cnpj LIKE ?'; params.append('%'+cnpj+'%')
    rows=con.execute(q+' ORDER BY c.razao', params).fetchall(); con.close()
    return render_template('contabilidade_area.html', rows=rows, meses=MESES, anos=ANOS, mes=mes, ano=ano, cnpj=cnpj)

def gerar_zip_contabilidade(contabilidade_id, mes, ano):
    con=db(); cont=con.execute('SELECT * FROM contabilidades WHERE id=?',(contabilidade_id,)).fetchone()
    docs=con.execute('''SELECT d.*, c.razao FROM documentos d JOIN clientes c ON c.id=d.cliente_id WHERE c.contabilidade_id=? AND d.mes=? AND d.ano=? ORDER BY c.razao''',(contabilidade_id,mes,ano)).fetchall()
    if not cont or not docs: con.close(); return None, cont, 0
    token=uuid.uuid4().hex
    nome_zip=f"contabilidade_{contabilidade_id}_{ano}_{mes}_{token[:8]}.zip"
    zip_path=os.path.join(ZIP_DIR,nome_zip)
    with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as z:
        for d in docs:
            origem=os.path.join(DOC_DIR,d['arquivo'])
            if os.path.exists(origem):
                pasta=secure_filename(d['razao']) or f"cliente_{d['cliente_id']}"
                z.write(origem, f"{pasta}/{d['nome_original']}")
    con.execute('INSERT INTO envios(contabilidade_id,mes,ano,arquivo_zip,token,email_destino,enviado_email,criado_em) VALUES(?,?,?,?,?,?,?,?)',(contabilidade_id,mes,ano,nome_zip,token,cont['email'],0,datetime.now().strftime('%d/%m/%Y %H:%M')))
    con.commit(); con.close(); return token, cont, len(docs)

@app.route('/enviar-documentos/<int:contabilidade_id>')
@login_required('admin')
def enviar_documentos(contabilidade_id):
    mes=request.args.get('mes','01'); ano=int(request.args.get('ano',2026)); modo=request.args.get('modo','link')
    token, cont, qtd = gerar_zip_contabilidade(contabilidade_id, mes, ano)
    if not token:
        flash('Não há documentos para essa contabilidade no mês selecionado.'); return redirect(url_for('contabilidades'))
    link=request.host_url.rstrip('/') + url_for('zip_publico', token=token)
    if modo == 'email':
        ok,msg=enviar_email_link(cont['email'], f'Documentos contábeis {mes}/{ano}', f'Olá, segue o link para baixar os documentos de {mes}/{ano}:\n\n{link}')
        con=db(); con.execute('UPDATE envios SET enviado_email=? WHERE token=?',(1 if ok else 0, token)); con.commit(); con.close(); flash(msg)
    else:
        flash('ZIP gerado. Link: '+link)
    return redirect(url_for('historico_envios'))

@app.route('/historico-envios')
@login_required('admin')
def historico_envios():
    con=db(); envios=con.execute('''SELECT e.*, co.nome contabilidade_nome FROM envios e LEFT JOIN contabilidades co ON co.id=e.contabilidade_id ORDER BY e.id DESC''').fetchall(); con.close()
    return render_template('historico_envios.html', envios=envios)

@app.route('/config-email', methods=['GET','POST'])
@login_required('admin')
def config_email():
    cfg=email_config()
    if request.method=='POST':
        cfg={k:request.form.get(k,'') for k in ['smtp_host','smtp_port','smtp_user','smtp_password','remetente_nome']}
        with open(CONFIG_EMAIL,'w',encoding='utf-8') as f: json.dump(cfg,f,ensure_ascii=False,indent=2)
        flash('Configuração de e-mail salva')
    return render_template('config_email.html', cfg=cfg)

@app.route('/download/<arquivo>')
@login_required()
def download(arquivo): return send_from_directory(DOC_DIR, arquivo, as_attachment=True)
@app.route('/certificado/<arquivo>')
@login_required('admin')
def certificado(arquivo): return send_from_directory(CERT_DIR, arquivo, as_attachment=True)
@app.route('/s/<token>')
def compartilhado(token):
    con=db(); d=con.execute('SELECT * FROM documentos WHERE token=?',(token,)).fetchone(); con.close()
    if not d: return 'Link inválido',404
    return send_from_directory(DOC_DIR, d['arquivo'], as_attachment=True)
@app.route('/zip/<token>')
def zip_publico(token):
    con=db(); e=con.execute('SELECT * FROM envios WHERE token=?',(token,)).fetchone(); con.close()
    if not e: return 'Link inválido',404
    return send_from_directory(ZIP_DIR, e['arquivo_zip'], as_attachment=True)


@app.route('/registrar-interesse-ch')
@login_required('contabilidade')
def registrar_interesse_ch():
    con = db()
    user = con.execute('SELECT * FROM users WHERE id=?', (session.get('user_id'),)).fetchone()
    cont = con.execute('SELECT * FROM contabilidades WHERE id=?', (session.get('contabilidade_id'),)).fetchone()

    usuario_nome = user['nome'] if user else 'Usuário não identificado'
    usuario_email = user['email'] if user else ''
    contabilidade_nome = cont['nome'] if cont else ''
    agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)

    assunto = 'Novo interesse de contabilidade - CH Contestado'
    corpo = f'''Uma contabilidade clicou no botão "Falar com a CH".

Usuário: {usuario_nome}
E-mail do usuário: {usuario_email}
Contabilidade: {contabilidade_nome}
Data/Hora: {agora}
IP: {ip}

Entrar em contato para apresentar o sistema CH Contestado.
'''

    ok, msg = enviar_email_simples('chcontestado@gmail.com', assunto, corpo)

    con.execute('''
        INSERT INTO interesses_ch(user_id, usuario_nome, usuario_email, contabilidade_id, contabilidade_nome, criado_em, ip, email_enviado)
        VALUES(?,?,?,?,?,?,?,?)
    ''', (session.get('user_id'), usuario_nome, usuario_email, session.get('contabilidade_id'), contabilidade_nome, agora, ip, 1 if ok else 0))
    con.commit()
    con.close()

    return {'ok': True, 'email_enviado': bool(ok), 'mensagem': msg}


@app.route('/interesses-ch')
@login_required('admin')
def interesses_ch():
    con = db()
    rows = con.execute('''
        SELECT * FROM interesses_ch
        ORDER BY id DESC
    ''').fetchall()
    con.close()
    return render_template('interesses_ch.html', rows=rows)



@app.route('/clientes/solicitar-exportacao')
@login_required('admin')
def solicitar_exportacao_clientes():

    import random
    senha = str(random.randint(100000, 999999))
    agora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    con = db()
    con.execute('INSERT INTO export_tokens(senha, criado_em, usado) VALUES(?,?,0)', (senha, agora))
    con.commit()
    con.close()

    try:
        assunto = 'Senha para exportar clientes - CH Contestado'
        corpo = f"""Senha: {senha}
Data/Hora: {agora}"""

        enviar_email_simples('chcontestado@gmail.com', assunto, corpo)
        flash('Senha enviada por e-mail com sucesso.')

    except Exception as e:
        print("ERRO EMAIL:", e)
        flash(f'SMTP não configurado. Senha gerada: {senha}')

    return redirect(url_for('clientes'))


@app.route('/clientes/exportar-csv', methods=['POST'])
@login_required('admin')
def exportar_clientes_csv():
    senha = request.form.get('senha_exportacao', '').strip()

    con = db()
    token = con.execute('''
        SELECT * FROM export_tokens
        WHERE senha=? AND usado=0
        ORDER BY id DESC
        LIMIT 1
    ''', (senha,)).fetchone()

    if not token:
        con.close()
        flash('Senha inválida ou já utilizada. Solicite uma nova senha.')
        return redirect(url_for('clientes'))

    con.execute('UPDATE export_tokens SET usado=1 WHERE id=?', (token['id'],))
    con.commit()

    rows = con.execute('''
        SELECT 
            c.id,
            c.razao,
            c.cnpj,
            COALESCE(co.nome, '') AS contabilidade,
            COALESCE(c.ano_certificado, '') AS ano_certificado,
            COALESCE(c.senha_certificado, '') AS senha_certificado,
            COALESCE(c.arquivo_certificado, '') AS arquivo_certificado,
            COALESCE(c.criado_em, '') AS criado_em
        FROM clientes c
        LEFT JOIN contabilidades co ON co.id = c.contabilidade_id
        ORDER BY c.razao
    ''').fetchall()
    con.close()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['ID', 'Cliente', 'CNPJ', 'Contabilidade', 'Ano Certificado', 'Senha Certificado', 'Arquivo Certificado', 'Criado em'])

    for r in rows:
        writer.writerow([
            r['id'], r['razao'], r['cnpj'], r['contabilidade'],
            r['ano_certificado'], r['senha_certificado'], r['arquivo_certificado'], r['criado_em']
        ])

    conteudo = output.getvalue()
    output.close()

    return Response(
        conteudo,
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': 'attachment; filename=clientes_ch_contestado.csv'}
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8090)))


# 🔥 BACKUP DOWNLOAD
@app.route('/backup-download')
def backup_download():
    try:
        return send_file(DB_PATH, as_attachment=True)
    except Exception as e:
        return f"Erro ao baixar backup: {str(e)}"

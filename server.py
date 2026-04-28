from waitress import serve
from app import app
from config import PORTA

print("============================================")
print(" CH Contestado - Servidor Local")
print(" Acesse na rede: http://DESKTOP-J4DJ8JS:8090")
print("============================================")
serve(app, host="0.0.0.0", port=PORTA)

import os
from flask import Flask, redirect, session, url_for
from dotenv import load_dotenv

# Blueprints
from routes.proveedores_bp import proveedores_bp
from routes.login_bp import login_bp
from routes.partidas_abiertas import partidas_bp
from routes.admin_usuarios import permisos_bp
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "supersecreto123")

# Registrar blueprints
app.register_blueprint(proveedores_bp)
app.register_blueprint(login_bp)
app.register_blueprint(partidas_bp)
app.register_blueprint(permisos_bp)
# Redirección si no hay login


if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True, port=7080)

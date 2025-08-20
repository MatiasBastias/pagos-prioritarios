from ldap3 import Server, Connection, ALL
import os
from functools import wraps
from flask import session, redirect, url_for, flash

def autenticar_usuario(usuario, password):
    ldap_server = os.getenv("LDAP_SERVER")
    base_dn = os.getenv("LDAP_BASE_DN")
    user_dn = f"{usuario}@albanesi.com"  # También podrías probar con CN=usuario,{base_dn}

    try:
        server = Server(ldap_server, get_info=ALL)
        conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        return True
    except Exception as e:
        print("❌ Error de autenticación:", e)
        return False




def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("usuario"):
            flash("⚠️ Tenés que iniciar sesión", "warning")
            return redirect(url_for("login_bp.login"))  # adaptá esto a tu blueprint de login
        return f(*args, **kwargs)
    return decorated_function
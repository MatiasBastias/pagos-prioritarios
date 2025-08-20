from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from ldap3 import Server, Connection, NTLM, core
from datetime import datetime
from db import get_connection
import os
import traceback

login_bp = Blueprint("login_bp", __name__)

# Variables de entorno para configuración LDAP
AD_SERVER = os.getenv("LDAP_SERVER", "ldap://albanesi.local")
AD_DOMAIN = os.getenv("LDAP_DOMAIN", "ALBANESI")
SEARCH_BASE = os.getenv("LDAP_SEARCH_BASE", "DC=albanesi,DC=local")

@login_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip().lower()
        clave = request.form.get("clave", "")

        if not usuario or not clave:
            flash("⚠️ Completá usuario y contraseña")
            return render_template("login.html")

        try:
            # Conexión a Active Directory
            server = Server(AD_SERVER, get_info=None)
            conn = Connection(
                server,
                user=f"{AD_DOMAIN}\\{usuario}",
                password=clave,
                authentication=NTLM,
                auto_bind=True
            )

            # Buscar usuario en AD
            conn.search(
                search_base=SEARCH_BASE,
                search_filter=f"(sAMAccountName={usuario})",
                attributes=["cn", "mail", "memberOf"]
            )

            if not conn.entries:
                flash("❌ Usuario no encontrado en el Active Directory")
                return render_template("login.html")

            user_entry = conn.entries[0]
            email = str(user_entry.mail) if "mail" in user_entry else None
            conn.unbind()

            # Validación en base de datos interna
            db = get_connection()
            cur = db.cursor()

            # Verificar si el usuario está dado de alta en la tabla `usuarios`
            cur.execute("SELECT rol_id FROM usuarios WHERE LOWER(usuario) =LOWER(%s)", (usuario,))
            existe = cur.fetchone()

            if not existe:
                flash("⚠️ El usuario está autenticado en AD pero no tiene acceso. Contactá al administrador para darlo de alta.")
                cur.close()
                db.close()
                return render_template("login.html")

            # Obtener el rol del usuario
            cur.execute("""
                SELECT r.nombre
                FROM usuarios u
                JOIN roles r ON u.rol_id = r.id
                WHERE u.usuario = %s
            """, (usuario,))
            rol = cur.fetchone()

            if not rol:
                flash("⚠️ Usuario autenticado, pero sin rol asignado")
                cur.close()
                db.close()
                return render_template("login.html")

            session["usuario"] = usuario
            session["email"] = email
            session["login_time"] = datetime.now().strftime("%d/%m/%Y %H:%M")
            session["rol"] = rol[0]

            # Obtener permisos del rol
            cur.execute("""
                SELECT p.recurso, p.tipo
                FROM usuarios u
                JOIN roles r ON u.rol_id = r.id
                JOIN roles_permisos rp ON r.id = rp.rol_id
                JOIN permisos p ON p.id = rp.permiso_id
                WHERE u.usuario = %s
            """, (usuario,))
            permisos_raw = cur.fetchall()
            permisos = {}
            for recurso, tipo in permisos_raw:
                if tipo not in permisos:
                    permisos[tipo] = set()
                permisos[tipo].add(recurso)
            session["permisos"] = {tipo: list(v) for tipo, v in permisos.items()}

            # Obtener plantas si no es admin
            if rol[0] != "admin":
                cur.execute("""
                    SELECT p.codigo
                    FROM usuario_planta up
                    JOIN usuarios u ON u.id = up.usuario_id
                    JOIN plantas p ON p.id = up.planta_id
                    WHERE u.usuario = %s
                """, (usuario,))
                plantas = [row[0] for row in cur.fetchall()]
                session["plantas"] = plantas
            else:
                session["plantas"] = []  # admin ve todo

            cur.close()
            db.close()

            flash("✅ Login exitoso")
            return redirect(url_for("partidas_bp.mis_prioritarios"))

        except core.exceptions.LDAPBindError:
            flash("❌ Usuario o contraseña incorrectos")
        except Exception:
            traceback.print_exc()
            flash("⚠️ Error inesperado. Contactá a soporte")

    return render_template("login.html")


@login_bp.route("/logout")
def logout():
    session.clear()
    flash("🔒 Sesión cerrada")
    return redirect("/login")

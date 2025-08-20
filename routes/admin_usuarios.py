from flask import Blueprint, render_template, session, request, flash, redirect, url_for
from db import get_connection

permisos_bp = Blueprint("permisos_bp", __name__)

@permisos_bp.route("/admin/usuarios")
def administrar_usuarios():
    if session.get("rol") != "admin":
        return "Acceso no autorizado", 403

    conn = get_connection()
    cur = conn.cursor()

    # Traer usuarios con rol_id, rol, y plantas
    cur.execute("""
        SELECT u.usuario, u.email, r.id AS rol_id, r.nombre AS rol,
               COALESCE(string_agg(p.descripcion, ', '), 'Sin asignar') AS plantas
        FROM usuarios u
        JOIN roles r ON u.rol_id = r.id
        LEFT JOIN usuario_planta up ON u.id = up.usuario_id
        LEFT JOIN plantas p ON up.planta_id = p.id
        GROUP BY u.usuario, u.email, r.id, r.nombre
        ORDER BY u.usuario
    """)
    usuarios = [
        {
            "usuario": row[0],
            "email": row[1],
            "rol_id": row[2],
            "rol": row[3],
            "plantas": row[4]
        }
        for row in cur.fetchall()
    ]

    # Traer plantas disponibles
    cur.execute("SELECT id, descripcion FROM plantas ORDER BY descripcion")
    plantas = cur.fetchall()

    # Traer roles disponibles
    cur.execute("SELECT id, nombre FROM roles ORDER BY nombre")
    roles = cur.fetchall()

    # Traer ids de plantas asignadas por usuario
    for u in usuarios:
        cur.execute("""
            SELECT p.id FROM usuario_planta up
            JOIN usuarios usr ON usr.id = up.usuario_id
            JOIN plantas p ON p.id = up.planta_id
            WHERE usr.usuario = %s
        """, (u["usuario"],))
        u["plantas_ids"] = {row[0] for row in cur.fetchall()}

    cur.close()
    conn.close()
    return render_template("admin/usuarios.html", usuarios=usuarios, plantas=plantas, roles=roles)


@permisos_bp.route("/admin/plantas-usuario/<usuario>", methods=["POST"])
def editar_plantas_usuario(usuario):
    if session.get("rol") != "admin":
        return "Acceso no autorizado", 403

    conn = get_connection()
    cur = conn.cursor()

    rol_id = int(request.form.get("rol_id"))
    usuario_nuevo = request.form.get("usuario").strip()
    email_nuevo = request.form.get("email").strip()
    nuevas_plantas = request.form.getlist("plantas")

    cur.execute("SELECT id FROM usuarios WHERE usuario = %s", (usuario,))
    row = cur.fetchone()
    if not row:
        return "Usuario no encontrado", 404

    usuario_id = row[0]
    # Actualizar el nombre/Mail
    cur.execute("UPDATE usuarios SET usuario = %s, email = %s WHERE id = %s", (usuario_nuevo,email_nuevo, usuario_id))

    # Actualizar el rol
    cur.execute("UPDATE usuarios SET rol_id = %s WHERE id = %s", (rol_id, usuario_id))

    # Limpiar y volver a insertar las plantas solo si no es admin
    cur.execute("DELETE FROM usuario_planta WHERE usuario_id = %s", (usuario_id,))
    cur.execute("SELECT nombre FROM roles WHERE id = %s", (rol_id,))
    rol_nombre = cur.fetchone()[0]

    if rol_nombre == "Usuario":
        for planta_id in nuevas_plantas:
            cur.execute(
                "INSERT INTO usuario_planta (usuario_id, planta_id) VALUES (%s, %s)",
                (usuario_id, planta_id)
            )

    conn.commit()
    cur.close()
    conn.close()

    flash(f"✅ Permisos actualizados para {usuario}", "success")
    return redirect(url_for("permisos_bp.administrar_usuarios"))




@permisos_bp.route("/admin/usuarios/nuevo", methods=["POST"])
def nuevo_usuario():
    if session.get("rol") != "admin":
        return "Acceso no autorizado", 403

    usuario = request.form.get("usuario").strip()
    email = request.form.get("email").strip()
    rol_id = int(request.form.get("rol_id"))
    plantas = request.form.getlist("plantas")

    conn = get_connection()
    cur = conn.cursor()

    # Validar si el usuario ya existe
    cur.execute("SELECT 1 FROM usuarios WHERE usuario = %s", (usuario,))
    if cur.fetchone():
        flash("⚠️ El usuario ya existe.", "warning")
        return redirect(url_for("permisos_bp.administrar_usuarios"))

    # Insertar nuevo usuario
    cur.execute("""
        INSERT INTO usuarios (usuario, email, rol_id)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (usuario, email, rol_id))
    usuario_id = cur.fetchone()[0]

    # Si el rol no es admin, asociar plantas
    cur.execute("SELECT nombre FROM roles WHERE id = %s", (rol_id,))
    rol_nombre = cur.fetchone()[0]
    if rol_nombre.lower() == "usuario":
        for planta_id in plantas:
            cur.execute("INSERT INTO usuario_planta (usuario_id, planta_id) VALUES (%s, %s)", (usuario_id, planta_id))

    conn.commit()
    cur.close()
    conn.close()

    flash(f"✅ Usuario {usuario} creado correctamente.", "success")
    return redirect(url_for("permisos_bp.administrar_usuarios"))

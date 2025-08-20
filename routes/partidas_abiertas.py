from flask import Blueprint, render_template, request, session, redirect, url_for, flash,jsonify
from db import get_connection
from datetime import datetime
from auth_utils import login_required
from io import BytesIO
from datetime import datetime
from flask import send_file
import pandas as pd

partidas_bp = Blueprint("partidas_bp", __name__)


# LISTAMOS TODAS LAS PARTIDAS ABIERTAS , VERIFICAMOS ADEMAS LAS PLANTAS ASOCIADAS A LOS USUARIOS , SI ES ADMIN MUESTRA
@partidas_bp.route("/partidas-abiertas")
@login_required
def listar_partidas():
    query = request.args.get("q", "").strip().lower()
    filtro = request.args.get("filtro")  # 'esenciales' | 'prioritarias' | 'sin_seleccion' | None
    page = int(request.args.get("page", 1))
    per_page = 20
    offset = (page - 1) * per_page

    usuario = session.get("usuario")
    rol = session.get("rol")
    plantas = session.get("plantas", [])

    conn = get_connection()
    cur = conn.cursor()

    # Obtener claves de partidas prioritarias (bukrs, belnr, gjahr)
    cur.execute("SELECT bukrs, belnr, gjahr FROM partidas_prioritarias")
    partidas_prioritarias_claves = {(row[0], row[1], row[2]) for row in cur.fetchall()}

    # Obtener proveedores esenciales
    cur.execute("SELECT lifnr FROM proveedores_esenciales")
    proveedores_esenciales = {row[0] for row in cur.fetchall()}

    # --- Consulta base (SIN 'where' al final) ---
    base_sql = """
       SELECT 
        a.id AS id, 
        a.bukrs, 
        a.lifnr, 
        v.txtmd AS nombre_proveedor,
        a.augdt,
        a.zuonr, 
        a.belnr, 
        a.gjahr,
        a.waers,
        a.budat,
        a.zfbdt,
        a.dmbtr, 
        a.wrbtr,
        a.bldat,
        a.xblnr,
        a.xref1_hd
       FROM vg_fi_acreedores_partidas_abiertas a
       LEFT JOIN (
           SELECT DISTINCT lifnr, proveedor_text AS txtmd 
           FROM vg_fi_acreedores_partidas_abiertas
       ) v ON a.lifnr = v.lifnr
    """

    where_clauses = []
    params = []

    # Filtro por plantas para roles no-admin
    if rol != "admin":
        if not plantas:
            cur.close()
            conn.close()
            return "⚠️ No tenés plantas asignadas para seleccionar pagos prioritarios, contacta al equipo de Pagos", 403
        placeholders = ",".join(["%s"] * len(plantas))
        where_clauses.append(f"a.bukrs IN ({placeholders})")
        params.extend(plantas)

    # Búsqueda libre
    if query:
        where_clauses.append(
            "(LOWER(a.lifnr) LIKE %s OR LOWER(a.bukrs) LIKE %s OR LOWER(a.xblnr) LIKE %s OR LOWER(v.txtmd) LIKE %s)"
        )
        params.extend([f"%{query}%"] * 4)

    # Concatena WHERE solo si hay filtros
    where_clause = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sql = base_sql + where_clause

    cur.execute(sql, params)
    all_partidas = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    all_dicts = [dict(zip(columnas, row)) for row in all_partidas]

    # Totales
    total_prioritarias = sum(1 for p in all_dicts if (p["bukrs"], p["belnr"], p["gjahr"]) in partidas_prioritarias_claves)
    total_esenciales = sum(1 for p in all_dicts if p["lifnr"] in proveedores_esenciales)
    total_sin_seleccion = sum(
        1 for p in all_dicts
        if (p["bukrs"], p["belnr"], p["gjahr"]) not in partidas_prioritarias_claves and p["lifnr"] not in proveedores_esenciales
    )

    # Filtrado según 'filtro'
    if filtro == "prioritarias":
        partidas_filtradas = [p for p in all_dicts if (p["bukrs"], p["belnr"], p["gjahr"]) in partidas_prioritarias_claves]
    elif filtro == "esenciales":
        partidas_filtradas = [p for p in all_dicts if p["lifnr"] in proveedores_esenciales]
    elif filtro == "sin_seleccion":
        partidas_filtradas = [
            p for p in all_dicts
            if (p["bukrs"], p["belnr"], p["gjahr"]) not in partidas_prioritarias_claves and p["lifnr"] not in proveedores_esenciales
        ]
    else:
        partidas_filtradas = all_dicts

    total = len(partidas_filtradas)
    total_pages = (total + per_page - 1) // per_page
    partidas_dict = partidas_filtradas[offset:offset + per_page]

    cur.close()
    conn.close()

    return render_template(
        "partidas/listar_partidas.html",
        partidas=partidas_dict,
        query=query,
        filtro=filtro,
        page=page,
        total_pages=total_pages,
        total=total,
        prioritarias_claves=partidas_prioritarias_claves,
        proveedores_esenciales=proveedores_esenciales,
        total_prioritarias=total_prioritarias,
        total_esenciales=total_esenciales,
        total_sin_seleccion=total_sin_seleccion
    )



# Permite marcar como pago prioritario al usuario . 
@partidas_bp.route("/marcar-prioritaria", methods=["POST"])
@login_required
def marcar_prioritaria():
    usuario = session.get("usuario")
    comentario = request.form.get("comentario", "").strip()
    bukrs = request.form.get("bukrs")
    belnr = request.form.get("belnr")
    gjahr = request.form.get("gjahr")

    if not usuario:
        return "⚠️ No estás logueado", 403

    if not comentario:
        flash("⚠️ Debés ingresar un comentario antes de marcar como prioritaria", "warning")
        return redirect(url_for("partidas_bp.listar_partidas"))

    if not (bukrs and belnr and gjahr):
        flash("❌ Faltan datos clave de la partida (bukrs, belnr, gjahr)", "danger")
        return redirect(url_for("partidas_bp.listar_partidas"))

    conn = get_connection()
    cur = conn.cursor()

    # ✅ Verificar si ya fue marcada como prioritaria
    cur.execute("""
        SELECT 1 FROM partidas_prioritarias
        WHERE bukrs = %s AND belnr = %s AND gjahr = %s
    """, (bukrs, belnr, gjahr))
    if cur.fetchone():
        flash("⚠️ Esta partida ya fue marcada como prioritaria", "warning")
        return redirect(url_for("partidas_bp.listar_partidas"))

    # ✅ Obtener datos de la partida original
    cur.execute("""
        SELECT bukrs, lifnr, augdt, zuonr, belnr, waers,
               budat, bldat, zfbdt, dmbtr, wrbtr, proveedor_text, xblnr
        FROM vg_fi_acreedores_partidas_abiertas
        WHERE bukrs = %s AND belnr = %s AND gjahr = %s
    """, (bukrs, belnr, gjahr))
    partida = cur.fetchone()

    if not partida:
        flash("❌ Partida no encontrada", "danger")
        return redirect(url_for("partidas_bp.listar_partidas"))

    # ✅ Insertar en partidas_prioritarias
    cur.execute("""
        INSERT INTO partidas_prioritarias (
            bukrs, lifnr, augdt, zuonr, belnr, waers,
            budat, bldat, zfbdt, dmbtr, wrbtr,
            proveedor_text, xblnr, gjahr,
            usuario, comentario, id_estado, fecha_marcado
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        RETURNING id_prioritario
    """, (*partida, gjahr, usuario, comentario, 1))  # 1 = ID del estado "Ingresado"

    id_prioritario = cur.fetchone()[0]

    # ✅ Insertar en historial el primer estado
    cur.execute("""
        INSERT INTO historial_pagos_prioritarios (
            id_prioritario, estado_anterior, estado_nuevo, comentario_admin, usuario_admin
        ) VALUES (%s, NULL, %s, %s, %s)
    """, (id_prioritario, 1, comentario, usuario))

    conn.commit()
    cur.close()
    conn.close()

    flash("✅ Partida marcada como prioritaria", "success")
    return redirect(url_for("partidas_bp.listar_partidas"))


# Puede editar comentario mientras el estado del pago sea "INGRESADO"
@partidas_bp.route("/editar-prioritario/<int:id>", methods=["POST"])
@login_required
def editar_prioritario(id):
    usuario = session.get("usuario")
    if not usuario:
        return "⚠️ No estás logueado", 403

    comentario = request.form.get("comentario", "").strip()
    if not comentario:
        flash("⚠️ Debés ingresar un comentario", "warning")
        return redirect(url_for("partidas_bp.mis_prioritarios"))

    conn = get_connection()
    cur = conn.cursor()

    # Verificar que el pago sea del usuario, esté en estado ingresado (id_estado = 1)
    cur.execute("""
        SELECT 1 FROM partidas_prioritarias
        WHERE id_prioritario = %s AND usuario = %s AND id_estado = 1
    """, (id, usuario))
    existe = cur.fetchone()

    if not existe:
        cur.close()
        conn.close()
        flash("❌ No podés editar este pago (no te pertenece o ya cambió de estado)", "danger")
        return redirect(url_for("partidas_bp.mis_prioritarios"))

    # Actualizar el comentario
    cur.execute("""
        UPDATE partidas_prioritarias
        SET comentario = %s
        WHERE id_prioritario = %s
    """, (comentario, id))

    conn.commit()
    cur.close()
    conn.close()

    flash("✅ Comentario actualizado correctamente", "success")
    return redirect(url_for("partidas_bp.mis_prioritarios"))



# Elimina Pago prioritario 
@partidas_bp.route("/eliminar-prioritario/<int:id>", methods=["POST"])
def eliminar_prioritario(id):
    usuario = session.get("usuario")
    if not usuario:
        return "⚠️ No estás logueado", 403

    conn = get_connection()
    cur = conn.cursor()

    # Verificar que el pago pertenezca al usuario y esté en estado "Ingresado"
    cur.execute("""
        SELECT 1 FROM partidas_prioritarias
        WHERE id_prioritario = %s AND usuario = %s 
    """, (id, usuario))
    existe = cur.fetchone()

    if not existe:
        cur.close()
        conn.close()
        flash("❌ No podés eliminar este pago (no te pertenece o ya cambió de estado)", "danger")
        return redirect(url_for("partidas_bp.mis_prioritarios"))

    # Eliminar el pago
    cur.execute("DELETE FROM historial_pagos_prioritarios WHERE id_prioritario = %s", (id,))
    cur.execute("DELETE FROM partidas_prioritarias WHERE id_prioritario = %s", (id,))
    conn.commit()
    cur.close()
    conn.close()

    flash("✅ Pago prioritario eliminado correctamente", "success")
    return redirect(url_for("partidas_bp.mis_prioritarios"))



# listado de pagos prioritarios del usuario logeado 
@partidas_bp.route("/")
@partidas_bp.route("/mis-prioritarios")
@login_required
def mis_prioritarios():
    usuario = session.get("usuario")
    estado_filtro = request.args.get("estado")  # Ahora es el ID del estado


    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT pp.id_prioritario,pp.bukrs, pp.lifnr, pp.augdt, pp.zuonr, pp.belnr, pp.waers,
               pp.budat, pp.bldat, pp.zfbdt, pp.dmbtr, pp.wrbtr,TO_CHAR(pp.fecha_pago, 'DD/MM/YYYY') AS fecha_pago,TO_CHAR(pp.fecha_marcado, 'DD/MM/YYYY HH24:MI') AS fecha_marcado,
               pp.usuario,ep.id_estado, ep.nombre AS estado,ep.color,
               pp.comentario, pp.comentario_admin, pp.proveedor_text
        FROM partidas_prioritarias pp
        LEFT JOIN estados_pagos_prioritarios ep ON pp.id_estado = ep.id_estado
        WHERE pp.usuario = %s
    """
    params = [usuario]

    if estado_filtro:
        query += " AND pp.id_estado = %s"
        params.append(estado_filtro)

    query += " ORDER BY pp.id_prioritario DESC"

    cur.execute(query, params)
    rows = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    partidas = [dict(zip(columnas, r)) for r in rows]

    # También cargamos todos los estados disponibles para usar en filtros (select en HTML)
    cur.execute("SELECT id_estado, nombre FROM estados_pagos_prioritarios ORDER BY orden")
    estados = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "partidas/mis_prioritarios.html",
        partidas=partidas,
        estado_filtro=estado_filtro,
        estados=estados
    )



# Listado de pagos desde seccion de administracion para poder realizar cambios de estados.
@partidas_bp.route("/admin/pagos-prioritarios")
def ver_pagos_prioritarios_admin():
    if session.get("rol") != "admin":
        return "⚠️ Acceso no autorizado", 403

    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=25, type=int)
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    # 🔍 Traer pagos con paginación
    cur.execute("""
        SELECT pp.id_prioritario, pp.xblnr,pp.bukrs, pp.lifnr, pp.augdt, pp.zuonr, pp.belnr, pp.waers,
               TO_CHAR(pp.fecha_marcado, 'DD/MM/YYYY HH24:MI') AS fecha_marcado,
               pp.budat, pp.bldat, pp.zfbdt, pp.dmbtr, pp.wrbtr, pp.usuario,
               ep.nombre AS estado, ep.color, pp.fecha_pago,
               TO_CHAR(pp.fecha_pago, 'DD/MM/YYYY') AS fecha_pago_to,
               pp.comentario, pp.comentario_admin, pp.proveedor_text
        FROM partidas_prioritarias pp
        LEFT JOIN estados_pagos_prioritarios ep ON pp.id_estado = ep.id_estado
        ORDER BY pp.fecha_alta DESC
        LIMIT %s OFFSET %s
    """, (per_page, offset))

    pagos = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    pagos_dict = [dict(zip(columnas, fila)) for fila in pagos]

    # ✅ Agregar historial a cada pago
    for p in pagos_dict:
        id_prioritario = p["id_prioritario"]

        cur.execute("""
            SELECT h.fecha_cambio, ea.nombre AS estado_anterior, en.nombre AS estado_nuevo,
                   h.usuario_admin, h.comentario_admin
            FROM historial_pagos_prioritarios h
            LEFT JOIN estados_pagos_prioritarios ea ON ea.id_estado = h.estado_anterior
            LEFT JOIN estados_pagos_prioritarios en ON en.id_estado = h.estado_nuevo
            WHERE h.id_prioritario = %s
            ORDER BY h.fecha_cambio DESC
        """, (id_prioritario,))
        historial = cur.fetchall()

        # Guardamos el historial dentro del diccionario del pago
        p["historial"] = historial

    # 🔍 Traer lista de estados
    cur.execute("SELECT id_estado, nombre, color FROM estados_pagos_prioritarios ORDER BY orden")
    estados_disponibles = cur.fetchall()

    # 🧮 Contar pagos por estado
    estado_counts = {}
    for p in pagos_dict:
        estado = p.get("estado", "Sin Estado")
        estado_counts[estado] = estado_counts.get(estado, 0) + 1


        # 🧮 Total ML (dmbtr) para estados Ingresado (1) y A pagar (2)
    cur.execute("""
        SELECT COALESCE(SUM(dmbtr), 0)
        FROM partidas_prioritarias
        WHERE id_estado IN (1, 2)
    """)
    total_ml = cur.fetchone()[0]
    

    # 🔍 Total de registros para paginación
    cur.execute("SELECT COUNT(*) FROM partidas_prioritarias")
    total_items = cur.fetchone()[0]
    total_pages = (total_items + per_page - 1) // per_page

    cur.close()
    conn.close()

    return render_template(
        "partidas/admin_listado_prioritarios.html",
        pagos=pagos_dict,
        estado_counts=estado_counts,
        estados=estados_disponibles,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        total_ml=total_ml
    )


# o ajustá según cómo tengas importado esto

import requests
# ADMINISTRADORES actualizan estados de pagos prioritarios  y geneneran alertas QUE SON ENVIADAS POR TEAMS 

@partidas_bp.route("/actualizar-estado-pago", methods=["POST"])
def actualizar_estado_pago():
    usuario = session.get("usuario")
    if not usuario or session.get("rol") != "admin":
        return "❌ Acceso no autorizado", 403

    partida_id = request.form.get("id_prioritario")
    estado = request.form.get("estado")
    fecha_pago = request.form.get("fecha_pago")
    comentario_admin = request.form.get("comentario_admin")

    conn = get_connection()
    cur = conn.cursor()

    # ✅ Traer datos completos de la partida
    cur.execute("""
        SELECT id_estado, bukrs, lifnr, dmbtr, comentario, usuario
        FROM partidas_prioritarias
        WHERE id_prioritario = %s
    """, (partida_id,))
    row = cur.fetchone()
    if not row:
        flash("❌ Partida no encontrada", "danger")
        return redirect(url_for("partidas_bp.ver_pagos_prioritarios_admin"))

    estado_anterior, bukrs, lifnr, dmbtr, comentario_usuario, usuario_destino = row

    # ✅ Ajustar fecha según el nuevo estado
    fecha_pago_final = fecha_pago if int(estado) in (2,3)  else None

    # ✅ Actualizar la partida
    cur.execute("""
        UPDATE partidas_prioritarias
        SET id_estado = %s,
            fecha_pago = %s,
            comentario_admin = %s
        WHERE id_prioritario = %s
    """, (estado, fecha_pago_final, comentario_admin, partida_id))

    # ✅ Guardar en historial
    cur.execute("""
        INSERT INTO historial_pagos_prioritarios (id_prioritario, estado_anterior, estado_nuevo, comentario_admin, usuario_admin)
        VALUES (%s, %s, %s, %s, %s)
    """, (partida_id, estado_anterior, estado, comentario_admin, usuario))

    # ✅ Obtener el nombre del nuevo estado
    cur.execute("SELECT nombre FROM estados_pagos_prioritarios WHERE id_estado = %s", (estado,))
    estado_row = cur.fetchone()
    estado_nombre = estado_row[0] if estado_row else "Desconocido"

    # ✅ Obtener el correo del usuario destino
    cur.execute("SELECT email FROM usuarios WHERE usuario = %s", (usuario_destino,))
    email_row = cur.fetchone()
    email_destino = email_row[0] if email_row else None

    conn.commit()
    cur.close()
    conn.close()

    # ✅ Notificación a Power Automate
    try:
        webhook_url = "https://prod-121.westus.logic.azure.com:443/workflows/57441cfbd59848a1bb451c7bddda7c62/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=FPBNIKGBU2WdBQ0qPI7fU64W0OTcRTCZ3bAJyWW-0Sc"
        payload = {
            "usuario": usuario_destino,
            "correo": email_destino,
            "mensaje": (
                f"<b>🔔 Pago prioritario actualizado</b><br><br>"
                f"<b>📌 ID:</b> #{partida_id}<br>"
                f"<b>📅 Nuevo estado:</b> {estado_nombre}<br><br>"
                f"<hr>"
                f"<b>📝 Detalles del pago:</b><br>"
                f"<ul>"
                f"<li><b>💬 Comentario admin:</b> {comentario_admin or '-'}</li>"
                f"<li><b>💬 Comentario usuario:</b> {comentario_usuario or '-'}</li>"
                f"<li><b>👤 Usuario:</b> {usuario_destino}</li>"
                f"<li><b>💲 Importe:</b> ${dmbtr:,.2f}</li>"
                f"<li><b>🏢 Sociedad:</b> {bukrs}</li>"
                f"<li><b>🧾 Acreedor:</b> {lifnr}</li>"
                f"<li><b>📆 Fecha de pago:</b> {fecha_pago or '-'}</li>"
                f"</ul>"
            )
        }
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        print("⚠️ Error al enviar notificación:", str(e))

    flash("✅ Estado del pago actualizado correctamente", "success")
    return redirect(url_for("partidas_bp.ver_pagos_prioritarios_admin"))





from xlsxwriter.utility import xl_col_to_name

@partidas_bp.route("/admin/pagos-prioritarios/export", methods=["GET"])
@login_required
def export_pagos_prioritarios_admin():
    if session.get("rol") != "admin":
        return "⚠️ Acceso no autorizado", 403

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            pp.bukrs as Soc, pp.lifnr as nro_prov,pp.xblnr as nro_ref, pp.proveedor_text as NombreProveedor,
            pp.belnr doc_contable , pp.gjahr periodo , pp.waers Moneda,
            pp.dmbtr ML, pp.wrbtr ME,
                pp.budat AS f_contabilizacion,
                pp.bldat AS f_documento,
               pp.zfbdt AS f_vto_neto,
                pp.usuario,
            TO_CHAR(pp.fecha_marcado, 'DD/MM/YYYY HH24:MI') AS fecha_marcado,
            ep.nombre AS estado, 
            TO_CHAR(pp.fecha_pago, 'DD/MM/YYYY') AS fecha_pago_to,
            pp.comentario, pp.comentario_admin
        FROM partidas_prioritarias pp
        LEFT JOIN estados_pagos_prioritarios ep ON pp.id_estado = ep.id_estado
        ORDER BY pp.fecha_alta DESC
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close(); conn.close()

    df = pd.DataFrame(rows, columns=cols)

    # 👇 Forzar numérico (si venía como texto/objeto no suma en Excel)
    for c in ("ml", "me"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Pagos")
        wb = writer.book
        ws = writer.sheets["Pagos"]

        fmt_money = wb.add_format({"num_format": "#,##0.00"})
        fmt_date  = wb.add_format({"num_format": "dd/mm/yyyy"})

        # Anchos + formato de columnas numéricas
        for i, col in enumerate(df.columns):
            width = max(12, min(50, df[col].astype(str).map(len).max() if not df.empty else 12))
            ws.set_column(i, i, width)
        for c in ("ml", "me"):
            if c in df.columns:
                col_idx = df.columns.get_loc(c)
                ws.set_column(col_idx, col_idx, 14, fmt_money)

        # 🧮 SUM al final de dmbtr
        if "ml" in df.columns and len(df) > 0:
            col_idx = df.columns.get_loc("ml")           # 0-based
            col_letter = xl_col_to_name(col_idx)            # 'A'..'AA' seguro
            last_data_row_excel = len(df) + 1               # fila Excel donde termina la data (incluye header en 1)
            # escribir fórmula en la fila siguiente a la data (0-based -> +1)
            ws.write_formula(last_data_row_excel + 1, col_idx,
                             f"=SUM({col_letter}2:{col_letter}{last_data_row_excel})",
                             fmt_money)

    output.seek(0)
    fname = f"pagos_prioritarios_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(output, as_attachment=True, download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

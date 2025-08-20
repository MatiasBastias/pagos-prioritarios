from flask import Blueprint, render_template, request, redirect, url_for, flash,session
import psycopg2
import os
from dotenv import load_dotenv
from auth_utils import login_required
import pandas as pd
from io import BytesIO
from flask import send_file
load_dotenv()

proveedores_bp = Blueprint('proveedores_bp', __name__)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

@proveedores_bp.route("/proveedores", methods=["GET"])
@login_required
def listar_proveedores():
    query = request.args.get("q", "").strip().lower()
    page = int(request.args.get("page", 1))
    per_page = 20
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    base_sql = """
        SELECT v.lifnr, v.txtmd,
               CASE WHEN e.lifnr IS NOT NULL THEN 'Sí' ELSE 'No' END AS esencial
        FROM v_md_vendor v
        LEFT JOIN proveedores_esenciales e ON v.lifnr = e.lifnr
    """

    count_sql = "SELECT COUNT(*) FROM v_md_vendor v"

    where_clause = ""
    params = []

    if query:
        where_clause = "WHERE LOWER(v.lifnr) LIKE %s OR LOWER(v.txtmd) LIKE %s"
        params.extend([f"%{query}%", f"%{query}%"])
        count_sql += " " + where_clause

    full_sql = f"{base_sql} {where_clause} ORDER BY v.txtmd LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    cur.execute(full_sql, params)
    proveedores = cur.fetchall()

    cur.execute(count_sql, params[:2] if query else [])
    total = cur.fetchone()[0]

    cur.close()
    conn.close()

    total_pages = (total + per_page - 1) // per_page

    return render_template(
        "proveedores.html",
        proveedores=proveedores,
        query=query,
        page=page,
        total_pages=total_pages
    )



@proveedores_bp.route("/marcar_esencial", methods=["POST"])
def marcar_esencial():
    lifnr = request.form.get("lifnr")
    proveedor=request.form.get("nombre_proveedor")
    motivo = request.form.get("motivo")
    usuario = session.get("usuario")


    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO proveedores_esenciales (lifnr, usuario, motivo,proveedor_text)
            VALUES (%s, %s, %s,%s)
        """, (lifnr, usuario, motivo,proveedor))
        conn.commit()
        flash("Proveedor marcado como esencial.", "success")
    except Exception as e:
        flash(f"Error al marcar proveedor: {e}", "danger")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("proveedores_bp.listar_proveedores"))






@proveedores_bp.route("/partidas_abiertas/<lifnr>")
def partidas_abiertas(lifnr):
    conn = get_connection()
    cur = conn.cursor()

    # Traer las partidas
    cur.execute("""
        SELECT gjahr, belnr, xblnr, dmbtr, wrbtr, budat, bldat, cpudt, ZFBDT
        FROM vg_fi_acreedores_partidas_abiertas
        WHERE lifnr = %s
        ORDER BY gjahr DESC, belnr DESC
    """, (lifnr,))
    partidas = cur.fetchall()

    # Calcular métricas resumen
    cur.execute("""
        SELECT
            COALESCE(SUM(dmbtr), 0) AS total,
            COUNT(DISTINCT belnr) AS cantidad_docs,
            ROUND(COALESCE(AVG(dmbtr), 0), 2) AS promedio
        FROM vg_fi_acreedores_partidas_abiertas
        WHERE lifnr = %s
    """, (lifnr,))
    resumen = cur.fetchone()

    cur.close()
    conn.close()

    return render_template(
        "partidas/partidas_por_proveedor.html",  # ⚠️ nuevo template dedicado
        partidas=partidas,
        lifnr=lifnr,
        total=resumen[0],
        cantidad=resumen[1],
        promedio=resumen[2]
    )

@proveedores_bp.route("/reportes", endpoint="reportes")
def reportes():
    conn = get_connection()
    cur = conn.cursor()

    # 1. Total pagos prioritarios
    cur.execute("SELECT COUNT(*) FROM partidas_prioritarias;")
    total_prioritarios = cur.fetchone()[0]

    # 2. Totales por estado
    cur.execute("""
        SELECT ep.nombre, COUNT(*) 
        FROM partidas_prioritarias pp
        JOIN estados_pagos_prioritarios ep ON ep.id_estado = pp.id_estado
        GROUP BY ep.nombre
    """)
    estados = cur.fetchall()  # [(Pendiente, 10), (En Proceso, 5), (Completado, 8)]

    # 3. Gráfico: Partidas por semana y estado
    cur.execute("""
    SELECT 
        TO_CHAR(DATE_TRUNC('week', fecha_alta), 'DD/MM') || ' - ' ||
        TO_CHAR(DATE_TRUNC('week', fecha_alta) + interval '6 days', 'DD/MM') AS rango_semana,
        ep.nombre,
        COUNT(*) AS total
    FROM partidas_prioritarias pp
    JOIN estados_pagos_prioritarios ep ON ep.id_estado = pp.id_estado
    WHERE fecha_alta IS NOT NULL
    GROUP BY rango_semana, ep.nombre, DATE_TRUNC('week', fecha_alta)
    ORDER BY DATE_TRUNC('week', fecha_alta);
""")
    partidas_por_semana = cur.fetchall()


    # 4. Top 5 usuarios con más pedidos
    cur.execute("""
        SELECT usuario, COUNT(*) 
        FROM partidas_prioritarias
        GROUP BY usuario
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    top_usuarios = cur.fetchall()

    
    cur.close()
    conn.close()

    return render_template(
        "reportes.html",
        total_prioritarios=total_prioritarios,
        estados=estados,
        partidas_por_semana=partidas_por_semana,
        top_usuarios=top_usuarios
    )



@proveedores_bp.route("/admin/proveedores-esenciales")
def admin_proveedores_esenciales():
    if session.get("rol") != "admin":
        return "⚠️ Acceso no autorizado", 403

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT lifnr, proveedor_text, usuario, motivo
        FROM proveedores_esenciales
        ORDER BY proveedor_text
    """)
    proveedores = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    proveedores_dict = [dict(zip(columnas, fila)) for fila in proveedores]

    cur.close()
    conn.close()

    return render_template("proveedores/admin_proveedores_esenciales.html", proveedores=proveedores_dict)



@proveedores_bp.route("/admin/desmarcar_esencial", methods=["POST"])
def admin_desmarcar_esencial():
    lifnr = request.form.get("lifnr")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM proveedores_esenciales WHERE lifnr = %s", (lifnr,))
        conn.commit()
        flash("Proveedor desmarcado como esencial.", "warning")
    except Exception as e:
        flash(f"Error al quitar proveedor: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("proveedores_bp.admin_proveedores_esenciales"))


@proveedores_bp.route("/mis-proveedores-esenciales")
@login_required
def mis_proveedores_esenciales():
    usuario = session.get("usuario")

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT lifnr, proveedor_text, motivo
        FROM proveedores_esenciales
        WHERE usuario = %s
        ORDER BY proveedor_text
    """, (usuario,))
    proveedores = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("proveedores/mis_proveedores_esenciales.html", proveedores=proveedores)

@proveedores_bp.route("/desmarcar_esencial", methods=["POST"])
def desmarcar_esencial():
    lifnr = request.form.get("lifnr")
    usuario = session.get("usuario")
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM proveedores_esenciales WHERE lifnr = %s", (lifnr,))
        cur.execute("""
        SELECT lifnr, proveedor_text, motivo
        FROM proveedores_esenciales
        WHERE usuario = %s
        ORDER BY proveedor_text
    """, (usuario,))
        proveedores = cur.fetchall()
        conn.commit()
        flash("Proveedor desmarcado como esencial.", "warning")
    except Exception as e:
        flash(f"Error al quitar proveedor: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return render_template("proveedores/mis_proveedores_esenciales.html", proveedores=proveedores)








@proveedores_bp.route("/exportar_proveedores_excel")
def exportar_proveedores_excel():
    if session.get("rol") != "admin":
        return "⚠️ Acceso no autorizado", 403

    conn = get_connection()
    cur = conn.cursor()

    # Traer proveedores esenciales
    cur.execute("""
        SELECT lifnr, proveedor_text, usuario, motivo
        FROM proveedores_esenciales
        ORDER BY proveedor_text
    """)
    proveedores = cur.fetchall()
    columnas_proveedores = [desc[0] for desc in cur.description]

    data_final = []

    for prov in proveedores:
        lifnr, proveedor_text, usuario, motivo = prov
        # Traer partidas para este proveedor
        cur.execute("""
            SELECT gjahr, belnr, buzei, dmbtr, wrbtr, budat, bldat, cpudt, zuonr
            FROM vg_fi_acreedores_partidas_abiertas
            WHERE lifnr = %s
            ORDER BY gjahr DESC, belnr DESC
        """, (lifnr,))
        partidas = cur.fetchall()

        for p in partidas:
            row = {
                "Proveedor Nº": lifnr,
                "Razon Social": proveedor_text,
                "Usuario": usuario,
                "Motivo": motivo,
                "Periodo": p[0],
                "Nro Sap": p[1],
                "Posicion": p[2],
                "Importa ML": p[3],
                "Importa MD": p[4],
                "Fecha Cont": p[5],
                "Fecha Doc": p[6],
                
            }
            data_final.append(row)

    cur.close()
    conn.close()

    df = pd.DataFrame(data_final)

    # Crear Excel en memoria
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Proveedores_Partidas", index=False)

    output.seek(0)

    return send_file(
        output,
        download_name="Proveedores_Partidas.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

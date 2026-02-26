"""
===========================================================================
 SISTEMA CRUD DE INVENTARIO — Aplicación Completa (Flask + SQLite)
 Incluye: Limpieza de datos, CRUD, Dashboard de KPIs, Auto-init de BD
===========================================================================
"""

from flask import Flask, render_template, request, redirect, url_for, jsonify
import sqlite3
import pandas as pd
import re
import os

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, 'inventario.db')
EXCEL_FILE = os.path.join(BASE_DIR, 'sistema crud.xlsx')


# ---------------------------------------------------------------------------
# Utilidades de conexión
# ---------------------------------------------------------------------------
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Lógica de Limpieza de Datos
# ---------------------------------------------------------------------------

def estandarizar_categoria(cat):
    """Normaliza variaciones de texto en la columna Categoría."""
    if not cat or str(cat).strip().lower() in ('nan', 'none', ''):
        return 'Sin Categoría'
    c = str(cat).strip().lower()
    if c.startswith('mob') or c.startswith('mobi'):
        return 'Mobiliario'
    if c.startswith('elec') or c.startswith('élec'):
        return 'Electrónica'
    if c.startswith('pap') or c.startswith('ofic'):
        return 'Papelería'
    if c.startswith('limp') or c.startswith('aseo'):
        return 'Limpieza'
    return cat.strip().title()


# Diccionario de palabras clave del producto → categoría
PRODUCTO_CATEGORIA_MAP = {
    'Electrónica': [
        'laptop', 'mouse', 'teclado', 'monitor', 'impresora', 'disco duro',
        'memoria', 'cámara', 'camara', 'audífono', 'audifono', 'auricular',
        'tablet', 'celular', 'teléfono', 'telefono', 'bocina', 'parlante',
        'cable', 'usb', 'router', 'switch', 'servidor', 'proyector',
        'escáner', 'escaner', 'batería', 'bateria', 'cargador', 'adaptador',
        'pantalla', 'cpu', 'procesador', 'tarjeta', 'hub', 'docking',
    ],
    'Mobiliario': [
        'silla', 'escritorio', 'mesa', 'estante', 'archivero', 'librero',
        'sofá', 'sofa', 'gabinete', 'mueble', 'anaquel', 'repisa',
        'banco', 'banca', 'cajonera', 'vitrina', 'credenza',
    ],
    'Papelería': [
        'papel', 'folder', 'carpeta', 'pluma', 'lápiz', 'lapiz',
        'engrapadora', 'grapa', 'clip', 'sobre', 'cuaderno', 'agenda',
        'cartulina', 'cinta', 'pegamento', 'tijera', 'marcador', 'post-it',
    ],
    'Limpieza': [
        'jabón', 'jabon', 'detergente', 'escoba', 'trapeador', 'cloro',
        'desinfectante', 'toalla', 'papel higiénico', 'bolsa basura',
        'aromatizante', 'franela', 'guante', 'cubeta',
    ],
}


def auto_categorizar(categoria, nombre_producto):
    """Si la categoría es 'Sin Categoría', deduce la correcta del producto."""
    if categoria != 'Sin Categoría':
        return categoria
    producto = str(nombre_producto).lower()
    for cat, keywords in PRODUCTO_CATEGORIA_MAP.items():
        for kw in keywords:
            if kw in producto:
                return cat
    return 'Sin Categoría'


# ---------------------------------------------------------------------------
# Inicialización automática de la base de datos
# ---------------------------------------------------------------------------
def init_db():
    """
    Lee el archivo Excel, aplica TODA la limpieza de datos
    y crea la base de datos SQLite. Se ejecuta automáticamente
    si el archivo inventario.db no existe al iniciar la app.
    """
    if not os.path.exists(EXCEL_FILE):
        # Si no hay Excel, crear tabla vacía
        conn = sqlite3.connect(DB_FILE)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS inventario (
                ID_Movimiento TEXT PRIMARY KEY,
                Fecha TEXT,
                Nombre_Producto TEXT,
                Categoria TEXT,
                Tipo_Movimiento TEXT,
                Cantidad INTEGER,
                Precio_Unitario REAL,
                Vendedor TEXT
            )
        ''')
        conn.commit()
        conn.close()
        print("✅ Base de datos creada (tabla vacía, sin Excel disponible).")
        return

    print(f"📖 Leyendo {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)

    # 1. Eliminar filas completamente vacías
    df.dropna(how='all', inplace=True)

    # 2. Limpiar espacios extra en columnas de texto
    string_cols = ['ID_Movimiento', 'Nombre_Producto', 'Categoria',
                   'Tipo_Movimiento', 'Vendedor']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # --- REGLA A: Categoría (Estandarización + Auto-categorización) ---
    if 'Categoria' in df.columns:
        df['Categoria'] = df['Categoria'].apply(estandarizar_categoria)
        if 'Nombre_Producto' in df.columns:
            df['Categoria'] = df.apply(
                lambda row: auto_categorizar(row['Categoria'],
                                             row['Nombre_Producto']),
                axis=1
            )

    # --- REGLA B: Tipo de Movimiento ---
    if 'Tipo_Movimiento' in df.columns:
        df['Tipo_Movimiento'] = df['Tipo_Movimiento'].replace(
            {'Entrd': 'Entrada', 'Slaida': 'Salida'}
        )

    # --- REGLA C: Cantidad (Absoluto y sin nulos) ---
    if 'Cantidad' in df.columns:
        df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce') \
                           .fillna(0).astype(int).abs()

    # --- REGLA D: Precio Unitario (Quitar '$' y limpiar) ---
    if 'Precio_Unitario' in df.columns:
        df['Precio_Unitario'] = df['Precio_Unitario'].astype(str) \
            .str.replace('$', '', regex=False) \
            .str.replace(',', '', regex=False)
        df['Precio_Unitario'] = pd.to_numeric(
            df['Precio_Unitario'], errors='coerce'
        ).fillna(0.0).astype(float)

    # --- REGLA E: Fechas (Estandarización YYYY-MM-DD) ---
    if 'Fecha' in df.columns:
        meses_es = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11',
            'diciembre': '12',
        }

        def parse_date(date_val):
            date_str = str(date_val).strip().lower()
            if date_str in ('nan', 'nat', 'none', ''):
                return pd.NaT
            match = re.search(r'(\d{1,2})\s+de\s+([a-z]+)', date_str)
            if match:
                day = match.group(1).zfill(2)
                month = meses_es.get(match.group(2), '01')
                return f"2026-{month}-{day}"
            return date_val

        df['Fecha'] = df['Fecha'].apply(parse_date)
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce') \
                        .dt.strftime('%Y-%m-%d')

    # 3. Guardar en SQLite
    conn = sqlite3.connect(DB_FILE)
    df.to_sql('inventario', conn, if_exists='replace', index=False)
    conn.close()
    print(f"✅ Base de datos creada con {len(df)} registros limpios.")


# === Ejecutar auto-init si la BD no existe ===
if not os.path.exists(DB_FILE):
    init_db()


# ---------------------------------------------------------------------------
# CRUD Routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    conn = get_db_connection()
    items = conn.execute('SELECT * FROM inventario').fetchall()
    categorias = [r[0] for r in conn.execute(
        'SELECT DISTINCT Categoria FROM inventario ORDER BY Categoria'
    ).fetchall()]
    tipos = [r[0] for r in conn.execute(
        'SELECT DISTINCT Tipo_Movimiento FROM inventario ORDER BY Tipo_Movimiento'
    ).fetchall()]
    conn.close()
    return render_template('index.html', items=items,
                           categorias=categorias, tipos=tipos)


@app.route('/create', methods=('GET', 'POST'))
def create():
    if request.method == 'POST':
        id_mov   = request.form['ID_Movimiento']
        fecha    = request.form['Fecha']
        producto = request.form['Nombre_Producto']
        categoria = estandarizar_categoria(request.form['Categoria'])
        categoria = auto_categorizar(categoria, producto)
        tipo     = request.form['Tipo_Movimiento']
        cantidad = request.form['Cantidad']
        precio   = request.form['Precio_Unitario']
        vendedor = request.form['Vendedor']

        conn = get_db_connection()
        conn.execute(
            '''INSERT INTO inventario
               (ID_Movimiento, Fecha, Nombre_Producto, Categoria,
                Tipo_Movimiento, Cantidad, Precio_Unitario, Vendedor)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (id_mov, fecha, producto, categoria, tipo,
             cantidad, precio, vendedor)
        )
        conn.commit()
        conn.close()
        return redirect(url_for('index'))

    return render_template('create.html')


@app.route('/<string:id_mov>/edit', methods=('GET', 'POST'))
def edit(id_mov):
    conn = get_db_connection()
    item = conn.execute(
        'SELECT * FROM inventario WHERE ID_Movimiento = ?', (id_mov,)
    ).fetchone()

    if request.method == 'POST':
        fecha    = request.form['Fecha']
        producto = request.form['Nombre_Producto']
        categoria = estandarizar_categoria(request.form['Categoria'])
        categoria = auto_categorizar(categoria, producto)
        tipo     = request.form['Tipo_Movimiento']
        cantidad = request.form['Cantidad']
        precio   = request.form['Precio_Unitario']
        vendedor = request.form['Vendedor']

        conn.execute(
            '''UPDATE inventario SET Fecha=?, Nombre_Producto=?, Categoria=?,
               Tipo_Movimiento=?, Cantidad=?, Precio_Unitario=?, Vendedor=?
               WHERE ID_Movimiento=?''',
            (fecha, producto, categoria, tipo, cantidad,
             precio, vendedor, id_mov)
        )
        conn.commit()
        conn.close()
        return redirect(url_for('index'))

    conn.close()
    return render_template('edit.html', item=item)


@app.route('/<string:id_mov>/delete', methods=('POST',))
def delete(id_mov):
    conn = get_db_connection()
    conn.execute(
        'DELETE FROM inventario WHERE ID_Movimiento = ?', (id_mov,)
    )
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


# ---------------------------------------------------------------------------
# Dashboard Routes
# ---------------------------------------------------------------------------

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


@app.route('/api/dashboard')
def api_dashboard():
    conn = get_db_connection()

    # ---- Parámetros de filtro ----
    fecha_inicio = request.args.get('fecha_inicio', '')
    fecha_fin    = request.args.get('fecha_fin', '')
    categoria    = request.args.get('categoria', '')
    vendedor     = request.args.get('vendedor', '')

    # ---- WHERE dinámico ----
    conditions = []
    params = []
    if fecha_inicio:
        conditions.append("Fecha >= ?");  params.append(fecha_inicio)
    if fecha_fin:
        conditions.append("Fecha <= ?");  params.append(fecha_fin)
    if categoria:
        conditions.append("Categoria = ?");  params.append(categoria)
    if vendedor:
        conditions.append("Vendedor = ?");  params.append(vendedor)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    # ---- KPIs ----
    sales_cond = (where + " AND Tipo_Movimiento = 'Salida'") \
        if where else " WHERE Tipo_Movimiento = 'Salida'"

    ingresos_totales = conn.execute(
        f"SELECT COALESCE(SUM(Cantidad * Precio_Unitario), 0) "
        f"FROM inventario{sales_cond}", params
    ).fetchone()[0]

    total_articulos = conn.execute(
        f"SELECT COALESCE(SUM(Cantidad), 0) FROM inventario{where}", params
    ).fetchone()[0]

    total_transacciones = conn.execute(
        f"SELECT COUNT(ID_Movimiento) FROM inventario{where}", params
    ).fetchone()[0]

    # ---- Gráfico 1: Ingresos por Vendedor ----
    rows = conn.execute(
        f"SELECT Vendedor, SUM(Cantidad * Precio_Unitario) as Total "
        f"FROM inventario{sales_cond} GROUP BY Vendedor ORDER BY Total DESC",
        params
    ).fetchall()
    ingresos_vendedor = [{"vendedor": r[0], "total": round(r[1], 2)}
                         for r in rows]

    # ---- Gráfico 2: Top 5 Categorías más vendidas ----
    rows = conn.execute(
        f"SELECT Categoria, SUM(Cantidad) as Total "
        f"FROM inventario{sales_cond} GROUP BY Categoria "
        f"ORDER BY Total DESC LIMIT 5", params
    ).fetchall()
    top_categorias = [{"categoria": r[0], "total": int(r[1])} for r in rows]

    # ---- Gráfico 3: Tendencia de ingresos en el tiempo ----
    rows = conn.execute(
        f"SELECT Fecha, SUM(Cantidad * Precio_Unitario) as Total "
        f"FROM inventario{sales_cond} GROUP BY Fecha ORDER BY Fecha ASC",
        params
    ).fetchall()
    tendencia = [{"fecha": r[0], "total": round(r[1], 2)} for r in rows]

    # ---- Listas para filtros ----
    categorias = [r[0] for r in conn.execute(
        "SELECT DISTINCT Categoria FROM inventario ORDER BY Categoria"
    ).fetchall()]
    vendedores = [r[0] for r in conn.execute(
        "SELECT DISTINCT Vendedor FROM inventario ORDER BY Vendedor"
    ).fetchall()]

    conn.close()

    return jsonify({
        "kpis": {
            "ingresos_totales": round(ingresos_totales, 2),
            "total_articulos": int(total_articulos),
            "total_transacciones": int(total_transacciones),
        },
        "ingresos_vendedor": ingresos_vendedor,
        "top_categorias": top_categorias,
        "tendencia": tendencia,
        "filtros": {
            "categorias": categorias,
            "vendedores": vendedores,
        }
    })


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True, port=5000)

import pandas as pd
import sqlite3
import re
import os

excel_file = 'sistema crud.xlsx'
db_file = 'inventario.db'

print(f"Leyendo {excel_file}...")
df = pd.read_excel(excel_file)

# 1. Eliminar filas completamente vacías
df.dropna(how='all', inplace=True)

# 2. LIMPIEZA ESTRICTA (Reglas de Negocio)

# Limpiar espacios extra en columnas de texto
string_cols = ['ID_Movimiento', 'Nombre_Producto', 'Categoria', 'Tipo_Movimiento', 'Vendedor']
for col in string_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# --- REGLA A: Categoría (Estandarización completa) ---
def estandarizar_categoria(cat):
    """Normaliza las variaciones de texto en la columna Categoria."""
    if not cat or str(cat).strip().lower() in ['nan', 'none', '']:
        return 'Sin Categoría'
    cat_clean = str(cat).strip().lower()
    if cat_clean.startswith('mob') or cat_clean.startswith('mobi'):
        return 'Mobiliario'
    if cat_clean.startswith('elec') or cat_clean.startswith('élec'):
        return 'Electrónica'
    if cat_clean.startswith('pap') or cat_clean.startswith('ofic'):
        return 'Papelería'
    if cat_clean.startswith('limp') or cat_clean.startswith('aseo'):
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

def auto_categorizar_por_producto(row):
    """Asigna categoría basándose en el nombre del producto si es 'Sin Categoría'."""
    if row['Categoria'] != 'Sin Categoría':
        return row['Categoria']
    producto = str(row.get('Nombre_Producto', '')).lower()
    for categoria, keywords in PRODUCTO_CATEGORIA_MAP.items():
        for kw in keywords:
            if kw in producto:
                return categoria
    return 'Sin Categoría'

if 'Categoria' in df.columns:
    df['Categoria'] = df['Categoria'].apply(estandarizar_categoria)
    # Auto-categorizar registros sin categoría basándose en el producto
    if 'Nombre_Producto' in df.columns:
        df['Categoria'] = df.apply(auto_categorizar_por_producto, axis=1)

# --- REGLA B: Tipo de Movimiento ---
if 'Tipo_Movimiento' in df.columns:
    df['Tipo_Movimiento'] = df['Tipo_Movimiento'].replace({'Entrd': 'Entrada', 'Slaida': 'Salida'})

# --- REGLA C: Cantidad (Absoluto y sin nulos) ---
if 'Cantidad' in df.columns:
    df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').fillna(0).astype(int)
    df['Cantidad'] = df['Cantidad'].abs() # Convierte negativos a positivos

# --- REGLA D: Precio Unitario (Quitar '$' y limpiar) ---
if 'Precio_Unitario' in df.columns:
    # Convertir a texto, quitar el símbolo $ y comas, luego pasar a número
    df['Precio_Unitario'] = df['Precio_Unitario'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
    df['Precio_Unitario'] = pd.to_numeric(df['Precio_Unitario'], errors='coerce').fillna(0.0).astype(float)

# --- REGLA E: Fechas (Estandarización YYYY-MM-DD) ---
if 'Fecha' in df.columns:
    meses_es = {'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04', 'mayo': '05', 'junio': '06', 
                'julio': '07', 'agosto': '08', 'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'}
    
    def parse_date(date_val):
        date_str = str(date_val).strip().lower()
        if date_str in ['nan', 'nat', 'none', '']:
            return pd.NaT
        
        # Buscar el patrón "DD de MES"
        match = re.search(r'(\d{1,2})\s+de\s+([a-z]+)', date_str)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2)
            month = meses_es.get(month_name, '01')
            return f"2026-{month}-{day}"
        
        return date_val

    df['Fecha'] = df['Fecha'].apply(parse_date)
    # Forzar formato a YYYY-MM-DD
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce').dt.strftime('%Y-%m-%d')
    # Opcional: llenar NaT con fecha base si es requerido, o dejarlos como nulos
    # df['Fecha'].fillna('2026-01-01', inplace=True)

# Borrar inventario.db si existe
if os.path.exists(db_file):
    print(f"\nEliminando base de datos antigua: {db_file}...")
    try:
        os.remove(db_file)
        print("Eliminada correctamente.")
    except Exception as e:
        print(f"Advertencia: No se pudo eliminar la base de datos antigua: {e}")

# 3. Guardar en SQLite
print(f"\nGuardando en {db_file}...")
conn = sqlite3.connect(db_file)
df.to_sql('inventario', conn, if_exists='replace', index=False)
conn.close()

print("\n¡Migración y limpieza estricta completada con éxito!")
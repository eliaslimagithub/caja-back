import logging.config
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from api.data.db import get_connection, if_table_exists, catalogs_by_name
from api.services.services import Productos, Inventarios, Politicas, Usuarios, Departamentos, Lineas
from settings import loggin_setup
import jwt
import datetime
from functools import wraps
import bcrypt
from inspect import signature
import pandas as pd
import openpyxl
import os
import tempfile
import math
import unicodedata
import re
loggin_setup.setup_logging()

log = logging.getLogger(__name__)
log.info("Inicio del programa")
app = Flask("main")

cors = CORS(app, resources={r'/*': {'origins': 'http://localhost:4200'}})
app.config['SECRET_KEY'] = 'clave-super-secreta'
#app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB espacio para la carga de un archivo
BATCH_SIZE = 100  # Tamaño del bloque de inserción para registros con mas de 1000 datos

def clean(v):
    """Convierte NaN o None en None válido para MySQL."""
    return None if (v is None or (isinstance(v, float) and math.isnan(v))) else v
def normalice(k):
    k = k.strip().lower()  # convertir en minusculas
    k = unicodedata.normalize('NFKD', k).encode('ascii', 'ignore').decode('utf-8')  # eliminar acentos
    # Mantener el espacio solo en "precio publico"
    if k == "precio publico" or k == 'precio minimo' or k == "costo promedio" or k == "ultimo costo":
        return k  # se queda con espacio
    else:
        return re.sub(r'\s+', '', k)  # elimina los espacios de la cadena

def token_requerido(f):
    @wraps(f)
    def decorador(*args, **kwargs):
        token = request.headers.get('Authorization')

        if not token or not token.startswith("Bearer "):
            return jsonify({'mensaje': 'Token faltante o inválido'}), 401

        token = token.replace("Bearer ", "")
        try:
            datos = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            usuario = datos.get('usuario')

            # Checar si la función decorada acepta 'usuario'
            sig = signature(f)
            if 'usuario' in sig.parameters:
                return f(*args, usuario=usuario, **kwargs)
            else:
                return f(*args, **kwargs)

        except jwt.ExpiredSignatureError:
            return jsonify({'mensaje': 'Token expirado'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'mensaje': 'Token inválido'}), 401
        except Exception as e:
            return jsonify({'mensaje': f'Error al procesar el token: {str(e)}'}), 401

    return decorador
@app.route('/keep-alive', methods=['POST'])
@token_requerido
def keep_alive(usuario=None):
    try:
        # Crear un nuevo token con tiempo renovado
        nuevo_token = jwt.encode({
            'usuario': usuario,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=300)  # 30 minutos
        }, app.config['SECRET_KEY'], algorithm='HS256')

        # (Opcional) actualizar el token en la base de datos
        u = Usuarios()
        user = u.login(usuario)
        if not user:
            return jsonify({'mensaje': 'Usuario no encontrado'}), 404

        sql = "UPDATE usuarios SET token = %s WHERE id_usuario = %s"
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, (nuevo_token, user.get('id_usuario')))
        conn.commit()

        return jsonify({'token': nuevo_token})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    alias = data.get('alias')
    clave = data.get('clave')
    log.info("Clave %s", clave)
    if not alias or not clave:
        log.error("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        u = Usuarios()
        ret = u.login(alias)
        if not ret:
           return jsonify({'mensaje': 'Usuario no existe'}), 406

        if ret and bcrypt.checkpw(clave.encode('utf-8'), ret.get('password').encode('utf-8')):
            token = jwt.encode({
                'usuario': alias,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=300)
            }, app.config['SECRET_KEY'], algorithm='HS256')
            sql = "update usuarios set token = %s where id_usuario = %s"
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (token, ret.get('id_usuario')))
            conn.commit()
            rows = u.get_user_by_id(ret.get('id_usuario'))
            return jsonify({'data': rows})
        else:
            return jsonify({'mensaje': 'La contraseña no es correcta'}), 406
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logout', methods=['POST'])
def logout():
    data = request.get_json()
    alias = data.get('alias')
    try:
        log.info("Logged user %s", alias)
        sql = "update usuarios set token = NULL where alias = %s"
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, (alias,))
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    # Clear specific session data or the entire session
    return jsonify({'message' : 'Se ha cerrado la sesión'}), 200

@app.route('/usuarios', methods=['GET'])
@token_requerido
def obtener_usuarios(usuario):
    alias = request.args.get('alias')  # <-- usando query params

    params = []

    sql = f"""
    select u.id_usuario, u.alias, u.nombre as nombre,r.id as id_rol,
    r.name rol, t.id_tienda as id_tienda,
    t.descripcion_tienda tienda 
    from usuarios u inner join roles r
    on u.id_rol = r.id
    left join tiendas t
    on t.id_tienda = u.id_tienda
                    """

    try:

        conn = get_connection()
        cursor = conn.cursor()

        if alias:
            sql += f""" where u.alias like %s"""
            params.append("%" + alias + "%")
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)


        usuarios = cursor.fetchall()

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()
    return jsonify(usuarios)

@app.route('/usuarios', methods=['POST'])
@token_requerido
def crear_usuario():
    data = request.get_json()
    nombre = data.get('nombre')
    alias = data.get('alias')
    clave = data.get('clave')
    id_rol = data.get('id_rol')
    id_tienda = data.get('id_tienda')

    if not nombre or not alias or not clave or not id_rol:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    hashed = bcrypt.hashpw(clave.encode('utf-8'), bcrypt.gensalt())
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "insert into usuarios(alias, nombre, password, id_rol, id_tienda) values(%s,%s,%s,%s,%s)"
            cursor.execute(sql, (alias, nombre, hashed, id_rol, id_tienda))
            conn.commit()

        log.info("Usuario creado correctamente")
        return jsonify({'mensaje': 'Usuario creado correctamente'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/usuarios/<int:id>', methods=['PUT'])
def actualizar_usuario(id):
    print("actualizando...")
    data = request.get_json()
    nombre = data.get('nombre')
    alias = data.get('alias')
    clave = data.get('clave')
    id_rol = data.get('id_rol')
    id_tienda = data.get('id_tienda')


    if not nombre or not alias or not clave or not id_rol:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "update usuarios set alias = %s, nombre = %s, clave = %s, id_rol = %s, id_tienda = %s where id_usuario = %s"
            cursor.execute(sql, (alias, nombre, clave, id_rol, id_tienda, id))
            conn.commit()
            if cursor.rowcount == 0:
                log.info("Usuario no encontrado")
                return jsonify({'mensaje': 'Usuario no encontrado'}), 404
        log.info("Usuario modificado correctamente")
        return jsonify({'mensaje': 'Usuario modificado correctamente'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/usuarios/<int:id>', methods=['DELETE'])
def eliminar_usuario(id):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "DELETE FROM usuarios WHERE id_usuario = %s"
            cursor.execute(sql, (id,))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({'mensaje': 'Usuario no encontrado'}), 404
        return jsonify({'mensaje': 'Usuario eliminado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/tiendas', methods=['GET'])
@token_requerido
def obtener_tiendas():
    try:
        sql = f"""
select t.id_tienda, t.descripcion_tienda, t.direccion, t.telefono, p.id as id_precio, p.name as descripcion_precio  
from tiendas t, cat_precios p
where t.id_precio_omision = p.id;
                    """
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        tiendas = cursor.fetchall()

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()
    return jsonify(tiendas)

@app.route('/tiendas', methods=['POST'])
@token_requerido
def crear_tienda():
    data = request.get_json()
    descripcion_tienda = data.get('descripcion_tienda')
    direccion = data.get('direccion')
    telefono = data.get('telefono')
    id_precio_omision = data.get('id_precio_omision')

    if not descripcion_tienda or not id_precio_omision:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = f"""
            insert into tiendas(descripcion_tienda, direccion, telefono, id_precio_omision) 
            values(%s, %s, %s, %s);
               """
            cursor.execute(sql, (descripcion_tienda, direccion, telefono, id_precio_omision))
            conn.commit()
        log.info("Tienda creada correctamente")
        return jsonify({'mensaje': 'Tienda creada correctamente'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/tiendas/<int:id>', methods=['PUT'])
@token_requerido
def actualizarr_tienda(id):
    data = request.get_json()
    descripcion_tienda = data.get('descripcion_tienda')
    direccion = data.get('direccion')
    telefono = data.get('telefono')
    id_precio_omision = data.get('id_precio_omision')

    if not descripcion_tienda or not id_precio_omision:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = f"""
update tiendas set descripcion_tienda = %s,
                   direccion = %s,
                   telefono = %s,
                   id_precio_omision = %s
                   where id_tienda = %s;
               """
            cursor.execute(sql, (descripcion_tienda, direccion, telefono, id_precio_omision, id))
            conn.commit()
        log.info("Tienda actualizada correctamente")
        return jsonify({'mensaje': 'Tienda actualizada correctamente'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/tiendas/<int:id>', methods=['DELETE'])
@token_requerido
def eliminar_tienda(id):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "DELETE FROM tiendas WHERE id_tienda = %s"
            cursor.execute(sql, (id,))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({'mensaje': 'Tienda no encontrada'}), 404
        return jsonify({'mensaje': 'Tienda eliminada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/productos', methods=['GET'])
def get_producto():
    data = request.get_json()
    id_tienda = data.get('id_tienda')
    clave = data.get('clave')
    if not id_tienda:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400
    try:
        p = Productos()
        productos = p.get_productos(id_tienda= id_tienda, clave=clave)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify(productos)

@app.route('/productos', methods=['DELETE'])
def delete_producto():
    data = request.get_json()
    tienda = data.get("id_tienda")
    producto = data.get("id_producto")
    try:
        p = Productos()
        ret = p.delete_producto(tienda, producto)
        if ret == 0:
            return jsonify({'mensaje': 'Producto no encontrado'}), 404
        return jsonify({'mensaje': 'Producto eliminado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/productos', methods=['POST'])
def save_producto():

    data = request.get_json()
    id_tienda = data.get("id_tienda")
    id_producto = data.get('id_producto')
    descripcion = data.get('descripcion')
    clave = data.get("clave")
    unidad_entrada =  data.get('unidad_entrada')
    existencia = data.get("existencia")
    id_linea = data.get("id_linea")

    if not id_tienda or not id_producto or not clave or not descripcion:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        p = Productos()
        ret = p.crear_producto(id_producto, clave, descripcion, data.get("clave_alterna"),
                               unidad_entrada, data.get("editar_precio"), id_linea)
        if ret == 0:
            log.info(f"No hubo cambios o registro para el producto {clave}")
        else:
            log.info(f"Nuevo registro o modificación del artículo {clave}")

        res = p.get_productos(id_tienda= id_tienda, clave=clave)
        id_producto_tmp = res[0].get('id_producto')
        log.info(f"Producto obtenido {clave}")

        if id_producto_tmp:
            inv = p.alta_inventario(id_tienda, id_producto_tmp, existencia, True)
            if inv == 0:
                log.warning("Inventario no registrado")
            else:
                log.info(f"Inventario para el articulo {clave} registrado correctamente")

            if data.get("precios") and len(data.get("precios")) > 0:
                p.upsert_precios(id_producto_tmp, data.get("precios"))

        return jsonify({'mensaje': 'Producto registrado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload/<string:method>', methods=['POST'])
@token_requerido
def subir_archivo(method):
    if method == "massive-products":
        if 'file' not in request.files:
            return jsonify({'error': 'Archivo no enviado'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify(error="Nombre del archivo vacío"), 400
        cat_precios = catalogs_by_name('cat_precios')
        #list_precios = [{normalice(k): v for k, v in row.items()} for row in cat_precios] # se llama a normalice para que solo el name le elimine los espacios
        list_precios = {normalice(k): v for d in cat_precios for k, v in d.items()}  # se llama a normalice para que solo el name le elimine los espacios
        cat_linea = catalogs_by_name('lineas', {0:'id_linea', 1:'clave'})
        rows_line = {k: v for d in cat_linea for k, v in d.items()} # unificar todo el array

        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                file.save(tmp.name)
                df = pd.read_excel(tmp.name)
                df = df.where(pd.notnull(df), None)
            #print("Excel ", df)
            columnas_esperadas = {
                'clave', 'descripcion', 'clave de linea', 'clave departamento',
                'clave de moneda', 'precio publico', 'precio2', 'precio3',
                'precio4', 'precio5', 'precio6', 'precio7', 'precio8', 'precio9',
                'precio minimo', 'clave alterna', 'unidad de entrada',
                'costo promedio', 'ultimo costo'
            }

            if not columnas_esperadas.issubset(df.columns):
                return jsonify({'error': f'Faltan columnas. Se requieren: {list(columnas_esperadas)}'}), 400

            conn = get_connection()
            cursor = conn.cursor()

            sql = """
                INSERT INTO productos (clave, descripcion, clave_alterna, unidad_entrada, editar_precio, id_linea)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    clave = VALUES(clave),
                    descripcion = VALUES(descripcion),
                    clave_alterna = COALESCE(VALUES(clave_alterna), clave_alterna),
                    unidad_entrada = COALESCE(VALUES(unidad_entrada), unidad_entrada),
                    editar_precio = 0,
                    id_linea = VALUES(id_linea);
                """

            exitosos = 0
            errores = []
            batch = []
            batch_precios = {}

            for index, row in df.iterrows():
                try:
                    clave = clean(row['clave'])
                    des = clean(row['descripcion'])
                    claLine = clean(row['clave de linea'])
                    claAlt = clean(row['clave alterna'])
                    unidad_entrada = clean(row['unidad de entrada'])

                    # Construir el batch para la lista de los catalogos con el asociativo clave de producto
                    if clave not in batch_precios:
                        batch_precios[clave] = {}
                    for k, id_precio in list_precios.items():
                        valor = clean(row.get(k))
                        if valor is not None and valor != "":
                            batch_precios[clave][id_precio] = valor

                    id_line = rows_line.get(f'{claLine}') # se compara con el value, en el cat de array de la linea si existe el id
                    if id_line is None:
                        errores.append({'fila': index + 2, 'error': f'Línea no encontrada: {claLine}'})
                        continue

                    batch.append((clave, des, claAlt, unidad_entrada, 0, id_line))
                    # Ejecutar en bloques de N filas
                    if len(batch) >= BATCH_SIZE:
                        #print("Longitud ", batch)
                        #print("BATCH ", BATCH_SIZE)
                        cursor.executemany(sql, batch)
                        conn.commit()
                        exitosos += len(batch)
                        batch.clear()

                except Exception as e:
                    errores.append({'fila': index + 2, 'error': str(e)})

            #

            # Ejecutar las filas restantes
            if batch:
                print("Ver cuando entra aqui", batch)
                cursor.executemany(sql, batch)
                conn.commit()
                exitosos += len(batch)
            if batch_precios:
                sql_precios = "INSERT INTO precios (id_producto, id_precio, precio) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE precio = VALUES(precio)"
                precios_insert = []
                for id_producto, precios in batch_precios.items():
                    sql_id_products = "SELECT id_producto FROM productos WHERE clave = %s"
                    cursor.execute(sql_id_products, id_producto)
                    id_prod = cursor.fetchone()
                    print("id_producto asd", id_prod.get('id_producto'))
                    for id_tipo_precio, valor in precios.items():
                        precios_insert.append((id_prod.get('id_producto'), id_tipo_precio, valor))

                if precios_insert:
                    cursor.executemany(sql_precios, precios_insert)
                    conn.commit()
            cursor.close()
            conn.close()

            return jsonify({
                'total': len(df),
                'exitosos': exitosos,
                'errores': errores
            })

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            if os.path.exists(tmp.name):
                os.remove(tmp.name)

    else:
        return jsonify({'error': ''}), 400
    return jsonify(mensaje=f"Archivo recibido: "), 200

@app.route('/inventarios/movimiento/<int:t>', methods=['POST'])
def movimiento_inventario(t):

    data = request.get_json()
    referencia = data.get("referencia")
    id_tienda = data.get("id_tienda")
    entradas = [1, 2, 5, 10]
    salidas = [6, 8]

    mov = Inventarios()
    p = Productos()

    try:
        n = 0
        for d in data.get("lista"):

            n = n + 1

            cantidad = d.get("cantidad")
            id_producto = d.get("id_producto")
            costo_unidad = d.get("costo_unidad")
            costo_total = d.get("costo_total")

            ret = mov.movimiento_inventario(referencia=referencia, id_tipo_movimiento=t, numero=n,
                                            cantidad=cantidad, id_producto=id_producto, costo_unidad=costo_unidad,
                                            costo_total=costo_total, id_tienda=id_tienda)
            if ret == 0:
                log.warning("Movimiento al inventario no registrado")

            else:
                log.info(f"Movimiento al inventario registrado correctamente")

                if t in salidas:
                    cantidad = -cantidad
                    print(f"->> cantidad {cantidad}")
                inv = p.alta_inventario(id_tienda, id_producto, cantidad)
                if inv == 0:
                    log.info(f"Hubo un error al calcular el stock")
                else:
                    log.info(f"Stock actializado correctamente")
        return jsonify({'mensaje': 'Movimiento registrado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/politicas', methods=['GET'])
def get_politicas():
    data = request.get_json()
    id_tienda = data.get("id_tienda")
    politica = data.get("politica")
    try:
        p = Politicas()
        politicas = p.get_politicas(id_tienda= id_tienda, politica = politica)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify(politicas)

@app.route('/politicas/<int:id_politica>', methods=['DELETE'])
def delete_politica(id_politica):

    try:
        p = Politicas()
        ret = p.delete_politica(id_politica)
        if ret == 0:
            return jsonify({'mensaje': 'Política no encontrado'}), 404
        return jsonify({'mensaje': 'Política eliminada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/politicas', methods=['POST'])
def save_politica():

    data = request.get_json()
    id_tienda = data.get("id_tienda")
    descripcion = data.get("descripcion")
    id_aplicacion = data.get("id_aplicacion")
    id_tipo = data.get("id_tipo")
    activa = data.get("activa")
    afectar_sobre = data.get("afectar_sobre")
    valor = data.get("valor")
    volumen_minimo = data.get("volumen_minimo")
    id_cliente = data.get("id_cliente")
    id_clasificacion = data.get("id_clasificacion")
    id_linea = data.get("id_linea")
    id_departamento = data.get("id_departamento")
    horario1_inicio = data.get("horario1_inicio")
    horario1_fin = data.get("horario1_fin")
    horario2_inicio = data.get("horario2_inicio")
    horario2_fin = data.get("horario2_fin")
    vigencia_inicio = data.get("vigencia_inicio")
    vigencia_fin = data.get("vigencia_fin")
    producto_desde = data.get("producto_desde")
    producto_hasta = data.get("producto_hasta")

    log.info(f"Request politicas: {data}")

    if not id_tienda or not descripcion or not id_aplicacion or not id_tipo:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        p = Politicas()
        ret = p.insert_politica(descripcion, id_tienda, activa, id_tipo,
                                     afectar_sobre, id_aplicacion, valor, volumen_minimo, id_cliente,
                                     id_clasificacion, id_linea, id_departamento, horario1_inicio,
                                     horario1_fin, horario2_inicio, horario2_fin, vigencia_inicio,
                                     vigencia_fin, producto_desde, producto_hasta)
        if ret == 0:
            log.warning(f"La politica {descripcion} no fue registrado")
            return jsonify({'mensaje': f'La politica {descripcion} no fue registrado'}), 404
        log.info(f"Politica {descripcion} registrada correctamente")

        return jsonify({'mensaje': f'Politica {descripcion} registrada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/politicas/<int:id_politica>', methods=['PUT'])
def update_politica(id_politica):

    data = request.get_json()
    id_tienda = data.get("id_tienda")
    descripcion = data.get("descripcion")
    id_aplicacion = data.get("id_aplicacion")
    id_tipo = data.get("id_tipo")
    activa = data.get("activa")
    afectar_sobre = data.get("afectar_sobre")
    valor = data.get("valor")
    volumen_minimo = data.get("volumen_minimo")
    id_cliente = data.get("id_cliente")
    id_clasificacion = data.get("id_clasificacion")
    id_linea = data.get("id_linea")
    id_departamento = data.get("id_departamento")
    horario1_inicio = data.get("horario1_inicio")
    horario1_fin = data.get("horario1_fin")
    horario2_inicio = data.get("horario2_inicio")
    horario2_fin = data.get("horario2_fin")
    vigencia_inicio = data.get("vigencia_inicio")
    vigencia_fin = data.get("vigencia_fin")
    producto_desde = data.get("producto_desde")
    producto_hasta = data.get("producto_hasta")

    log.info(f"Request politicas: {data}")

    if not id_tienda or not descripcion or not id_aplicacion or not id_tipo:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        p = Politicas()
        ret = p.update_politica(id_politica, descripcion, id_tienda, activa, id_tipo,
                                     afectar_sobre, id_aplicacion, valor, volumen_minimo, id_cliente,
                                     id_clasificacion, id_linea, id_departamento, horario1_inicio,
                                     horario1_fin, horario2_inicio, horario2_fin, vigencia_inicio,
                                     vigencia_fin, producto_desde, producto_hasta)
        if ret == 0:
            log.warning(f"Error al modificar la politica {descripcion}")
            return jsonify({'mensaje': f'Error al modificar la politica {descripcion}'}), 404
        log.info(f"Politica {descripcion} modificada correctamente")

        return jsonify({'mensaje': f'Politica {descripcion} modificada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/departamentos', methods=['GET'])
def get_departamentos():
        data = request.get_json()
        clave = data.get('clave')

        try:
            d = Departamentos()
            dptos = d.get_departamentos(clave)

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        return jsonify(dptos)

@app.route('/departamentos', methods=['POST'])
def create_departamento():

    data = request.get_json()
    clave = data.get('clave')
    descripcion = data.get("descripcion")

    log.info(f"Request politicas: {data}")

    if not clave or not descripcion:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        d = Departamentos()
        ret = d.crear_departamento(clave, descripcion)

        log.info(f"Departamento {descripcion} creado correctamente")

        return jsonify({'mensaje': f'Departamento {descripcion} creado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/departamentos/<int:id>', methods=['PUT'])
def update_departamento(id):
    print("actualizando departamento...")
    data = request.get_json()
    clave = data.get('clave')
    descripcion = data.get('descripcion')

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "update departamentos set clave = %s, descripcion_departamento = %s where id_departamento = %s"
            cursor.execute(sql, (clave, descripcion, id))
            conn.commit()
            if cursor.rowcount == 0:
                log.info("Departamento no encontrado")
                return jsonify({'mensaje': 'Departamento no encontrado'}), 404

        return jsonify({'mensaje': 'Departamento actualizado correctamente'}), 201
    except Exception as e:
        log.error(f"Error al actuzalizar departamento: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/departamentos/<int:id>', methods=['DELETE'])
def delete_departamento(id):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "DELETE FROM departamentos WHERE id_departamento = %s"
            cursor.execute(sql, (id,))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({'mensaje': 'Departamento no encontrado'}), 404
        return jsonify({'mensaje': 'Departamento eliminado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/lineas/<int:id>', methods=['DELETE'])
def delete_linea(id):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "DELETE FROM lineas WHERE id_linea = %s"
            cursor.execute(sql, (id,))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({'Linea': 'Linea no encontrada'}), 404
        return jsonify({'mensaje': 'Linea eliminada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/lineas', methods=['POST'])
def create_lineas():

    data = request.get_json()
    clave = data.get('clave')
    id_departamento = data.get('id_departamento')
    descripcion = data.get("descripcion")


    if not id_departamento or not clave or not descripcion:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        l = Lineas()
        ret = l.crear_linea(clave, descripcion, id_departamento)

        log.info(f"Linea {descripcion} creada correctamente")

        return jsonify({'mensaje': f'Linea {descripcion} creada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/lineas', methods=['GET'])
def get_lineas():
        data = request.get_json()
        clave = data.get('clave')

        try:
            l = Lineas()
            lineas = l.get_lineas(clave)

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        return jsonify(lineas)

@app.route('/lineas/<int:id>', methods=['PUT'])
def update_lineas(id):
    print("Actualizando linea...")
    data = request.get_json()
    clave = data.get('clave')
    id_departamento = data.get('id_departamento')
    descripcion = data.get('descripcion')

    try:
        l = Lineas()
        ret = l.update_linea(id_departamento, clave, descripcion, id)
        if ret == 0:
            log.info(f"Ningun cambio para la linea {clave}")
            return jsonify({'mensaje': 'Ninguna modificacin para la linea'}), 201


        return jsonify({'mensaje': 'Linea actualizada correctamente'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/catalogs/<string:name>', methods=['GET'])
def get_catalogs(name):
    print("Catalogos")
    log.info("Catalogos %s" % name)

    try:
        tabl = if_table_exists(name)
        if tabl.get('exi') == 0:
            return jsonify({'error': 'Catalogo no encontrado'}), 404

        sql = "SELECT id, name FROM "+name+" WHERE status = 0"
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        catalogs = cursor.fetchall()
    except Exception as e:
        jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()
    return jsonify(catalogs), 201

if __name__ == '__main__':
    app.run(debug=True) # debug=True para desarrollo
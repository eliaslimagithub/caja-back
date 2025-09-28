import logging.config
from flask import Flask, request, jsonify
from flask_cors import CORS
from api.data.db import get_connection
from api.services.services import Productos, Inventarios, Politicas, Usuarios, Departamentos, Lineas
from settings import loggin_setup

loggin_setup.setup_logging()

log = logging.getLogger(__name__)
log.info("Inicio del programa")
app = Flask("main")
cors = CORS(app, resources={r'/*': {'origins': 'http://localhost:4200'}})
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    alias = data.get('alias')
    clave = data.get('clave')

    if not alias or not clave:
        log.error("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        u = Usuarios()
        ret = u.login(alias, clave)
        if not ret:
            return jsonify({'mensaje': 'Usuario o contraseña incorrectos'}), 406
        return jsonify(ret)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/usuarios', methods=['GET'])
def obtener_usuarios():

    try:
        sql = f"""
        select u.id_usuario, u.alias, u.nombre, 
case 
	when editar_precio = 0 then 'No'
	when editar_precio = 1 then 'Si' 
end editar_precio,
       r.descripcion_rol as rol  from usuarios u, roles r
where u.id_rol = r.id_rol
                """
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        usuarios = cursor.fetchall()

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()
    return jsonify(usuarios)

@app.route('/usuarios', methods=['POST'])
def crear_usuario():
    data = request.get_json()
    nombre = data.get('nombre')
    alias = data.get('alias')
    clave = data.get('clave')
    id_rol = data.get('id_rol')
    editar_precio = data.get('editar_precio')

    if not nombre or not alias or not clave or not id_rol:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "insert into usuarios(alias, nombre, clave, id_rol, editar_precio) values(%s,%s,%s,%s,%s)"
            cursor.execute(sql, (alias, nombre, clave, id_rol, editar_precio))
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
    editar_precio = data.get('editar_precio')


    if not nombre or not alias or not clave or not id_rol:
        log.warning("Faltan datos requeridos")
        return jsonify({'error': 'Faltan datos requeridos'}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "update usuarios set alias = %s, nombre = %s, clave = %s, id_rol = %s, editar_precio = %s where id_usuario = %s"
            cursor.execute(sql, (alias, nombre, clave, id_rol, editar_precio, id))
            conn.commit()
            if cursor.rowcount == 0:
                log.info("Usuario no encontrado")
                return jsonify({'mensaje': 'Usuario no encontrado'}), 404
        log.info("Usuario creado correctamente")
        return jsonify({'mensaje': 'Usuario creado correctamente'}), 201
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
def obtener_tiendas():
    try:
        sql = f"""
select t.id_tienda, t.descripcion_tienda, t.direccion, t.telefono, p.descripcion_precio  
from tiendas t, cat_precios p
where t.id_precio_omision = p.id_precio;
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
            inv = p.alta_inventario(id_tienda, id_producto_tmp, existencia)
            if inv == 0:
                log.warning("Inventario no registrado")
            else:
                log.info(f"Inventario para el articulo {clave} registrado correctamente")

            if data.get("precios") and len(data.get("precios")) > 0:
                p.upsert_precios(id_producto_tmp, data.get("precios"))

        return jsonify({'mensaje': 'Producto registrado correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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

if __name__ == '__main__':
    app.run(debug=True) # debug=True para desarrollo
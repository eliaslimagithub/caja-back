import logging.config
from flask import Flask, request, jsonify
from api.data.db import get_connection
from settings import loggin_setup

loggin_setup.setup_logging()

log = logging.getLogger(__name__)
log.info("Inicio del programa")
app = Flask("main")

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
from tiendas t, precios p
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
                return jsonify({'mensaje': 'Tienda no encontrad'}), 404
        return jsonify({'mensaje': 'Tienda eliminada correctamente'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True) # debug=True para desarrollo
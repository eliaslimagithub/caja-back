import logging.config
from api.data.db import get_connection
from settings import loggin_setup
from decimal import Decimal
import json
from datetime import timedelta

loggin_setup.setup_logging()

log = logging.getLogger("Productos")
log.info("Inicio del programa")

class Usuarios:
    def login(self, alias:str):
        log.info("Logueando a: " + alias)
        try:
            sql = "SELECT * FROM usuarios WHERE alias = %s"
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (alias,))
            user = cursor.fetchone()

        except Exception as e:
            log.warning("Hubo un error al intentar loguearse", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return user
    def get_user_by_id(self,id: int):
        log.info("Inicio del programa")
        try:
            sql = "SELECT * FROM usuarios WHERE id_usuario = %s"
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (id,))
            user = cursor.fetchone()
        except Exception as e:
            log.warning("Hubo un error al intentar loguearse", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return user
class Productos:
    def get_products(self, params: any= None):
        print("Entando al catalogo de Productos")
        try:
            sql = f"""SELECT p.id_producto, p.clave, p.descripcion, p.clave_alterna, p.unidad_entrada, p.editar_precio, l.id_linea, l.clave, l.descripcion_linea, 
                    d.id_departamento, d.clave, d.descripcion_departamento, JSON_ARRAYAGG(JSON_OBJECT(cp.name, pr.precio)) AS lista_precios
                FROM `productos` p
                    INNER JOIN lineas l ON l.id_linea = p.id_linea
                    INNER JOIN departamentos d ON d.id_departamento = l.id_departamento
                    LEFT JOIN precios pr ON pr.id_producto = p.id_producto
                    LEFT JOIN cat_precios cp ON cp.id = pr.id_precio
                GROUP BY 1;"""
            #sql2 = f"""SELECT p.id_producto, p.clave, p.descripcion, p.clave_alterna, p.unidad_entrada, p.editar_precio, l.id_linea, l.clave, l.descripcion_linea,
            #        d.id_departamento, d.clave, d.descripcion_departamento
            #    FROM `productos` p
            #        INNER JOIN lineas l ON l.id_linea = p.id_linea
            #        INNER JOIN departamentos d ON d.id_departamento = l.id_departamento
            #    GROUP BY 1;"""
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            productrs = cursor.fetchall()
            print(f"{len(productrs)} productos encontrados")
            print(f"array {productrs}")

        except Exception as e:
            log.warning("Hubo un error al consultar productos", e)
            return e
        finally:
            cursor.close()
            conn.close()
        return productrs

    def get_productos(self, id_tienda: int = None, clave: str = None):
        print(f"Tienda: {id_tienda}")
        print(f"clave: {clave}")
        params = []
        try:
            sql = f"""
    select p.id_producto, p.clave clave, p.descripcion, p.clave_alterna, l.clave as clave_linea, l.descripcion_linea,
    i.stock existencias,
    pr_1.precio precio_publico,
    pr_2.precio precio_2,
    pr_3.precio precio_3,
    pr_4.precio precio_4,
    pr_5.precio precio_5,
    pr_6.precio precio_6,
    pr_7.precio precio_7,
    pr_8.precio precio_8,
    pr_9.precio precio_9,
    pr_10.precio precio_minimo,
    pr_11.precio costo_promedio,
    pr_12.precio ultimo_costo
    from productos p 
                        left join inventarios i
                        on i.id_producto = p.id_producto
                        left join lineas l on l.id_linea = p.id_linea                   
                        left join precios pr_1 on pr_1.id_producto = p.id_producto 
                        and pr_1.id_precio = 1
                        left join precios pr_2 on pr_2.id_producto = p.id_producto 
                        and pr_2.id_precio = 2
                        left join precios pr_3 on pr_3.id_producto = p.id_producto 
                        and pr_3.id_precio = 3
                        left join precios pr_4 on pr_4.id_producto = p.id_producto 
                        and pr_4.id_precio = 4
                        left join precios pr_5 on pr_5.id_producto = p.id_producto 
                        and pr_5.id_precio = 5
                        left join precios pr_6 on pr_6.id_producto = p.id_producto 
                        and pr_6.id_precio = 6
                        left join precios pr_7 on pr_7.id_producto = p.id_producto 
                        and pr_7.id_precio = 7
                        left join precios pr_8 on pr_8.id_producto = p.id_producto 
                        and pr_8.id_precio = 8
                        left join precios pr_9 on pr_9.id_producto = p.id_producto 
                        and pr_9.id_precio = 9
                        left join precios pr_10 on pr_10.id_producto = p.id_producto 
                        and pr_10.id_precio = 10
                        left join precios pr_11 on pr_11.id_producto = p.id_producto 
                        and pr_11.id_precio = 11
                        left join precios pr_12 on pr_12.id_producto = p.id_producto 
                        and pr_12.id_precio = 12           
                            """
            if id_tienda and id_tienda is not None:
                sql += f"""inner join tiendas t 
                        on t.id_tienda = i.id_tienda """
                sql += f""" and i.id_tienda = %s"""
                params.append(id_tienda)

            if clave and clave is not None:
                sql += f""" where p.clave like %s"""
                params.append("%" + clave + "%")

            print(f"--SQL: {sql}")
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            prods = cursor.fetchall()

        except Exception as e:
            log.warning("Hubo un error al consultar productos", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return prods

    def delete_producto(self, id_tienda: int = None, id_producto: int = None):
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = "delete from inventarios where id_tienda = %s and id_producto = %s;"
                cursor.execute(sql, (id_tienda, id_producto))
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

    def crear_producto(self, id_producto:int, clave:str, descripcion:str, clave_alterna:str,
                             unidad_entrada:str, editar_precio:bool, id_linea:int):
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""insert into productos values(%s, %s, %s, %s, %s, %s, %s)
                          ON DUPLICATE KEY UPDATE clave = values(clave), 
                                                  descripcion = values(descripcion),
                                                  clave_alterna = coalesce(values(clave_alterna), clave_alterna),
                                                  unidad_entrada = coalesce(values(unidad_entrada), unidad_entrada),
                                                  editar_precio = coalesce(values(editar_precio), editar_precio, id_linea);
                       """
                cursor.execute(sql, (id_producto, clave, descripcion, clave_alterna,
                               unidad_entrada, editar_precio, id_linea))

                conn.commit()
                log.info("->>> Producto procesado correctamente")
            return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

    def upsert_precios(self, id_poducto:int, precios:list):
        try:
            print("->>>> 1")
            conn = get_connection()
            with conn.cursor() as cursor:
                for p in precios:
                    sql = f"""insert into precios values(%s, %s, %s)
                                              ON DUPLICATE KEY UPDATE precio = coalesce(values(precio), precio);
                                           """
                    cursor.execute(sql, (id_poducto, p.get("id_precio"), p.get("precio")))

            conn.commit()
            log.info("->>> Precios registrados correctamente")
            return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

    def alta_inventario(self, id_tienda:int = None, id_producto:int = None, cantidad:float = 0, edit:bool = False):

        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                   insert into inventarios (id_tienda, id_producto, stock)
                       values(%s, %s, %s)
                       
                   """
                if edit:
                    sql += f""" ON DUPLICATE KEY UPDATE stock = coalesce(VALUES(stock), 0);"""
                else:
                    sql += f""" ON DUPLICATE KEY UPDATE stock = stock + coalesce(VALUES(stock), 0);"""


                cursor.execute(sql, (id_tienda, id_producto, cantidad))
                conn.commit()
            return cursor.rowcount
        except Exception as e:
            log.error(f"Error al registrar inventario: {e}")
            raise e
        finally:
            conn.close()

class Inventarios:

    def movimiento_inventario(self, referencia:str, id_tipo_movimiento:int,
                                    id_tienda:int, id_tienda_origen:int, lista=list):
        salidas = [6, 8]

        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql1 = "insert into movimientos_inventario values(null, %s, %s, now(), %s, %s)"
                cursor.execute(sql1, (referencia, id_tipo_movimiento,
                                     id_tienda, id_tienda_origen))

                for d in lista:

                    n = d.get("numero")
                    cantidad = d.get("cantidad")
                    id_producto = d.get("id_producto")
                    costo_unidad = d.get("costo_unidad")
                    costo_total = d.get("costo_total")

                    sql2 = f"""
                           insert into movimientos_producto(id_movimiento, numero, cantidad, id_producto, costo_unidad, costo_total)
                           select mi.id_movimiento id_movimiento, %s numero, %s cantidad, %s id_producto, %s costo_unidad, %s costo_total
                           from movimientos_inventario mi where mi.referencia = %s;
                            """
                    cursor.execute(sql2, (n, cantidad, id_producto,
                                         costo_unidad, costo_total, referencia))

                    if id_tipo_movimiento in salidas:
                        cantidad = -cantidad
                        log.info(f"->> cantidad {cantidad}")

                    sql3 = f"""
                            insert into inventarios values(null, %s, %s, %s)
                             ON DUPLICATE KEY UPDATE stock = stock + coalesce(VALUES(stock), 0);
                            """
                    cursor.execute(sql3, (id_tienda, id_producto, cantidad))

                log.info("Registro exitoso")
                conn.commit()

            return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

    def get_movimientos(self, referencia:str):
        log.info(f"Recibiendo referencia: {referencia}...")
        params = []
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                select mi.id_movimiento, referencia, fecha_movimiento, tm.descripcion_movimiento movimiento,
                       t.descripcion_tienda tienda, t_origen.descripcion_tienda tienda_origen,
                       JSON_ARRAYAGG(
           JSON_OBJECT(
           'numero', mp.numero,
           'id_produco', mp.id_producto,
           'clave_prod', p.clave, 
           'descripcion', p.descripcion,
           'cantidad', mp.cantidad,
           'costo_unidad', mp.costo_unidad,
           'costo_total', mp.costo_total 
           )) as lista
                        from  movimientos_inventario mi inner join tipo_movimiento tm
                        on mi.id_tipo_movimiento = tm.id_tipo_movimiento
                        inner join movimientos_producto mp on mp.id_movimiento = mi.id_movimiento
                        inner join productos p on p.id_producto = mp.id_producto 
                         inner join tiendas t on t.id_tienda = mi.id_tienda 
                         left join tiendas t_origen on t_origen.id_tienda = mi.id_tienda_origen
                     """
                if referencia and referencia is not None:
                    sql += " where referencia like  %s"
                    params.append("%" + referencia + "%")
                sql += " group by mi.id_movimiento;"

                cursor.execute(sql, params)
                ret = cursor.fetchall()

        except Exception as e:
            raise e
        finally:
            conn.close()
        return ret

class Politicas:

    def get_politicas(self, id_tienda: int = None, politica: str = None):

        params = []
        try:
            sql = f"""
select descripcion_politica, t.descripcion_tienda as tienda,
case 
	when activa = 0 then 'No'
	when activa = 1 then 'Si' 
end activa,
valor, volumen_minimo,
tp.descripcion_tipo_politica as tipo_politica,
vigencia_inicio, vigencia_fin,
cast(horario1_inicio as char) horario1_inicio,
cast(horario1_fin as char) horario1_fin,
cast(horario2_inicio as char) horario2_inicio,
cast(horario2_fin as char) horario2_fin,
a1.clave desde, a2.clave hasta,
ap.descripcion as aplicacion
from politicas p, tiendas t, tipo_politica tp, productos a1, productos a2,
     cat_aplicacion ap
        where p.id_tienda = t.id_tienda
        and p.id_tipo = tp.id_tipo_politica
        and a1.id_producto = producto_desde
        and a2.id_producto = producto_desde
        and ap.id_aplicacion = p.id_aplicacion
        and p.id_tienda = %s
                            """
            params.append(id_tienda)

            if politica:
                sql += " and p.descripcion_politica like  %s"
                params.append("%" + politica + "%")


            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)



            pols = cursor.fetchall()

        except Exception as e:
            log.warning("Hubo un error al consultar las politicas", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return pols

    def delete_politica(self, id_politica: int = None):
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = "delete from politicas where id_politica = %s;"
                cursor.execute(sql, (id_politica))
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

    def insert_politica(self, descripcion_politica: str, id_tienda: int, activa: bool, id_tipo: int, afectar_sobre: int,
                             id_aplicacion: int, valor: float, volumen_minimo: float, id_cliente: int,
                             id_clasificacion: int, id_linea: int, id_departamento: int, horario1_inicio: str,
                             horario1_fin: str, horario2_inicio: str, horario2_fin: str, vigencia_inicio: str,
                             vigencia_fin: str, producto_desde: int, producto_hasta: int):

        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""insert into politicas values(
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                        );"""
                cursor.execute(sql, (None, descripcion_politica, id_tienda, activa, id_tipo,
                                     afectar_sobre, id_aplicacion, valor, volumen_minimo, id_cliente,
                                     id_clasificacion, id_linea, id_departamento, horario1_inicio,
                                     horario1_fin, horario2_inicio, horario2_fin, vigencia_inicio,
                                     vigencia_fin, producto_desde, producto_hasta))
                conn.commit()
            return cursor.rowcount
        except Exception as e:
            log.error("Hubo un error al registrar la politica: ", e)
            raise e
        finally:
            conn.close()

    def update_politica(self, id_politica, descripcion_politica: str, id_tienda: int, activa: bool, id_tipo: int, afectar_sobre: int,
                             id_aplicacion: int, valor: float, volumen_minimo: float, id_cliente: int,
                             id_clasificacion: int, id_linea: int, id_departamento: int, horario1_inicio: str,
                             horario1_fin: str, horario2_inicio: str, horario2_fin: str, vigencia_inicio: str,
                             vigencia_fin: str, producto_desde: int, producto_hasta: int):

        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                update politicas set
descripcion_politica = coalesce(%s, descripcion_politica),
id_tienda = coalesce(%s, id_tienda),
activa = coalesce(%s, activa),
id_tipo = coalesce(%s, id_tipo),
afectar_sobre = coalesce(%s, afectar_sobre),
id_aplicacion = coalesce(%s, id_aplicacion),
valor = coalesce(%s, valor),
volumen_minimo = coalesce(%s, volumen_minimo),
id_cliente = coalesce(%s, id_cliente),
id_clasificacion = coalesce(%s, id_clasificacion),
id_linea = coalesce(%s, id_linea),
id_departamento = coalesce(%s, id_departamento),
horario1_inicio = coalesce(%s, horario1_inicio),
horario1_fin = coalesce(%s, horario1_fin),
horario2_inicio = coalesce(%s, horario2_inicio),
horario2_fin = coalesce(%s, horario2_fin),
vigencia_inicio = coalesce(%s, vigencia_inicio),
vigencia_fin = coalesce(%s, vigencia_fin),
producto_desde = coalesce(%s, producto_desde),
producto_hasta = coalesce(%s, producto_hasta)
where id_politica = %s
                      """
                cursor.execute(sql, (descripcion_politica, id_tienda, activa, id_tipo,
                                     afectar_sobre, id_aplicacion, valor, volumen_minimo, id_cliente,
                                     id_clasificacion, id_linea, id_departamento, horario1_inicio,
                                     horario1_fin, horario2_inicio, horario2_fin, vigencia_inicio,
                                     vigencia_fin, producto_desde, producto_hasta, id_politica))
                conn.commit()
            return cursor.rowcount
        except Exception as e:
            log.error("Hubo un error al registrar la politica: ", e)
            raise e
        finally:
            conn.close()

class Departamentos:
    def get_departamentos(self, clave:str = None):
        log.info(f"Departamento: {clave}")
        params =[]
        try:
            conn = get_connection()
            cursor = conn.cursor()
            sql = f"""
                    select * from departamentos
                        """
            if clave:
                sql += f""" where clave like %s"""
                params.append("%" + clave + "%")
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            dptos = cursor.fetchall()

        except Exception as e:
            log.warning("Hubo un error al consultar departamentos", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return dptos

    def crear_departamento(self, clave:str, descripcion:str):
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""insert into departamentos values(null, %s, %s);
                       """
                cursor.execute(sql, (clave, descripcion))

                conn.commit()
                log.info(f"->>> departamento {descripcion} creado correctamente")
            return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

class Lineas:
    def get_lineas(self, clave:str = None):
        log.info(f"Consultando linea: {clave}")
        params =[]
        try:
            conn = get_connection()
            cursor = conn.cursor()
            sql = f"""
                    select l.id_linea, l.clave, l.descripcion_linea, d.descripcion_departamento,
                           l.id_departamento 
                     from lineas l, departamentos d 
                    where l.id_departamento = d.id_departamento; 
                        """
            if clave:
                sql += f""" where clave like %s"""
                params.append("%" + clave + "%")
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            dptos = cursor.fetchall()

        except Exception as e:
            log.warning("Hubo un error al consultar lineas", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return dptos

    def crear_linea(self, clave:str, descripcion:str, id_departamento:int):

        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""insert into lineas (clave, descripcion_linea, id_departamento) values(%s, %s, %s);
                       """
                cursor.execute(sql, (clave, descripcion, id_departamento))

                conn.commit()
                log.info(f"->>> linea {descripcion} creada correctamente")
            return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

    def update_linea(self, id_departamento:int, clave:str, descripcion:str, id_linea:int):
        print("*>>>>>>>> update_linea")
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                        update lineas set clave = %s, descripcion_linea = %s, id_departamento = %s
                        where id_linea = %s
                              """
                cursor.execute(sql, (clave, descripcion, id_departamento, id_linea))
                conn.commit()
            return cursor.rowcount
        except Exception as e:
            log.error("Hubo un error modificar la linea: ", e)
            raise e
        finally:
            conn.close()




#ret = Productos()
#print(f"->>> {ret.get_productos(3)}")
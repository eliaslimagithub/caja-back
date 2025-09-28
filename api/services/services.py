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
    def login(self, alias:str, clave:str):
        log.info("Logueando a: " + alias)
        try:
            sql = f"""
                   select * from usuarios where alias = %s and clave  = %s
                                """


            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (alias, clave))

            user = cursor.fetchone()

        except Exception as e:
            log.warning("Hubo un error al intentar loguearse", e)
            raise e
        finally:
            cursor.close()
            conn.close()
        return user



class Productos:

    def get_productos(self, id_tienda:int = None, clave:str = None):
        print(f"Tienda: {id_tienda}")
        print(f"clave: {clave}")
        params =[]
        try:
            sql = f"""
                    select * from productos p 
                    left join inventarios i
                    on i.id_producto = p.id_producto
                    left join tiendas t 
                    on t.id_tienda = i.id_tienda 
                    and i.id_tienda = %s
                        """
            params.append(id_tienda)
            if clave:
                sql += f""" where p.clave like %s"""
                params.append("%" + clave + "%")

            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            print(f"->> sql {sql}")
            prods = cursor.fetchall()
            print(f"->>>> prods:  {prods}")

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

    def alta_inventario(self, id_tienda:int = None, id_producto:int = None, cantidad:float = 0):

        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = f"""
                   insert into inventarios (id_tienda, id_producto, stock)
                       values(%s, %s, %s)
                       ON DUPLICATE KEY UPDATE stock = stock + coalesce(VALUES(stock), 0);
                   """

                cursor.execute(sql, (id_tienda, id_producto, cantidad))
                conn.commit()
            return cursor.rowcount
        except Exception as e:
            log.error(f"Error al registrar inventario: {e}")
            raise e
        finally:
            conn.close()

class Inventarios:

    def movimiento_inventario(self, referencia:str, id_tipo_movimiento:int, numero:int,
                                    cantidad:float, id_producto:int, costo_unidad:float, costo_total:float,
                                    id_tienda:int):
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                sql = "insert into movimientos_inventario values(%s, %s, %s, %s, %s, %s, %s, %s, now(), %s)"
                cursor.execute(sql, (None, referencia, id_tipo_movimiento, numero,
                                     cantidad, id_producto, costo_unidad, costo_total,
                                     id_tienda))
                log.info("Registro exitoso")
                conn.commit()
            return cursor.rowcount
        except Exception as e:
            raise e
        finally:
            conn.close()

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
    def get_departamentos(self, clave):
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
    def get_lineas(self, clave):
        log.info(f"Consultando linea: {clave}")
        params =[]
        try:
            conn = get_connection()
            cursor = conn.cursor()
            sql = f"""
                    select * from lineas
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
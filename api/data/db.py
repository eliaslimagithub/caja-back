import pymysql.cursors

def get_connection():
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='',
            database='caja',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Conectando...")
        return conn
    except Exception as e:
        print(f"Error de conexion: {e}")
        raise e

def if_table_exists(table):
    print("Method con, name base: %s", table)
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "SELECT COUNT(*) as exi FROM information_schema.tables WHERE table_name = %s"
        cursor.execute(sql, (table,))
        dt = cursor.fetchone()

    except Exception as e:
        print(f"Error de conexion: {e}")
    finally:
        cursor.close()
        conn.close()
    return dt
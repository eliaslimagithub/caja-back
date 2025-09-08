import pymysql.cursors

def get_connection():
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='Elias_lima86',
            database='caja',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Conectando...")
        return conn
    except Exception as e:
        print(f"Error de conexion: {e}")
        raise e

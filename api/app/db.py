import psycopg2

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="sentinel_db",
        user="sentinel",
        password="sentinel"
    )

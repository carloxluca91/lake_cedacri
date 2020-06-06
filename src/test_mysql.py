import mysql.connector

config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'consulente',
    'password': 'Velvet2791!',
    'raise_on_warnings': True,
    'ssl_disabled': True
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
cursor.execute("show databases")

existing_databases: str = ", ".join(map(lambda x: x[0], cursor))
print(f"existing databases: {existing_databases}")
for (database, ) in cursor:

    print(type(database), database)

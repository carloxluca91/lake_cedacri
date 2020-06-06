import mysql.connector

config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'consulente',
    'password': 'Velvet2791!',
    'raise_on_warnings': True
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
cursor.execute("show databases")

for database in cursor:

    print(database)

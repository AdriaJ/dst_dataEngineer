from pyhive import hive
conn = hive.connect('localhost')
cursor = conn.cursor()
cursor.execute('SELECT driverid, event FROM geoloc_trucks.geo LIMIT 15')
print(cursor.fetchone())
print(cursor.fetchall())

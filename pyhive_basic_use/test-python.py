from pyhive import hive

host_name = "localhost"
port = 10000
user = "hduser"
password = "a"
database="geoloc_trucks"

def hiveconnection(host_name, port, user,password, database):
    conn = hive.Connection(host=host_name, port=port, username=user) #, password=password,
#                           database=database, auth='CUSTOM')
    print('Connnexion successful')
#    conn = hive.connect('localhost')
    cur = conn.cursor()
    cur.execute('select driverid, event from geoloc_trucks.geo limit 20')
    result = cur.fetchall()

    return result

# Call above function
output = hiveconnection(host_name, port, user,password, database)
print(output)

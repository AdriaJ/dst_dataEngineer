#!/usr/bin/python3

import pymysql.cursors

#connection to MySQL daatabase
connection=pymysql.connect(host='localhost', user ='root', password = 'sql')

try:
	with connection.cursor() as cursor:
		cursor.execute('CREATE DATABASE imdb')
finally:
	connection.close()

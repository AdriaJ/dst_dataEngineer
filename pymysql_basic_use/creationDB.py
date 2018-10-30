#!/usr/bin/python3

import pymysql.cursors

#connection to MySQL daatabase
connection=pymysql.connect(host='localhost', user ='root', password = 'sql')

try:
	cursor = connection.cursor()
	cursor.execute('CREATE DATABASE imdb')
finally:
	connection.close()

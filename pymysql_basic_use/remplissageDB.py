#!/usr/bin/python3

import pymysql.cursors

#connection to MySQL daatabase
connection=pymysql.connect(host='localhost', user ='root', password = 'sql', db = 'imdb')
print("Connexion réussie avec MySQL")


path = "/home/hduser/SQL_datasets/name.basics.tsv"
file = open(path, 'r')

try :
#	with connection.cursor() as cursor:
	cursor = connection.cursor()
	counter = 0
	for line in file:
		if counter ==0:
			#traitement du header : init de la table
			headers_command = ""
			fl = line.split()
			stfl = str(tuple(fl)).replace("'", "")
			stfl = stfl.replace("primaryName","firstName, name")
			for word in fl:
				if "Year" in word:
          				type_name = "INT"
				else : type_name = "VARCHAR(255)"
				headers_command += word + " " + type_name + ", "
			headers_command = headers_command[:-2]
#			print(headers_command)
			sql = "CREATE TABLE IF NOT EXISTS names("+headers_command+" );"
#			print(sql)
			cursor.execute(sql)
		elif counter == 1001 :
			print("Limite : 1 000 lignes atteintes")
			break
		else :
			#insert line in DB
			cursor = connection.cursor()
			l = [("NULL" if w=="\\N" else w) for w in line.split()]
			stl = str(tuple(l)).replace("'","")
			sql = "INSERT INTO names " + stfl + " VALUES " + stl + ";"
			print(sql)
#			cursor.execute(sql)
#			connection.commit()
		counter +=1
finally :
	connection.close()
	file.close()

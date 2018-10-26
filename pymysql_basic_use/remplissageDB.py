#!/usr/bin/python3

import pymysql.cursors

#connection to MySQL daatabase
connection=pymysql.connect(host='localhost', user ='root', password = 'sql', db = 'new_database')
print("Connexion réussie avec MySQL")


path = "/home/hduser/SQL_datasets/name.basics.tsv"
file = open(path, 'r')

try :
	with connection.cursor() as cursor:
		counter = 0
		for line in file:
			if counter ==0:
				#traitement du header : init de la table
			counter +=1
			# insert line in DB
			if counter == 10 : break


finally :
	connection.close()
	file.close()

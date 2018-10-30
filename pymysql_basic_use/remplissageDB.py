#!/usr/bin/python3

import pymysql.cursors

#connection to MySQL daatabase 
connection=pymysql.connect(host='localhost', user ='root', password = 'sql', db = 'imdb')
print("Connexion réussie avec MySQL")


path = "/home/hduser/SQL_datasets/name.basics.tsv"
file = open(path, 'r')

try :
	cursor = connection.cursor()
	counter = 0
	for line in file:
		if counter ==0:
			#traitement du header : init de la table
			print("Creation des colonnes \n")
			headers_command = ""
			fl = line.split()
			stfl = str(tuple(fl)).replace("'", "")
			stfl = stfl.replace("primaryName","firstName, name")
			ind = fl.index("primaryName")
			fl.pop(ind)
			fl.insert(ind,"firstName")
			fl.insert(ind+1,"name")
			col_num = len(fl)
			print("Nombre de colonnes :",col_num,"\n")
			for word in fl:
				if "Year" in word:
          				type_name = "INT"
				else : type_name = "VARCHAR(255)"
				headers_command += word + " " + type_name + ", "
			headers_command = headers_command[:-2]
#			print(headers_command)
			sql = "CREATE TABLE IF NOT EXISTS names("+headers_command+", PRIMARY KEY ("+ fl[0] +") );"
#			print(sql)
			cursor.execute(sql)
			print("remplissage des lignes \n")
		elif counter == 3001 :
			print("Limite : 3 000 lignes atteintes")
			break
		else :
			#insert line in DB
			cursor = connection.cursor()
			l = [("NULL" if w=="\\N" else w) for w in line.split()]
			#Verification du bon nombre d'arguments
			while len(l) > col_num:
				#concaténation du nom en plusieurs mots
				l[2] += " " + l.pop(3)
			if len(l) <6 :
				#situation où le nom n'est aps en deux mots
				l.insert(1,'NULL')
			if len(l) != col_num:
				#Error on the data format
				l = [l[0]] + ['NULL']*(col_num-1)
			stl = str(tuple(l))
			stl = stl.replace("'NULL'","NULL")
			sql = "INSERT INTO names " + stfl + " VALUES " + stl + ";"
			print("Commande SQL :",sql,'\n',counter,'\n')
			cursor.execute(sql)
			connection.commit()
		counter +=1
finally :
	connection.close()
	file.close()

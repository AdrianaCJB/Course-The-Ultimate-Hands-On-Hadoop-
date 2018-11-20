
'''      Incremental imports          '''

--check-colum and --last-value



--------------------------- IMPORT MOVIELENS DATA INTO A MYSQL DATABASE -----------------------
------------------------------------------------------------------------------------------------

#Por consola:
	mysql -u root -p
	create database xxxx;
	show databases;
	use xxxx;
	source xxxx.sql; # archivo .sql que contiene inserts, creates de tablas
	show tables;
	describe table_xxxx; # comando que muestra la estructura de una tablas
 
------------------------------- IMPORT THE MOVIES INTO HDFS ------------------------------------
------------------------------------------------------------------------------------------------

'''     Import data from MySQL HDFS    '''

sqoop import --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver --table movies -m 1


------------------------------- IMPORT THE MOVIES INTO HIVE ------------------------------------
------------------------------------------------------------------------------------------------

'''    Import data from MySQL drirectly into Hive     '''

sqoop import --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver --table movies -m 1 --hive-import


-------------------------------- EXPORT THE MOVIES BACK INTO MYSQL -----------------------------
------------------------------------------------------------------------------------------------

CREATE TABLE exported_movies (id INTEGER, title VARCHAR(255), releaseDate DATE);


'''      Export data from Hive to MySQL          '''

#Target table must already exist in MySQL.

sqoop export --connect jdbc:mysql://localhost/movielens -m 1 --driver com.mysql.jdbc.Driver --table exported_movies --export-dir /apps/hive/warehouse/movies --input-fields-terminated-by '\0001'








































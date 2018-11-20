## Run in an enviroment like Cacopy and install the starbase package (Rest Client for HBase) with pip command " pip install starbase "


from starbase import Connection

c = Connection("127.0.0.1","8080")

ratings = c.table('ratings')

if (ratings.exists()):
	print ("Dropping existing ratings table \n")
	ratings.drop()
	
ratings.create('rating')

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("C:\Users\AJIMENEZ\Documents\BIG DATA\The Ultimate Hands-On Hadoop - Udemy\ml-100k\u.data","r")

batch = ratings.batch()

for line in ratingFile:
	(userID, movieID, rating, timestamp) = line.split()
	batch.update(userID, {'rating': {movieID:rating}}

ratingFile.close()

print("Commiting ratings data to HBase via REST service \n")
batch.commit(finalize=True)

print("Get back ratings for some users... \n")
print("Ratings for user ID 1: \n")
print(ratings.fetch("1"))
print("Ratings for user ID 2: \n")
print(ratings.fetch("33"))



------------------------------- INTEGRATING PIG WITH HBase ------------------------------------
------------------------------------------------------------------------------------------------

hbase shell
list # para ver las tablas creadas
create 'users', 'userinfo'  # para crear tabla users
pig hbase.pig # para ejecutar los LOAD de las tablas (revisar archivo hbase.pig
scan 'users' # para ver los datos

# para eliminar
disable 'users'
drop 'users'
































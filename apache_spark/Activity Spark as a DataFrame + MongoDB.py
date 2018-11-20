# Install MongoDB

cd /var/lib/ambari-server/resources/stacks

# Add MongoDB to the list of services in the version installed 

cd /var/lib/ambari-server/resources/stacks/HDP/2.6/services

# Connector Mongo-Ambari available on GitHub

git clone http://github.com/nikunjness/mongo-ambari.git

# After we need to restar the Ambari server.

sudo service ambari restart

# this makes sure that all the bindings for Python and MongoDB are in place.
pip install pymongo

''' En resumen, se puede cargar a un tabla de MongoDB un archivo alojado en HDFS, mediante integracion con Spark'''

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 MongoSpark.py 


############################  MONGODB SHELL ##############################


mongo
use movielens
db.users.find({ user_id: 100})
db.users.explain().find({ user_id: 100})

# Setting up an index in MongoDB
db.users.createIndex({ user_id: 1})

db.users.aggregate( [
{ $group: { _id: { occupation: "$occupation"}, avgAge: { $avg: "$age" } } }
] )


db.users.count()
db.getCollectionInfos()
db.users.drop()





























# Pass parameters to our script
# This is saying that I want the version of the connector thtat is compatible with spark 2.0 and Scala
spark-submit --packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 CassandraSpark.py 


service cassandra stop


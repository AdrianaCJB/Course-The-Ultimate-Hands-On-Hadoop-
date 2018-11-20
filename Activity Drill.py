Setting up Drill

# 1. Create database and table in MongoDB

# Obtain file to install

wget http://archive.apache.org/dist/drill/drill-1.12.0/apache-drill-1.12.0.tar.gz

# Decompress file 

tar -xvf apache-drill-1.12.0.tar.gz

cd apache-drill-1.12.0

bin/drillbit.sh start -Ddrill.exec.port=8765

bin/drillbit.sh stop 

# In browser , pull up 127.0.0.1:8765







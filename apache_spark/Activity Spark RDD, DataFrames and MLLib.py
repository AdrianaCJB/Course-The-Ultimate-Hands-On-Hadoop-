/ ********************************************************************** /
*				 
*	
*								SPARK				
*			
*
/ ********************************************************************** /

--------------------------- FIND THE MOVIE WITH THE LOWEST AVERAGE -----------------------------------------
------------------------------------- RATING WITH RDD'S ----------------------------------------------------


from pyspark import SparkConf, SparkContext

# This function just creates a Python "dictionary" we can later
# user to convert movie ID's to movie names while printing out 
# the final results.

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames
	
	
# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average 

def parseInput(line):
		fields = line.split()
		return (int(fields[1]) , (float(fields[2]), 1.0))
		
if __name__ = "__main__":
	# The main script - create our SparkContext
	conf = SparkConf().setAppName("WorstMovies")
	sc = SparkContext(conf = conf)
	
	# Load up our movie ID -> movie name lookup table
	movieNames = loadMovieNames()
	
	# Load up the raw u.data file
	lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
	
	# Convert to (movieID, (rating,1.0))
	movieRatings = line.map(parseInput)
	
	# Reduce to (movieID, (sumOfRatings, totalRatings))
	ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2:(movie1[0] + movie2[0]))
	
	# Map to (movieID, averageRating)
	avergaeRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalRatings)
	
	# Sort by average rating
	sortedMovies = averageRatings.sorteBy(lambda x_ x[1])
	
	# Take the top 10 results
	results = sortedMovies.take(10)
	
	# Print them out:
	for result in results:
		print(movieNames[result[0], result[1])
	


------------------------------ FILTER THE LOWEST-RATED MOVIES  -----------------------------------------
---------------------------------- BY NUMBER OF RATINGS ------------------------------------------------


from pyspark import SparkConf, SparkContext

# This function just creates a Python "dictionary" we can later
# user to convert movie ID's to movie names while printing out 
# the final results.

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames
	
	
# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average 

def parseInput(line):
		fields = line.split()
		return (int(fields[1]) , (float(fields[2]), 1.0))
		
if __name__ = "__main__":
	# The main script - create our SparkContext
	conf = SparkConf().setAppName("WorstMovies")
	sc = SparkContext(conf = conf)
	
	# Load up our movie ID -> movie name lookup table
	movieNames = loadMovieNames()
	
	# Load up the raw u.data file
	lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
	
	# Convert to (movieID, (rating,1.0))
	movieRatings = line.map(parseInput)
	
	# Reduce to (movieID, (sumOfRatings, totalRatings))
	ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2:(movie1[0] + movie2[0]),movie1[1] + movie2[1]) )
	
	# Filter out movies rated 10 or fewer times
	popularTotalsAndCount = ratingTotalsAndCount.filter(lambda x: x[1][1] > 10)
	
	# Map to (movieID, averageRating)
	avergaeRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalRatings)
	
	# Sort by average rating
	sortedMovies = averageRatings.sorteBy(lambda x: x[1])
	
	# Take the top 10 results
	results = sortedMovies.take(10)
	
	# Print them out:
	for result in results:
		print(movieNames[result[0], result[1])


	
	
--------------------------- FIND THE MOVIE WITH THE LOWEST AVERAGE -----------------------------------------
------------------------------------- RATING WITH DATA FRAMES ----------------------------------------------


--> P.D: WE NEED TO EXPORT VERSION 2 FOR RUN THIS.
export SPARK_MAJOR_VERSION=2
--> And that will allow us to use things like the Spark session object.

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames

	
def parseInput(line):
		fields = line.split()
		return Row(movieID = int(fields[1]) , rating = float(fields[2]))	


if __name__ = "__main__":
	# Create a sparkSession
	spark = SparkSession().builder.appName("PopularMovies").getOrCreate()
	
	# Load up our movie ID -> name dictionary
	movieNames = loadMovieNames()
	
	# Get the raw data
	lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
	
	# Convert it to a RDD of Row objects with (movieID, (rating,1.0))
	movies = lines.map(parseInput)
	
	# Convert thtat to a DataFrame
	movieDataset = spark.createDataFrame(movies)
	
	# Compute average rating for each movieID
	averageRatings = movieDataset.groupBy("movieID").avg("rating")
	
	# Compute count of ratings for each movieID
	counts = movieDataset.groupBy("movieID").count()
	
	# Join the two together (we now have movieID, avg(rating), and count
	averageAndCounts = counts.join(averageRatings, "movieID")
	
	# Pull the topp 10 results
	topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

	# Print them out:
	for movie in topTen:
		print(movieNames[movie[0], movie[1], movie[2])
	
	# Stop the session
    spark.stop()


--------------------------- MOVIE RECOMMENDATIONS WITH MLLib IN SPARK -----------------------------------------
---------------------------------------------------------------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1].decode('ascii','ignore')
	return movieNames

	
def parseInput(line):
		fields = line.value.split()
		return Row(userID = int(fields[0]) ,movieID = int(fields[1]) , rating = float(fields[2]))	


if __name__ = "__main__":
	# Create a sparkSession
	spark = SparkSession().builder.appName("PopularMovies").getOrCreate()
	
	# Load up our movie ID -> name dictionary
	movieNames = loadMovieNames()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	






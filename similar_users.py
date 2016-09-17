#For each user, find at least 10 people that like the same movies.
# [user , (movieid, rating) ]
from pyspark import SparkConf, SparkContext

sparkConf = SparkConf().setMaster("local[*]").setAppName("Spark 1")
sc = SparkContext(conf = sparkConf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

#get rating data in pairs
def userRatings(line):
    line = line.split(',')
    user = int(line[0])
    movieid = int(line[1])
    rating = float(line[2])
    return (user, (movieid,rating))

#get ratings higher than 3.5
def getHighRatings(pair):
    return pair[1][1] >= 3.5

ratings = sc.textFile("ratings.csv").sample(False, 0.001) #Take samples from data
headerR = ratings.first() 
userRatings = ratings.filter(lambda x: x!= headerR).map(userRatings).filter(getHighRatings)\
.groupByKey().map(lambda x : (x[0], list(x[1])))

#eliminate self cartesian results
def ignoreSelf(pairlist):
    return not pairlist[0][0] == pairlist[1][0]

print userRatings.take(5)

#if two user have rated same movie, add these two in a list
def findSimilarity(pair):
    similarUser = ()
    if pair[0][1] == pair[1][1]:    
        similarUser = [pair[0][0],pair[1][0]]
    else:
        similarUser = [0] 
    return similarUser

#filter users that has no similar user
def filterSimilar(line):
    return not 0 in line

#choose ten users from list for each user
def tenSimilarUsers(list):
    return list[:10]

similarUsers = userRatings.cartesian(userRatings).filter(ignoreSelf).map(findSimilarity).filter(filterSimilar)\
.groupByKey().map(lambda x : ("user id:", x[0], "similar users:", tenSimilarUsers(list(x[1])))) 

print similarUsers.take(10)
print similarUsers.saveAsTextFile("SimilarUsers")
print "finished"
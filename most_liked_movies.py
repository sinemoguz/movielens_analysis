#Find the most liked movies and their average ratings for each tag.
# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import operator

sparkConf = SparkConf().setMaster("local[*]").setAppName("Spark 1")

sc = SparkContext(conf = sparkConf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

#count rating number for each film
def movieRatingCount(line):
    movieid = line.split(',')[1]
    count = 1
    return (movieid, count)

#obtain movie and rating data
def splitLines(line):
    line = line.split(",")
    movieid = line[1]
    rating = line[2]
    return (movieid, float(rating))

#create a movie dictionary, this will be used to find movie titles from movie ids
def movies_dictionary():
    movies_dict={}
    with open("movies.csv") as f:
        for each_movie in f:
            elementCount = len(each_movie.split(','))
    
            if elementCount == 3:
                movieid = each_movie.split(',')[0]
                title = each_movie.split(',')[1]
            else:
                movieid = each_movie.split('"')[0].replace(",", "")
                title = each_movie.split('"')[1]
            movies_dict[movieid]=title
    return (movies_dict)

movies_bcast=sc.broadcast(movies_dictionary())

#calculate average rating for each film: total rating/total rating count
def findAverageRating(pair):
    movie = pair[0]
    avgRating = (float(pair[1][0])/pair[1][1])
    return (movie, avgRating)
    
#Take ratings data and extract header
ratings = sc.textFile("ratings.csv")
headerR = ratings.first()
ratings = ratings.filter(lambda x: x!= headerR)

#rdd that computes total rating count for each film and sort by rating counts ascending
ratingCount = ratings.map(movieRatingCount).reduceByKey(lambda x,y: x+y).map(lambda (movieid,count):(count,movieid))\
.sortByKey(False).map(lambda (count,movieid):(movieid,count))

#rdd that computes total rating  for each film and sort by rating ascending
totalRating = ratings.map(splitLines).reduceByKey(lambda x,y: x+y).map(lambda (movieid,rating):(rating,movieid))\
.sortByKey(False).map(lambda (rating,movieid):(movieid,rating))

##rdd that computes average rating for each film and sort by rating ascending
avgRatingRDD = totalRating.join(ratingCount).map(findAverageRating).map(lambda (movieid,avgRating):(avgRating,movieid))\
.sortByKey(False).map(lambda (avgRating,movieid):(movieid,avgRating))

print avgRatingRDD.take(5)

#find movie names to obtain more readable results
ratingCountWTitle= avgRatingRDD.map(lambda (movieid,avgRating):(movieid, movies_bcast.value[movieid],avgRating))

#obtain movie and tag data
def splitTagFile(line):
    line = line.split(',')
    movieid = line[1]
    tags = line[2]
    return (movieid,tags)

#organize data after join operation
def organizeData(pair):
    movieid = pair[0]
    tags = pair[1][0]
    avgRating = pair[1][1]
    return (tags, (avgRating,movieid)) 

#get maximum rating for each tag
def getMax(dict):
    rating = max(dict.iteritems(), key=operator.itemgetter(1))[0]
    movieid = max(dict.iteritems(), key=operator.itemgetter(1))[1]
    title = movies_bcast.value[movieid]
    return (rating, movieid, title) 


#Take tags data and extract header
tags = sc.textFile("tags.csv")
headerT = tags.first()
tags = tags.filter(lambda x: x!= headerT).map(splitTagFile)

#this rdd includes joined data of avg rating for each film and films with tags. 
#after join I organized data and calculated max rating for each tag
tagsJoinedMovies = tags.join(avgRatingRDD).map(organizeData).groupByKey().map(lambda x : (x[0], getMax(dict(x[1]))))

#result set
print tagsJoinedMovies.take(10)
print tagsJoinedMovies.saveAsTextFile("TagsWithRatings")
print "finished"


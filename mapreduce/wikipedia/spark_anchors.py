import sys
from pyspark import SparkContext


sc = SparkContext(appName="WikipediaAnchors")

lines = sc.textFile(sys.argv[1])

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.saveAsTextFile(sys.argv[2])

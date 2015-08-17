import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext(appName="WikipediaAnchors")
ssc = StreamingContext(sc, 1)

lines = ssc.textFileStream(sys.argv[1])

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.saveAsTextFiles(sys.argv[2])

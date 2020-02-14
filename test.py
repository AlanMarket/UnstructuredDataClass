from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

 

import random

input_data_path = '../week1/big_data_intro.txt'
text_file = sc.textFile(input_data_path)

word_to_length_pair_rdd = text_file.flatMap(lambda line: line.split(" ")) \
.map(lambda word: (word, random.random()))

print(word_to_length_pair_rdd.take(10))

 

word_to_length_pair_keys_rdd = word_to_length_pair_rdd.keys()

print(word_to_length_pair_keys_rdd.take(10))

 

word_to_length_pair_group_rdd = word_to_length_pair_rdd.groupByKey()

group_result = word_to_length_pair_group_rdd.take(10)
for key, values in group_result:
print('key = {}'.format(key))
for value in values:
print(value)

 

bigdata_word_to_count_pair_rdd = sc.textFile('../week1/big_data_intro.txt') \
.flatMap(lambda line: line.split(" ")) \
.map(lambda word: (word, 1)) \
.reduceByKey(lambda a, b: a + b)
#bigdata_word_to_count_pair_rdd = bigdata_word_to_count_pair_rdd.repartition(len(bigdata_word_to_count_pair_rdd.collect()))

print(bigdata_word_to_count_pair_rdd.take(10))

hamlet_word_to_count_pair_rdd = sc.textFile('./hamlet.txt') \
.flatMap(lambda line: line.split(" ")) \
.map(lambda word: (word, 1)) \
.reduceByKey(lambda a, b: a + b)
#hamlet_word_to_count_pair_rdd = hamlet_word_to_count_pair_rdd.repartition(len(hamlet_word_to_count_pair_rdd.collect()))

print(hamlet_word_to_count_pair_rdd.take(10))

 

bigdata_join_hamlet_rdd = bigdata_word_to_count_pair_rdd.join(hamlet_word_to_count_pair_rdd)
print(bigdata_join_hamlet_rdd.collect())

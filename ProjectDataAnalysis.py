import pyspark
import csv

sc = pyspark.SparkContext.getOrCreate()

rdd = sc.textFile("s3n://donors-choose/Projects.csv")

rdd = rdd.mapPartitions(lambda x: csv.reader(x))
header = rdd.first()
rdd_pro = rdd.filter(lambda x: x != header).filter(lambda x: len(x) != 0)

# get the number of distinct projects
num_pro = rdd_pro.map(lambda x: x[0]).distinct().count()
print("The total number of projects posted online is: " + str(num_pro) + "\n")

# map into key,value pair: (Project Resource Category,1)
pro_category_type_pair = rdd_pro.filter(
    lambda x: len(x) == 18).map(lambda x: (x[12], 1))

# get the per resource Category post numbers
pro_category_count = pro_category_type_pair.groupByKey().mapValues(lambda x: len(x))

print("Projects category :" + " Total number of posts " + "\n")
for p in pro_category_count.collect():
    print(str(p[0]) + " : " + str(p[1]) + "\n")

# basic stat of Project cost
cost_sum = rdd_pro.filter(lambda x: len(x) == 18).map(
    lambda x: float(x[13])).sum()
cost_mean = rdd_pro.filter(lambda x: len(x) == 18).map(
    lambda x: float(x[13])).mean()
cost_max = rdd_pro.filter(lambda x: len(x) == 18).map(
    lambda x: float(x[13])).max()
cost_min = rdd_pro.filter(lambda x: len(x) == 18).map(
    lambda x: float(x[13])).min()

print("The total cost of projects posted online is: " +
      " $" + str(round(cost_sum, 2))+ "\n")
print("The mean cost of projects posted online is: " +
      " $" + str(round(cost_mean, 2))+ "\n")
print("The max cost of projects posted online is: " +
      " $" + str(round(cost_max, 2))+ "\n")
print("The min cost of projects posted online is: " +
      " $" + str(round(cost_max, 2))+ "\n")

sc.stop()
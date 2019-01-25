import pyspark
import csv

sc = pyspark.SparkContext.getOrCreate()

donation = sc.textFile("s3n://694zeng/Donations.csv")

header = donation.first()

donation = donation.filter(lambda line: line != header)
donation = donation.map(lambda line: line.split(','))

donor_ids = donation.map(lambda line: line[2])
num_donors = donor_ids.distinct().count()
print("The total number of donors is: " + str(num_donors) + "\n")

optional_pair = donation.map(lambda x: (x[3], 1))
optional_count = optional_pair.groupByKey().mapValues(lambda x: len(x))

print("Included optional donation :" + " Total number of donations " + "\n")
for p in optional_count.collect():
    print(str(p[0]) + " : " + str(p[1]) + "\n")
    
donation_sum = donation.map(lambda x: float(x[4])).sum()
donation_mean = donation.map(lambda x: float(x[4])).mean()
donation_stdev = donation.map(lambda x: float(x[4])).stdev()
donation_max = donation.map(lambda x: float(x[4])).max()
donation_min = donation.map(lambda x: float(x[4])).min()

print("The total donation amount is: " +
      " $" + str(round(donation_sum, 2))+ "\n")
print("The mean donation amount is: " +
      " $" + str(round(donation_mean, 2))+ "\n")
print("The standard deviaton of donation amount is: " +
      " $" + str(round(donation_stdev, 2))+ "\n")
print("The min donation amount is: " +
      " $" + str(round(donation_min, 2))+ "\n")
print("The max donation amount is: " +
      " $" + str(round(donation_max, 2))+ "\n")

sc.stop()

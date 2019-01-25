# Data Processing and Basic Analysis using PySpark

Open Database: https://www.kaggle.com/donorschoose/io

DonorsChoose.org is a non-profit organization that allows individuals to donate directly
to public classroom projects. In 77% of public schools in the United States, at least one project has been
requested on DonorsChoose.org. From 2013-2018, 3 million people and partners have funded 1.1 million
DonorsChoose.org projects from teachers. This dataset contains all informations about the past projects.

The data was loaded on AWS S3, then queried and analysed on different AWS EMR cluster settings (Core, Instance Count, Memory,
SSD Storage).

The script did some exploratory statistic analysis on the Projects and Donations datasets to get a picture
of educational need and charitable giving:

1) The total cost of projects posted
2) The mean cost of projects posted
3) The max/min cost of projects posted
4) The total donation amount
5) The standard deviaton of donation amount 
6) The max/min donation amount
7) The total number of donors
8) The total number of donations incluede optional donations or not

These statistics gave us insight into the average amount of charitable giving, how
much variation there was, and helped to understand how many donors truly went above and beyond
to meet the needs in American classrooms.

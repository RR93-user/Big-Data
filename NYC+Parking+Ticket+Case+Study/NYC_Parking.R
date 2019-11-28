#Initialzing SparkR
spark_path <- '/usr/local/spark'

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "yarn", sparkConfig = list(spark.driver.memory = "1g"))

#Reading data
NYC_Parking <- read.df('/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv', source="csv", inferSchema="true", header="true")


##################################### Examine Data  ################################################

# Understanding the dataset
head(NYC_Parking)
nrow(NYC_Parking)
ncol(NYC_Parking)
str(NYC_Parking)
printSchema(NYC_Parking)

# Summarisation of data
df <- collect(describe(NYC_Parking))
df

# Creating a calculated column "Year" in the dataframe
NYC_Parking$Year <- substr(NYC_Parking$`Issue Date`,1,4)

# Filtering the dataset for 2017
NYC_Parking_2017 <- subset(NYC_Parking, NYC_Parking$Year=="2017")
nrow(NYC_Parking_2017)

ParkingTickets <- collect(select(NYC_Parking_2017, countDistinct(NYC_Parking_2017$`Summons Number`)))
ParkingTickets

# Counting Number of Unique states
Count_State <- collect(select(NYC_Parking_2017, countDistinct(NYC_Parking_2017$`Registration State`)))
Count_State
# 65 unique states at first analysis

# Finding the state with highest parking tickets
State_counts <- summarize(groupBy(NYC_Parking_2017, NYC_Parking_2017$`Registration State`), count = n(NYC_Parking_2017$`Summons Number`))
head(arrange(State_counts, desc(State_counts$count)))
# NY has the highest parking tickets

# Replacing '99' with 'NY' in Registration State
NYC_Parking_2017$`Registration State` <- ifelse(NYC_Parking_2017$`Registration State`==99,"NY",NYC_Parking_2017$`Registration State`) 

# Counting Number of Unique states again
Count_State <- collect(select(NYC_Parking_2017, countDistinct(NYC_Parking_2017$`Registration State`)))
Count_State
# 64 unique states after data cleaning.

# Creating Temp View for SQL in R
createOrReplaceTempView(NYC_Parking_2017 , "Parking_2017")
# Adding jar file
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")


#####################################   Aggregation Tasks  ################################################

# Top five Violation Codes
Voilation_Frequency <- SparkR::sql("SELECT `Violation Code`, count(*) as frequency FROM Parking_2017 GROUP BY `Violation Code` ORDER BY frequency DESC")
head(Voilation_Frequency)


### Question 2 ###

# Top five Vehicle Body types
VehicleBodyType_Frequency <- SparkR::sql("SELECT `Vehicle Body Type`, count(*) as frequency FROM Parking_2017 GROUP BY `Vehicle Body Type` ORDER BY frequency DESC")
head(VehicleBodyType_Frequency)

# Top five Vehicle Make
VehicleMake_Frequency <- SparkR::sql("SELECT `Vehicle Make`, count(*) as frequency FROM Parking_2017 GROUP BY `Vehicle Make` ORDER BY frequency DESC")
head(VehicleMake_Frequency)


### Question 3 ###

# Top five Violation Precinct
ViolationPrecinct_Frequency <- SparkR::sql("SELECT `Violation Precinct`, count(*) as frequency FROM Parking_2017 GROUP BY `Violation Precinct` ORDER BY frequency DESC")
head(ViolationPrecinct_Frequency)

# Top five Issuer Precinct
IssuerPrecinct_Frequency <- SparkR::sql("SELECT `Issuer Precinct`, count(*) as frequency FROM Parking_2017 GROUP BY `Issuer Precinct` ORDER BY frequency DESC")
head(IssuerPrecinct_Frequency)


### Question 4 ###

# Violation Code frequency in top 3 Issuer Percinct
Top3Percinct_ViolationPrecinct_Frequency<- SparkR::sql("SELECT `Issuer Precinct`, `Violation Code`, count(*) as frequency FROM Parking_2017 WHERE `Issuer Precinct`=19 or `Issuer Precinct`=14 or `Issuer Precinct`=1  GROUP BY `Issuer Precinct`, `Violation Code` ORDER BY frequency DESC")
head(Top3Percinct_ViolationPrecinct_Frequency)


### Question 5 ###

# Missing values in Violation Time ?
ViolationTime_Null<- SparkR::sql("SELECT SUM(CASE WHEN `Violation Time` is NUll THEN 1 END) as Null_Count FROM Parking_2017")
head(ViolationTime_Null)

# Using dprona to clean the null values in the dataset
dropna(NYC_Parking_2017, how="any")


# Converting 12hr format to 24hrs format
NYC_Parking_2017$Violation_Time_24hrs <- ifelse( substr(NYC_Parking_2017$`Violation Time`,5,5)=="A", substr(NYC_Parking_2017$`Violation Time`,1,4), (substr(NYC_Parking_2017$`Violation Time`,1,4)+1200))
cast(NYC_Parking_2017$Violation_Time_24hrs,"integer")

# Creating a new view with the changes to the data frame
createOrReplaceTempView(NYC_Parking_2017 , "Parking_2017_New")


# Adding a new column 'bin_time' to the data set based on Violation Time
bins <- SparkR::sql("SELECT \ 
             CASE  WHEN (Violation_Time_24hrs >= 0 and Violation_Time_24hrs < 0400) THEN 'Late Night'\
                   WHEN (Violation_Time_24hrs >= 0400 and Violation_Time_24hrs < 0800) THEN 'Ealry Morning'\
                   WHEN (Violation_Time_24hrs >= 0800 and Violation_Time_24hrs < 1200) THEN 'Morning'\
                   WHEN (Violation_Time_24hrs >= 1200 and Violation_Time_24hrs < 1600) THEN 'Afternoon'\
                   WHEN (Violation_Time_24hrs >= 1600 and Violation_Time_24hrs < 2000) THEN 'Evening'\
                   WHEN (Violation_Time_24hrs >= 2000 and Violation_Time_24hrs < 2400) THEN 'Night'\
              ELSE 'ERROR' END as bin_time, `Violation Code` FROM Parking_2017_New")


# Filtering & Grouping for each bin
Late_Night <- summarize(groupBy(filter(bins, bins$bin_time == 'Late Night'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Early_Morning <- summarize(groupBy(filter(bins, bins$bin_time == 'Ealry Morning'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Morning <- summarize(groupBy(filter(bins, bins$bin_time == 'Morning'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Afternoon <- summarize(groupBy(filter(bins, bins$bin_time == 'Afternoon'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Evening <- summarize(groupBy(filter(bins, bins$bin_time == 'Evening'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Night <- summarize(groupBy(filter(bins, bins$bin_time == 'Night'), bins$`Violation Code`), count = n(bins$`Violation Code`))

# Sorting & Displaying the frequency of Violation Codes in each bin
head(arrange(Late_Night, desc(Late_Night$count)))
head(arrange(Early_Morning, desc(Early_Morning$count)))
head(arrange(Morning, desc(Morning$count)))
head(arrange(Afternoon, desc(Afternoon$count)))
head(arrange(Evening, desc(Evening$count)))
head(arrange(Night, desc(Night$count)))


# Filtering & Grouping for each Violation Code
ViolationCode_21 <- summarize(groupBy(filter(bins, bins$`Violation Code` == 21), bins$bin_time), count = n(bins$bin_time))
ViolationCode_36 <- summarize(groupBy(filter(bins, bins$`Violation Code` == 36), bins$bin_time), count = n(bins$bin_time))
ViolationCode_38 <- summarize(groupBy(filter(bins, bins$`Violation Code` == 38), bins$bin_time), count = n(bins$bin_time))

# Sorting & Displaying the frequency of Violation Codes in each bin
head(arrange(ViolationCode_21, desc(ViolationCode_21$count)))
head(arrange(ViolationCode_36, desc(ViolationCode_36$count)))
head(arrange(ViolationCode_38, desc(ViolationCode_38$count)))


### Question 6 ###

# Seperating Month from Issue Date
NYC_Parking_2017$Month <- substr(NYC_Parking_2017$`Issue Date`,6,7)
cast(NYC_Parking_2017$Month,"integer")

# Replacing the view with the changes to the data frame
createOrReplaceTempView(NYC_Parking_2017 , "Parking_2017_New")

# Adding a new column 'bin_season' to the data set based on Issue Date
Season_bins <- SparkR::sql("SELECT \ 
             CASE  WHEN (Month = 12 OR Month = 1 OR Month = 2) THEN 'Winter'\
                   WHEN (Month >= 3 AND Month <= 5) THEN 'Spring'\
                   WHEN (Month >= 6 AND Month <= 8) THEN 'Summer'\
                   WHEN (Month >= 9 AND Month <= 11) THEN 'Fall'\
              ELSE 'ERROR' END as bin_season, `Violation Code` FROM Parking_2017_New")

# Filtering & Grouping for each Season
Winter <- summarize(groupBy(filter(Season_bins, Season_bins$bin_season == 'Winter'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Spring <- summarize(groupBy(filter(Season_bins, Season_bins$bin_season == 'Spring'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Summer <- summarize(groupBy(filter(Season_bins, Season_bins$bin_season == 'Summer'), bins$`Violation Code`), count = n(bins$`Violation Code`))
Fall <- summarize(groupBy(filter(Season_bins, Season_bins$bin_season == 'Fall'), bins$`Violation Code`), count = n(bins$`Violation Code`))

# Sorting & Displaying the frequency of Violation Codes in each Season
head(arrange(Winter, desc(Winter$count)))
head(arrange(Spring, desc(Spring$count)))
head(arrange(Summer, desc(Summer$count)))
head(arrange(Fall, desc(Fall$count)))


### Question 7 ###

# Total occurance of violation codes 21, 36 & 38
Occurance <- SparkR::sql("SELECT count(*) FROM Parking_2017_New WHERE `Violation Code`=21 OR `Violation Code`=36 OR `Violation Code`=38")
head(Occurance)

# Total amount collected for violation codes 21, 36 & 38
Amount_21 <- SparkR::sql("SELECT count(*)*55 FROM Parking_2017_New WHERE `Violation Code`=21")
Amount_36 <- SparkR::sql("SELECT count(*)*55 FROM Parking_2017_New WHERE `Violation Code`=36")
Amount_38 <- SparkR::sql("SELECT count(*)*55 FROM Parking_2017_New WHERE `Violation Code`=38")
head(Amount_21)
head(Amount_36)
head(Amount_38)

# Stopping R session
sparkR.stop()

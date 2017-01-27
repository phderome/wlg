# Private  p a y   TM  L A B s

## Build and run
Copy a suitable data file such as `2015_07_22_mktplace_shop_web_log_sample.log.gz` to the `data` folder.
```
sbt package
spark-submit --class "phderome.challenge.WeblogChallenge"   --master local[4] target/scala-2.11/paytmphderome_2.11-1.0.jar  
data/2015_07_22_mktplace_shop_web_log_sample.log.gz
```

## Results

- 15 minutes (900000000 nanoseconds) window of inactivity, which is run-time parameter assuming that default, a window must contain two or more time samples
- computation of times is done using epoch time (seconds from Unix epoch 1970) but including nanoseconds, which fit in 64 bits Longs.
  
##### 1. Sessionize the web log by IP (aggregate all page hits, so count them).

Stored in `output/SessionizedCounts` directory as parquet or standard output for a sample in log
```
************************* Sessionized Count: (1.186.41.2,10)
************************* Sessionized Count: (1.186.98.61,3)
************************* Sessionized Count: (1.187.228.70,1)
************************* Sessionized Count: (1.22.184.42,6)
************************* Sessionized Count: (1.22.236.109,8)
************************* Sessionized Count: (1.23.136.80,4)
************************* Sessionized Count: (1.23.61.66,26)
************************* Sessionized Count: (1.23.67.41,1)
************************* Sessionized Count: (1.38.18.86,16)
************************* Sessionized Count: (1.38.19.21,6)
************************* Sessionized Count: (1.38.19.229,4)
************************* Sessionized Count: (1.38.21.36,11)
************************* Sessionized Count: (1.38.4.8,2)
************************* Sessionized Count: (1.39.13.242,1)
************************* Sessionized Count: (1.39.13.44,1)
************************* Sessionized Count: (1.39.14.174,87)
************************* Sessionized Count: (1.39.14.83,6)
************************* Sessionized Count: (1.39.15.215,10)
************************* Sessionized Count: (1.39.32.224,1)
************************* Sessionized Count: (1.39.33.197,17)
```

##### 2. Average session time
Stored in: `output/AvgDuration.txt`
```
8.924094 seconds
```

##### 3. Unique URL visits per session.
Stored in: `output/UniqueUrl.txt`
```
811984 113375 => avg unique per session 7.1619
```

##### 4. Find IPs with the longest session times
Stored in `output/EngagedClients` directory as parquet or standard output for a sample in log
```
*************************  engaged: client: 117.200.4.141 899.963580
*************************  engaged: client: 14.99.239.11 899.750790
*************************  engaged: client: 122.169.109.88 899.724810
*************************  engaged: client: 14.139.126.167 899.396050
*************************  engaged: client: 136.185.242.32 899.363350
*************************  engaged: client: 117.201.58.181 899.359500
*************************  engaged: client: 125.99.121.178 899.262530
*************************  engaged: client: 220.227.161.206 899.205955
*************************  engaged: client: 223.227.31.248 899.076400
*************************  engaged: client: 164.100.146.178 898.938910
*************************  engaged: client: 182.72.7.101 898.483600
*************************  engaged: client: 103.231.44.164 898.412900
*************************  engaged: client: 182.66.46.104 898.398560
*************************  engaged: client: 210.212.246.129 898.271150
*************************  engaged: client: 183.82.106.197 898.248400
*************************  engaged: client: 202.191.244.122 898.082490
*************************  engaged: client: 112.133.233.120 897.858810
*************************  engaged: client: 202.87.35.226 897.648725
*************************  engaged: client: 117.244.76.150 897.550850
*************************  engaged: client: 106.206.129.105 897.406690
```
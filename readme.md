This project aims to predict the outcome of IPL matches by using the concept of Probablities and Machine Learning. It uses Big Data technologies like MapReduce, Spark MLLib and clustering using Scala. The project was undertaken as an academic project for Big Data Course.

Phase-1 Data Collection:
The data was collected from www.cricsheet.org . It was downloaded in yaml format and then converted to csv format. Then, the runs scored and wickets taken was calculated for each batsman-bowler pair.

Phase-2 Clustering:
The batsmen and bowlers were divided into cluster to make up for unidentified pair. K-Means clustering was used to form clusters. n_Clusters:10 

Phase-3: Prediction using Probabilities:
The data derived in phase 1 is used to calculate probabilities for each batsman-bowler pair in format(batsman,bowler,0,1,2,3,4,6,wickets). If unknown pair was found, cluster number was used and respective cluster-pair probabilities was used. 
Runs: A random number was generated in the range(0,prob(6)) and checked where it lies. This gives us the runs scored in that ball.
Wickets: The probability of remaining not-out is set to one. The probability of remaining not-out is reduced each time a batsman faces a ball. When this reaches below a certain threshold, the player is deemed OUT.
This loop is run until wickets != 10 and balls<120.

Phase-4 Machine Learning Approach:
Spark MLLib was used to setup Decision Tree approach for machine learning. Two models were setup, 1 for runs and 1 for wickets. If wicket gives true the runs were ignored, else it was added to the total.

The overall efficiency of the project was pretty high. It predicted 8/10 results although the runs scored were not so much accurate.

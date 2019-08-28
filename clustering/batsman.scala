import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
//val data = sc.textFile("hdfs://localhost:54310/testmapred/batsman.csv")
//val test = sc.textFile("hdfs://localhost:54310/testmapred/bowler.csv")
val data = sc.textFile("file:///home/mehul/Downloads/New_BData/profiles/bat_profile.csv")

val header = data.first

val rows = data.filter(l => l != header)

// define case class

case class CC1(Player: String, Avg: Double, SR: Double)
//Player	Mat	Innings	NO	Runs	HS	Avg	BF	SR	100	50	0	4s	6s
// comma separator split

val allSplit = rows.map(line => line.split(","))

// map parts to case class

val allData = allSplit.map( p => CC1( p(0).toString, p(1).trim.toDouble, p(2).trim.toDouble))

// convert rdd to dataframe

val allDF = allData.toDF()

// convert back to rdd and cache the data

val rowsRDD = allDF.rdd.map(r => (r.getString(0), r.getDouble(1), r.getDouble(2)))

rowsRDD.cache()

val vectors = allDF.rdd.map(r => Vectors.dense( r.getDouble(1),r.getDouble(2) ))

vectors.cache()

val kMeansModel = KMeans.train(vectors, 10, 998)

kMeansModel.clusterCenters.foreach(println)

// Get the prediction from the model with the ID so we can link them back to other information

val predictions = rowsRDD.map{r => (r._1, kMeansModel.predict(Vectors.dense(r._2, r._3) ))}

// convert the rdd to a dataframe

val predDF = predictions.toDF("Player", "CLUSTER")
predDF.rdd.saveAsTextFile("new_bat_clustering")


val debus = sc.textFile("file:///home/mehul/Downloads/New_BData/profiles/debu_bat.csv")
val allSplit_debus = debus.map(line => line.split(","))

val allData_debus = allSplit_debus.map( p => CC1( p(0).toString, p(1).trim.toDouble, p(2).trim.toDouble))

val allDF_debus = allData_debus.toDF()

val rowsRDD_debus = allDF_debus.rdd.map(r => (r.getString(0), r.getDouble(1), r.getDouble(2)))

val predictions_debus = rowsRDD_debus.map{r => (r._1, kMeansModel.predict(Vectors.dense(r._2, r._3) ))}

val predDF_debus = predictions_debus.toDF("Player", "CLUSTER")
predDF_debus.rdd.saveAsTextFile("debu_bat_cluster")

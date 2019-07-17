import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("Insufficient number of args")
    }

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    val data : RDD[String] = sc.textFile(args(0)).map(_.toLowerCase())

    val alpha = 0.15

    val header = data.first()
    val links = data.filter(row => row != header).map(s => {
      val pairs = s.split(",")
      (pairs(0), pairs(1))
    }).groupByKey()

    val total = links.count()
    var ranks = links.mapValues(v => 10.0)

    for (i <- 1 to args(1).toInt) {
      val ranksFromAirports = links.join(ranks).values.flatMap(toNodesPageRank => {
        val fromAirportRank = toNodesPageRank._2
        val outLinks = toNodesPageRank._1.size
        val toAirportsList = toNodesPageRank._1

        toAirportsList.map(toPage => {
          val rankFromAirport = fromAirportRank / outLinks
          (toPage, rankFromAirport)
        })
      })
      ranks = ranksFromAirports.reduceByKey(_ + _).mapValues(rank => (1 - alpha) * rank + alpha / total)
    }
    ranks = ranks.sortBy(_._2, false)

    val result = ranks.map(airportRank => airportRank._1 + "\t" + airportRank._2)
    result.saveAsTextFile(args(2))
    sc.stop()
  }
}

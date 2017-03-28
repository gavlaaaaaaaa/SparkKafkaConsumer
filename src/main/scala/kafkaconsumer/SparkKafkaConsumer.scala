import org.apache.spark._
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json._
import org.apache.spark.SparkContext._
import org.apache.kudu.spark.kudu._


object SparkKafkaConsumer{

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafkaConsumer")
    val ssc = new StreamingContext(conf, Seconds(60))


    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "camus", Map(("twitter-topic", 1)))

    val lines = kafkaStream.map(x => x._2)

    val sqlContext = new SQLContext(ssc.sparkContext)

    // Use KuduContext to create, delete, or write to Kudu tables
    val kuduContext = new KuduContext("localhost:7051")

    lines.foreachRDD{ rdd =>
      //extract the tweet text from each tweet
      rdd.take(10).foreach(println)
      val tweetText = rdd.map(tweet => (Json.parse(tweet).\("id").as[Int], Json.parse(tweet).\("text").as[String]
                                                                                      .replaceAll("[\\s]+"," ") //get rid of all the spaces
                                                                                       .replaceAll("https?|bigdata|BigData|Bigdata", "")
                                                                                      .split("\\s").toList)) //split into a list of words
      import sqlContext.implicits._

      val df = tweetText.toDF("id", "text")
      val ngram = new NGram().setInputCol("text").setOutputCol("ngrams").setN(3)
      val ngramDataFrame = ngram.transform(df)

      val ngrams = ngramDataFrame.map(frame => frame.getAs[Stream[String]]("ngrams").toList)
        .flatMap(x => x)
        .map(ngram => (ngram,1))
        .reduceByKey((v1,v2) => v1 +v2)
        .map(tuple => (tuple._2, tuple._1))
        .sortByKey(false).map(tup => (tup._2, tup._1)).toDF("ngram", "count")

      ngrams.show(10)
      // Insert data
      kuduContext.insertRows(ngrams, "twitter_ngram")

    }

    ssc.start()
    ssc.awaitTermination()

  }


}
package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Network Word Count").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val docStream = ssc.textFileStream(args(0))

    // Task A
    val charOnlyWords = docStream.flatMap(rdd => rdd.split("\\s+")).filter(word => word.matches("[a-zA-Z]+"))
    val wordCounts = charOnlyWords.map(word => (word, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD { (rdd, time) =>
      if (rdd.count() > 0)
        rdd.saveAsTextFile(args(1) + "/Task_A/Task_A-" + time.milliseconds)
    }

    // Task B
    val wordPairStream = docStream.flatMap(eachLine => {
      val cleanedWords = eachLine.split("\\s+").filter(word => word.matches("[a-zA-Z]+") && word.length >= 5)
      for (i <- 0 until cleanedWords.length; j <- i + 1 until cleanedWords.length)
        yield ((cleanedWords(i), cleanedWords(j)), 1)
    })

    val coMatrix = wordPairStream.reduceByKey(_ + _)

    coMatrix.foreachRDD { (rdd, time) =>
      if (rdd.count() > 0)
        rdd.saveAsTextFile(args(1) + "/Task_B/Task_B-" + time.milliseconds)
    }

    // Task C
    ssc.checkpoint("./checkpoint/")

    val updatedCoMatrix = coMatrix.updateStateByKey[Int](updateCoMatrix _)

    updatedCoMatrix.foreachRDD { (rdd, time) =>
      if (rdd.count() > 0)
        rdd.saveAsTextFile(args(1) + "/Task_C/Task_C-" + time.milliseconds)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def updateCoMatrix(newPairCounts: Seq[Int], oldPairCount: Option[Int]): Option[Int] = {
    val updatedPairCount = oldPairCount.getOrElse(0) + newPairCounts.sum
    Some(updatedPairCount)
  }
}
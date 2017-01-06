import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Qloop on 2017/1/5.
  */
object FileObserverTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("FileObserverTest")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.textFileStream("E:\\tmp")
    val words = lines.flatMap(_.split("\n"))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    words.print()
//    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

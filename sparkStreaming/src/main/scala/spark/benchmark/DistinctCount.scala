package spark.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.matching.Regex

/**
 * Created by junjun on 2015/9/23.
 */
object DistinctCount {
	def main(args: Array[String]): Unit = {
		if (args.length != 5) {
			System.err.println("Usage: spark.benchmark.GrepBenchmark <hostname> <port> <receiver instance> <batchDuration> <up/down>")
			System.exit(1)
		}

		val (host, port, instance, batchDuration, direct) = (args(0), args(1).toInt, args(2).toInt, args(3).toInt, args(4))
		val direction: Boolean = direct match {
			case "up" => true
			case "down" => false
			case _ => System.err.println("direction must be \"up\" or \"down\"")
								System.exit(1)
								false
		}

		val conf = new SparkConf().setAppName("streaming-benchmark-DistinctCount")
		val ssc = new StreamingContext(conf, Seconds(batchDuration))
		val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(_.split(" "))
		val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _).map(x => (x._2, x._1))
		wordCount.foreachRDD(x => {
			val firstNum = x.sortByKey(direction).take(10 + 1)
			firstNum.take(10).foreach(println)
			if (firstNum.size > 10) println("...")
			println()
		})

		ssc.start()
		ssc.awaitTermination()
	}
}

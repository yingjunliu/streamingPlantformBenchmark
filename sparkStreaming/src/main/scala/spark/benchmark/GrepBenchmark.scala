package spark.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

import scala.util.matching.Regex

/**
 * Created by junjun on 2015/9/17.
 */
object GrepBenchmark {
	def main(args: Array[String]): Unit = {
		if (args.length != 5) {
			System.err.println("Usage: spark.benchmark.GrepBenchmark <hostname> <port> <receiver instance> <batchDuration> <regular expression>")
			System.exit(1)
		}

		val (host, port, instance, batchDuration, regular) = (args(0), args(1).toInt, args(2).toInt, args(3).toInt, args(4))

		val regex = new Regex(regular)

		def regexFunc(sourceString: String): Boolean = {
			//			sourceString match {
			//				case Some(regex(_*)) => true
			//				case _ => false
			//			}
			(regex findFirstMatchIn sourceString).nonEmpty
		}

		val conf = new SparkConf().setAppName("streaming-benchmark-GrepBenchmark")
		val ssc = new StreamingContext(conf, Seconds(batchDuration))
		val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(_.split(" ")).filter(x => regexFunc(x))
		val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)

		wordCount.print()
		ssc.start()
		ssc.awaitTermination()
	}
}

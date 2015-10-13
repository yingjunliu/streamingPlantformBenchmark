package spark.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.Predef._


/**
 * Created by junjun on 2015/9/23.
 */
object Sampling {
	def main(args: Array[String]): Unit = {
		if (args.length != 6) {
			System.err.println("Usage: spark.benchmark.GrepBenchmark <hostname> <port> <topic> <receiver instance> <batchDuration> <sampling ratio>")
			System.exit(1)
		}

		val (host, port, topics, instance, batchDuration, ratio) = (args(0), args(1).toInt, args(2), args(3).toInt, args(4).toInt, args(5).toDouble)
		val ratioInt = (ratio * 100).toInt

		def randomSampling(upper: Int): Boolean = {
			import scala.util.Random
			val seed = new Random()
			val nextInt = seed.nextInt(100)
			if(nextInt < upper) true else false
		}

		val conf = new SparkConf().setAppName("streaming-benchmark-Sampling")
		val ssc = new StreamingContext(conf, Seconds(batchDuration))

//		val Array(zkQuorum, group, topics, numThreads) = args
//		val topicMap = topics.split(",").map((_, 1)).toMap
		System.err.println(s"topics is $topics")
		val test = Predef.Map(topics -> 1)
		val lines = KafkaUtils.createStream(ssc, host + ":" + port, "test-group", test, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

//		val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(_.split(" ")).filter(x => true)

		val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
		wordCount.print()
		ssc.start()
		ssc.awaitTermination()
	}
}

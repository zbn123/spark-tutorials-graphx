package com.pinecone.tutorials.graphx

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leeivan on 8/19/16.
  */
object GraphVertexEdge {

  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[4]"
    }
    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName("GraphVertexEdge"))
    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 10L).map(id => (id, 1)))
    setA.collect.foreach(print)
    println
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 10L).flatMap(id => List((id, 1.0), (id, 2.0)))
    rddB.collect.foreach(print)
    println
    // There should be 200 entries in rddB
    println("count: " + rddB.count)
    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
    // There should be 100 entries in setB
    setB.collect.foreach(print)
    println
    println("count: " + setB.count)
    // Joining A and B should now be fast!
    val setC: VertexRDD[Double] = setA.innerJoin(setB)((id, a, b) => a + b)
    setC.collect.foreach(print)
  }
}

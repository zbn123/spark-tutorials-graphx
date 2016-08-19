package com.pinecone.tutorials.graphx

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leeivan on 8/18/16.
  */
object GraphPregelAPI {

  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[4]"
    }
    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName("GraphPregelAPI"))
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
    graph.vertices.collect.foreach(print)
    println
    println("-----------------------------------------------------------")
    graph.edges.collect().foreach(print)
    println
    println("-----------------------------------------------------------")
    val sourceId: VertexId = 0 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    initialGraph.vertices.collect.foreach(print)
    println
    println("-----------------------------------------------------------")
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {
        // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    sssp.collectNeighborIds(EdgeDirection.Out).collect.foreach(n => println(n._1 + "'s out neighbors : " + n._2.mkString(",")))
    println
    sssp.vertices.collect.foreach(print)
    println
    println("--------------------------------------------------------------------")
    sssp.edges.collect().foreach(print)
    println
    println("---------------------------------------------------------------------")

    val vertices01: RDD[(VertexId, (Int, Int))] =
      sc.parallelize(Array((1L, (7, -1)), (2L, (3, -1)),
        (3L, (2, -1)), (4L, (6, -1))))

    // Create an RDD for edges
    val relationships01: RDD[Edge[Boolean]] =
    sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true),
      Edge(2L, 4L, true), Edge(3L, 1L, true),
      Edge(3L, 4L, true)))

    // Create the graph
    val graph01 = Graph(vertices01, relationships01)

    // Check the graph
    graph01.vertices.collect.foreach(print)
    println
    println("-----------------------------------------------------")
    val initialMsg = 9999

    def vprog(vertexId: VertexId, value: (Int, Int), message: Int): (Int, Int) = {
      if (message == initialMsg)
        value
      else
        (message min value._1, value._1)
    }

    def sendMsg(triplet: EdgeTriplet[(Int, Int), Boolean]): Iterator[(VertexId, Int)] = {
      val sourceVertex = triplet.srcAttr

      if (sourceVertex._1 == sourceVertex._2)
        Iterator.empty
      else
        Iterator((triplet.dstId, sourceVertex._1))
    }

    def mergeMsg(msg1: Int, msg2: Int): Int = msg1 min msg2

    val minGraph = graph01.pregel(initialMsg,
      Int.MaxValue,
      EdgeDirection.Out)(
      vprog,
      sendMsg,
      mergeMsg)
//    minGraph.vertices.collect.foreach(print)
    minGraph.vertices.collect.foreach{
      case (vertexId, (value, original_value)) => println(value)
    }
  }
}

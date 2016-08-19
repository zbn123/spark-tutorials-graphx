package com.pinecone.tutorials.graphx.graphx

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leeivan on 8/19/16.
  */
object GraphBuild {

  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[4]"
    }
    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName("GraphBuild"))
    val graph = GraphLoader.edgeListFile(sc, "files/graphbuild.txt", true, -1)
    graph.vertices.collect.foreach(print)
    println
    graph.edges.collect.foreach(print)
    println
    println(graph.edges.partitions.size)

    val edges: RDD[Edge[String]] =
      sc.textFile("files/graphbuild01.txt").map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(2).toLong, fields(1))
      }
    val graph01: Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
    graph01.vertices.collect.foreach(print)
    println
    graph01.edges.collect.foreach(print)
    println
    println("num edges = " + graph01.numEdges);
    println("num vertices = " + graph01.numVertices);
  }
}

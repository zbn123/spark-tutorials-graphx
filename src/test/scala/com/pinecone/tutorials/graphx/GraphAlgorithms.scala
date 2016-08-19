package com.pinecone.tutorials.graphx

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leeivan on 8/16/16.
  */
object GraphAlgorithms {

  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[4]"
    }
    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName("GraphAlgorithms"))
    val graph = GraphLoader.edgeListFile(sc, "files/followers.txt")
    val users = sc.textFile("files/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    println("-----------------------------PageRank----------------------------")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    println("---------------------Connected Components---------------------------")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
    println("-----------------------Triangle Counting--------------------------------")
    // Load the edges in canonical order and partition the graph for triangle count
    val graph01 = GraphLoader.edgeListFile(sc, "files/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph01.triangleCount().vertices
    // Join the triangle counts with the usernames
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
  }
}

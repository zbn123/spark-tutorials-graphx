package com.pinecone.tutorials.graphx

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leeivan on 8/15/16.
  */
object GraphOperators {

  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[4]"
    }
    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName("GraphOperators"))
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)
    graph.vertices.collect.foreach(print)
    println("\n\n~~~~~~~~~ Confirm collectNeighborIds(OUT) ")
    graph.collectNeighborIds(EdgeDirection.Out).collect
      .foreach(n => println(n._1 + "'s out neighbors : " + n._2.mkString(",")))
    println("\n\n~~~~~~~~~ Confirm collectNeighborIds(IN) ")
    graph.collectNeighborIds(EdgeDirection.In).collect
      .foreach(n => println(n._1 + "'s in neighbors : " + n._2.mkString(",")))
    println("\n\n~~~~~~~~~ Confirm collectNeighborIds(Either) ")
    graph.collectNeighborIds(EdgeDirection.Either).collect
      .foreach(n => println(n._1 + "'s neighbors : " + n._2.distinct.mkString(",")))

    println("\n\n~~~~~~~~~ Confirm collectNeighbors(IN) ")
    graph.collectNeighbors(EdgeDirection.In).collect
      .foreach(n => println(n._1 + "'s in neighbors : " + n._2.mkString(",")))

    println("\n\n~~~~~~~~~ Confirm collectNeighbors(OUT) ")
    graph.collectNeighbors(EdgeDirection.Out).collect
      .foreach(n => println(n._1 + "'s out neighbors : " + n._2.mkString(",")))

    println("\n\n~~~~~~~~~ Confirm collectNeighbors(Either) ")
    graph.collectNeighbors(EdgeDirection.Either).collect
      .foreach(n => println(n._1 + "'s neighbors : " + n._2.distinct.mkString(",")))


    // Count all users which are postdocs
    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    println(graph.vertices.filter { case (name, pos) => pos == "postdoc" }.count)
    println("--------------------------------------------------------------------")
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count
    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    println("inDegrees")
    inDegrees.collect().foreach(print)
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println
    println("inDegrees Max: " + inDegrees.reduce(max))
    println("Property Operators: ")
    println("------------------------------------------------------------------")
    def mapUdf(a: Long, b: String, c: String): String = a.toString + "-" + b + "-" + c
    val newVertices = graph.vertices.map { case (id, attr) => (id, mapUdf(id, attr._1, attr._2)) }
    val newGraph01 = Graph(newVertices, graph.edges)
    println("newGraph01: ")
    newGraph01.vertices.collect().foreach(print(_))
    println
    println("newGraph02: ")
    val newGraph02 = graph.mapVertices((id, attr) => mapUdf(id, attr._1, attr._2))
    newGraph02.vertices.collect().foreach(print(_))
    println
    // Given a graph where the vertex property is the out degree
    println("outDegrees: ")
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.collect().foreach(print)
    println
    println("outDegrees Max: " + outDegrees.reduce(max))
    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank
    println("inputGraph: ")
    inputGraph.vertices.collect().foreach(print)
    println
    inputGraph.triplets.collect().foreach(print)
    println
    val outputGraph: Graph[Double, Double] =
      inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
    println("outputGraph: ")
    outputGraph.vertices.collect.foreach(print)
    println
    outputGraph.triplets.collect.foreach(print)
    println
    outputGraph.edges.collect.foreach(print)
    println
    println("----------------------------Structural Operators----------------------")
    val users01: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships01: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
      Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser01 = ("John Doe", "Missing")
    // Build the initial Graph
    val graph01 = Graph(users01, relationships01, defaultUser01)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph01.vertices.collect.foreach(print)
    println
    graph01.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    graph01.edges.collect.foreach(print)
    println
    graph01.reverse.edges.collect.foreach(print)
    println
    // Remove missing vertices as well as the edges to connected to them
    println("----------------------------subgraph-------------------")
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph01.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    val validGraph01 = graph01.subgraph(epred = e => e.attr == "student")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(print)
    println
    validGraph.edges.collect.foreach(print)
    println
    validGraph.triplets.collect.foreach(print)
    println
    validGraph01.vertices.collect.foreach(print)
    println
    validGraph01.edges.collect.foreach(print)
    println
    validGraph01.triplets.collect.foreach(print)
    println
    println("---------------------------------------------------------------------")
    // Run Connected Components
    val ccGraph = graph01.connectedComponents() // No longer contains missing field
    ccGraph.vertices.collect().foreach(print)
    println
    ccGraph.edges.collect.foreach(print)
    println
    // Remove missing vertices as well as the edges to connected to them
    println("----------------------mask----------------------------------")
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
    val validCCGraph01 = ccGraph.mask(validGraph01)
    validCCGraph.vertices.collect().foreach(print)
    println
    validCCGraph.edges.collect.foreach(print)
    println
    validCCGraph01.vertices.collect().foreach(print)
    println
    validCCGraph01.edges.collect.foreach(print)
    println
    println("----------------------------Join Operators---------------------")
    graph01.vertices.collect.foreach(print)
    println
    val outDegrees01: VertexRDD[Int] = graph01.outDegrees
    outDegrees01.collect.foreach(print)
    println
    val degreeGraph = graph01.outerJoinVertices(outDegrees01) { (id, oldAttr, outDegOpt) =>
      (oldAttr, outDegOpt match {
        case Some(outDeg) => outDeg
        case None => 0 // No outDegree means zero outDegree
      })
    }
    degreeGraph.vertices.collect.foreach(print)
    println
    println(degreeGraph.vertices.count)
    println("-----------------------Aggregate Messages----------------------------")
    // Import random graph generation library
    import org.apache.spark.graphx.util.GraphGenerators
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph03: Graph[Long, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100)
    graph03.vertices.take(10).foreach(print)
    println
    println("count: ")
    println(graph03.vertices.count)
    graph03.edges.take(10).foreach(print)
    println
    println("count: ")
    println(graph03.edges.count)
    graph03.triplets.take(10).foreach(print)
    println
    val graph04: Graph[Double, Int] = graph03.mapVertices((id, _) => id.toDouble)
    graph04.vertices.take(10).foreach(print)
    println
    graph04.edges.take(10).foreach(print)
    println
    graph04.triplets.take(10).foreach(print)
    println
    println("---------------------------------------------------------------------")

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, String)] = graph04.aggregateMessages[(Int, String)](
      triplet => {
        // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr.toString)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + "|" + b._2) // Reduce Function
    )
    olderFollowers.take(10).foreach(print)
    println()
    // Divide total age by number of older followers to get average age of older followers
    val olderFollowers01: VertexRDD[(Int, Double)] = graph04.aggregateMessages[(Int, Double)](
      triplet => {
        // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    olderFollowers01.take(10).foreach(print)
    println
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers01.mapValues((id, value) => value match {
        case (count, totalAge) => totalAge / count
      })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(print)
    println
    println("---------------------------------------------------------------------")
    // A graph with edge attributes containing distances

    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 100L).map(id => (id, 1)))
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 100L).flatMap(id => List((id, 1.0), (id, 2.0)))
    // There should be 200 entries in rddB
    rddB.count
    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
    // There should be 100 entries in setB
    setB.count
    // Joining A and B should now be fast!
    val setC: VertexRDD[Double] = setA.innerJoin(setB)((id, a, b) => a + b)
  }
}

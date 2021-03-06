package org.temporalframes

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{array, broadcast, col, count, explode, expr, monotonically_increasing_id, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.graphframes._
import org.graphframes.lib._
import org.graphframes.pattern._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.graphx._

import scala.collection.mutable

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx.{Graph, Edge, Pregel}
import org.graphframes._
import spark.implicits._

import scala.collection.mutable._
import scala.math._

class TemporalFrame(@transient private val _vertices: DataFrame,
                    @transient private val _edges: DataFrame) extends GraphFrame {

  override def vertices: DataFrame = _vertices
  override def edges: DataFrame = _edges
  def construct_timestamps(): Array[String] = {
    def split_string(x: String): String = {x.split("_")(1)}
    val columns = this._edges.columns
    val time_columns = columns.filter(n => n.startsWith("time_"))
    val time_stamps = time_columns.map(split_string).map(x => x).sorted
    time_stamps
  }

  var timestamps = construct_timestamps()

  def graph_snapshot(timestamp: String): GraphFrame = {
    try {
      val snapshot_edges = _edges.filter("time_" + timestamp + " > 0")
      val selectedColumns: Seq[Column] = snapshot_edges.columns.filterNot(_.startsWith("time_")).map(c => col(c))
      GraphFrame(_vertices, snapshot_edges.select(selectedColumns: _*))
    } catch {
      case e: Exception => {
        print("The timestamp does not exist in our snapshots")
        throw(e)
      }
    }
  }
  def topological_corr_coef(): Double = {
    val col_names = timestamps.map(x => "time_" + x)
    val unique_vals = this.edges.dropDuplicates("src").select("src").collect().map(_(0)).toArray.map(_.toString).map(_.toInt)
    var tot = 0.0
    var count = 0
    for (b <- unique_vals) {
      val subset = this.edges.filter(col("src") === b)
      for (a <- 0 to col_names.size - 2) {
        val num = (subset.withColumn("prod", col(col_names(a)) * col(col_names(a + 1))).agg(sum("prod"))
          .first.get(0).toString.toDouble)
        val denom = (math.sqrt(subset.agg(sum(col_names(a))).first.get(0).toString.toDouble *
          subset.agg(sum(col_names(a + 1))).first.get(0).toString.toDouble))
        if (denom != 0.0) {
          tot = tot + num / denom
        }
        count = count + 1
      }
    }
    if (count > 0) {
      tot / count
    } else {0.0}
  }

  def mean_arr(x: Seq[Int]): Double = {
    if (x.size != 0) {
      x.sum / x.size
    } else {
      0
    }
  }

  def std_arr(x: Seq[Int]): Double = {
    val mean = mean_arr(x)
    val y = x.map(c => math.pow(c - mean, 2)).sum
    math.sqrt(y)
  }

  def burstiness(): DataFrame = {
    val cols = this._edges.columns.filter(p => p.contains("time"))
    def generate_burstiness(x: WrappedArray[String]): Double = {
      val y = x.toArray[String].map(_.toDouble)
      var res_seq = Seq[Int]()
      var pos = 0
      for (i <- 0 until y.length) {
        if (y(i) > 0) {
          res_seq = res_seq :+ (i - pos)
          pos = i
        }
      }
      val mean = mean_arr(res_seq)
      val std = std_arr(res_seq)
      if ((std + mean) != 0){
      (std - mean) / (std + mean)
        } else {
        0
      }
    }
    def burst_udf = spark.udf.register("burst", generate_burstiness _)
    val columns = this._edges.columns
    val time_columns = columns.filter(n => n.startsWith("time_"))
    val edges_with_burst = this._edges.withColumn("col_arr",array(time_columns.map(c => col(c)):_*)).withColumn("burstiness", burst_udf(col("col_arr")))
    edges_with_burst.select("src", "dst", "burstiness")
  }

  def extractDouble(expectedNumber: Any): Double = {
    expectedNumber.toString.toDouble
  }

  def volatility(distance: String = "Hamming"): Double = {
    val time_columns = this._edges.columns.filter(n => n.startsWith("time_"))
    val edges_with_col = this._edges.withColumn("col_arr", array(time_columns.map(c => col(c)): _*))

    def hamming(x: WrappedArray[String]): Double = {
      var total: Double = 0.0
      var pos = x(0)
      for (i <- 0 until x.length)
      {
        if (pos != x(i)) {
          total = total + 1
        }
        pos = x(i)
      }
      total
    }

    def euclidean(x: WrappedArray[String]): Double = {
      var total: Double = 0.0
      val y = x.toArray[String].map(_.toDouble)
      var pos = y(0)
      for (i <- 0 until y.length) {
        total = total + math.pow(y(i) - pos, 2)
        pos = y(i)
      }
      math.sqrt(total)
    }

    val new_df = {
      if (distance == "Hamming") {
        def dist_hamming = spark.udf.register("hamm", hamming _)

        this.edges.withColumn("col_arr", array(time_columns.map(c => col(c)): _*)).withColumn("dist", dist_hamming(col("col_arr")))
      } else if (distance == "Euclidean") {
        def dist_euclidean = spark.udf.register("eucl", euclidean _)

        this.edges.withColumn("col_arr", array(time_columns.map(c => col(c)): _*)).withColumn("dist", dist_euclidean(col("col_arr")))
      } else {
        this.edges.withColumn("dist", lit(0))
      }
    }
    val new_df_filter = new_df.where(col("dist") !== 0)
    extractDouble(new_df_filter.agg(avg("dist")).collect()(0)(0))
  }


//  def temporal_pagerank(alpha: Double = 0.15, beta: Double = 1.0): DataFrame = {
//    val time_columns = this._edges.columns.filter(n => n.startsWith("time_"))
//    def normalize_tuple(x: Double, y: Double): (Double, Double) = {
//      var norm_factor = math.sqrt(x * x + y * y)
//      if (norm_factor == 0.0) {
//        norm_factor = 1.0
//      }
//      (x / norm_factor, y / norm_factor)
//    }
//    def edge_rank(x: WrappedArray[String], alpha: Double, beta: Double): Double = {
//      var r_src = 0.0
//      var r_dst = 0.0
//      var s_src = 0.0
//      var s_dst = 0.0
//      for (i <- 0 until x.length) {
//        if (x(i).toDouble > 0) {
//          r_src = r_src + (1 - alpha)
//          s_src = s_src + (1 - alpha)
//          r_dst = r_dst + s_src * alpha
//          if (beta  < 1) {
//            s_dst = s_dst + s_src * (1 - beta) * alpha
//            s_src = s_src * beta
//          } else {
//            s_dst = s_dst + s_src * alpha
//            s_src = 0
//          }
//        }
//      }
//      //normalize_tuple(r_src, r_dst)._1
//      r_src
//    }
//    def pr_udf = spark.udf.register("e_udf", edge_rank _)
//    val res = this.edges.withColumn("col_arr", array(time_columns.map(c => col(c)): _*)).withColumn("tpr", pr_udf(col("col_arr"), lit(alpha), lit(beta))).select("src", "dst", "tpr")
//
//  }




  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}

object TemporalFrame extends GraphFrame {
  val ID: String = "id"

  /**
   * Column name for source vertices of edges.
   *  - In [[GraphFrame.edges]], this is a column of vertex IDs.
   *  - In [[GraphFrame.triplets]], this is a column of vertices with schema matching
   * [[GraphFrame.vertices]].
   */
  val SRC: String = "src"

  /**
   * Column name for destination vertices of edges.
   *  - In [[GraphFrame.edges]], this is a column of vertex IDs.
   *  - In [[GraphFrame.triplets]], this is a column of vertices with schema matching
   * [[GraphFrame.vertices]].
   */
  val DST: String = "dst"

  /**
   * Column name for edge in [[GraphFrame.triplets]].  In [[GraphFrame.triplets]],
   * this is a column of edges with schema matching [[GraphFrame.edges]].
   */
  val EDGE: String = "edge"

  /**
   * Create a new [[TemporalFrame]] from vertex and edge `DataFrame`s.
   *
   * @param vertices  Vertex DataFrame.  This must include a column "id" containing unique vertex IDs.
   *           All other columns are treated as vertex attributes.
   * @param edges  Edge DataFrame.  This must include columns "src" and "dst" containing source and
   *           destination vertex IDs.  All other columns are treated as edge attributes.
   * @return  New [[TemporalFrame]] instance
   */

  def apply (vertices: DataFrame, edges: DataFrame): TemporalFrame = {
    require (vertices.columns.contains (ID),
      s"Vertex ID column '$ID' missing from vertex DataFrame, which has columns: "
        + vertices.columns.mkString (",") )
    require (edges.columns.contains (SRC),
      s"Source vertex ID column '$SRC' missing from edge DataFrame, which has columns: "
        + edges.columns.mkString (",") )
    require (edges.columns.contains (DST),
      s"Destination vertex ID column '$DST' missing from edge DataFrame, which has columns: "
        + edges.columns.mkString (",") )
    require (edges.columns.exists(item => item.startsWith("time")),
      s"Destination time prefix in at least one column missing from edge DataFrame, which has columns: "
        + edges.columns.mkString (",") )

    new TemporalFrame (vertices, edges)
  }
}



//val edge_path = "gs://shrp2/shrp2_edge.csv"
//val vertex_path = "gs://shrp2/shrp2_vertex.csv"
//val shrp2_edge = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(edge_path)
//val shrp2_vertex = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(vertex_path)

//val graph = GraphFrame(shrp2_vertex, shrp2_edge)

//val temp_graph = new TemporalFrame(shrp2_vertex, shrp2_edge)


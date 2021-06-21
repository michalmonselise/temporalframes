package org.temporalframes

import java.util.Random

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{array, broadcast, col, count, explode, struct, udf, monotonically_increasing_id, expr}
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
import scala.collection.mutable.WrappedArray
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
  def construct_timestamps(): Array[Int] = {
    def split_string(x: String): String = {x.split("_")(1)}
    val columns = this._edges.columns
    val time_columns = columns.filter(n => n.startsWith("time_"))
    val time_stamps = time_columns.map(split_string).map(x => x.toInt).sorted
    time_stamps
  }

  var timestamps = construct_timestamps()

  def graph_snapshot(timestamp: Int): GraphFrame = {
    try {
      val snapshot_edges = _edges.filter("time_" + timestamp.toString() + " == 1")
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
    val col_names = timestamps.map(x => "time_" + x.toString())
    val unique_vals = this._edges.dropDuplicates("src").select("src").collect().map(_(0)).toArray.map(_.toString).map(_.toInt)
    var tot = 0.0
    var count = 0
    for (b <- unique_vals) {
      val subset = this._edges.filter($"src" === b)
      for (a <- 0 to col_names.size - 2) {
        val num = (subset.withColumn("prod", col(col_names(a)) * col(col_names(a + 1))).agg(sum("prod"))
          .first.get(0).toString.toDouble)
        val denom = (math.sqrt(subset.agg(sum(col_names(a + 1))).first.get(0).toString.toDouble *
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
  def volatility(distance: String = "Hamming"): Double = {
    val time_columns = this._edges.columns.filter(n => n.startsWith("time_"))
    val edges_with_col = this._edges.withColumn("col_arr",array(time_columns.map(c => col(c)):_*))
    def hamming
  }
}



//val edge_path = "gs://shrp2/shrp2_edge.csv"
//val vertex_path = "gs://shrp2/shrp2_vertex.csv"
//val shrp2_edge = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(edge_path)
//val shrp2_vertex = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(vertex_path)

//val graph = GraphFrame(shrp2_vertex, shrp2_edge)

//val temp_graph = new TemporalFrame(shrp2_vertex, shrp2_edge)


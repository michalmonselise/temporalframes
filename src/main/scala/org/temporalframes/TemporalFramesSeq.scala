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

import TemporalFrame._

class TemporalFrameSeq(@transient private val _vertices: DataFrame,
                    @transient private val _edges: DataFrame, timestampCol: String) extends GraphFrame {

  override def vertices: DataFrame = _vertices
  override def edges: DataFrame = _edges
  def timestampColumn: String = timestampCol
  def construct_timestamps(): DataFrame = {
    val time_stamps_distinct = this._edges.select(timestampColumn).distinct.sort(asc(timestampColumn))
    time_stamps_distinct.createOrReplaceTempView("timestamp_table")
    val time_stamps = spark.sql("select row_number() over (order by " + timestampColumn + " ) as timestampId , * from timestamp_table")
    time_stamps
  }

  var timestamps = construct_timestamps()

  def graph_snapshot(timestamp: String): GraphFrame = {
    try {
      val snapshot_edges = _edges.filter(col(timestampCol) === timestamp)
      GraphFrame(_vertices, snapshot_edges)
    } catch {
      case e: Exception => {
        print("The timestamp does not exist in our snapshots")
        throw(e)
      }
    }
  }

  def transform(): DataFrame = {
    val df = this.edges.groupBy(col("src"), col("dst")).pivot(timestampCol).agg(count(timestampCol)).na.fill(0.0)
    val cols = df.columns.filter(x => (x != "src") & (x != "dst"))
    df.withColumn("col_arr",array(cols.map(c => col(c)):_*)).select(col("src"), col("dst"), col("col_arr"))
  }

  val transformedEdges = transform()

  def topological_corr_coef(): Double = {
    def top_corr(x: mutable.WrappedArray[Double]): Double = {
      var total = 0.0
      for (a <- 0 until x.length) {

      }
    }
    val col_names = timestamps.map(x => "time_" + x.toString())
    val unique_vals = this._edges.dropDuplicates("src").select("src").collect().map(_(0)).toArray.map(_.toString).map(_.toInt)
    var tot = 0.0
    var count = 0
    for (b <- unique_vals) {
      val subset = this._edges.filter($"src" === b)
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
    def generate_burstiness(x: WrappedArray[Double]): Double = {
      //val y = x.toArray[String].map(_.toDouble)
      var res_seq = Seq[Int]()
      var pos = 0
      for (i <- 0 until x.length) {
        if (x(i) > 0) {
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
    val edges_with_burst = transformedEdges.withColumn("burstiness", burst_udf(col("col_arr")))
    edges_with_burst.select("src", "dst", "burstiness")
  }

  def extractDouble(expectedNumber: Any): Double = {
    expectedNumber.toString.toDouble
  }

  def volatility(distance: String = "Hamming"): Double = {

    def hamming(x: mutable.WrappedArray[String]): Double = {
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

    def euclidean(x: mutable.WrappedArray[String]): Double = {
      var total: Double = 0.0
      val y = x.toArray[String].map(_.toDouble)
      var pos = y(0)
      for (i <- 0 until y.length) {
        total = total + math.pow(y(i) - pos, 2)
        pos = y(i)
      }
      math.sqrt(total)
    }

    if (distance == "Hamming") {
      def dist_hamming = spark.udf.register("hamm", hamming _)
      val new_df = transformedEdges.withColumn("dist", dist_hamming(col("col_arr")))
    } else if{
      def dist_euclidean = spark.udf.register("eucl", euclidean _)
      val new_df = transformedEdges.withColumn("dist", dist_euclidean(col("col_arr")))
    } else {
      val new_df = this._edges.withColumn("dist", lit(0))
    }
    val new_df_filter = new_df.where(col("dist") !== 0)
    extractDouble(new_df_filter.agg(avg("dist")).collect()(0)(0))
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}




//val path = "gs://graph-files/college_timestamp.csv"
//val edge = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(path)

//val vertex = edge.select("src").distinct.withColumnRenamed("src", "id").union(edge.select("dst").distinct.withColumnRenamed("dst", "id"))

//val graph = GraphFrame(vertex, edge)

//val temp_graph = new TemporalFrameSeq(vertex, edge, "timestamp")


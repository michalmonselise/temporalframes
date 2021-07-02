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

def volatility_hamming(gf: Seq[GraphFrame])): Double = {
  var total: Long = 0
  for (i <- 0 to gf.length - 2) {
    total = total + gf(i).edges.count + gf(i+1).edges.count - gf(i).edges.join(gf(i+1).edges, Seq("src", "dst")).count
  }
}
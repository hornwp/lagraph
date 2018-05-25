/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.lagraph.impl
// TODO get rid of printlns
// scalastyle:off println

import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.reflect.ClassTag
import scala.collection.mutable.{Map => MMap}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.ibm.lagraph._

class LagDstrV3Suite extends FunSuite with Matchers with SharedSparkContext {
  val DEBUG = false

  def mult[A](
      a: Vector[Vector[A]],
      b: Vector[Vector[A]])(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    for (row <- a)
      yield
        for (col <- b.transpose)
          yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  // ********
  test("LagDstrContext.mTm3") {
    //  def LagDstrContext_mTm3(sc: SparkContext) = {
    val add_mul = LagSemiring.plus_times[Double]
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    val sr = LagSemiring.plus_times[Double]
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        if (DEBUG) println("LagDstrContext.mTm", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)

        val nv = graphSize

        val nr = nv
        val nc = nv
        val nA = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 1.0)
        val nB = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 101.0)
        val sparseValue = 0.0
        val mA =
          hc.mFromMap((graphSize, graphSize),
              LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
        val mB =
          hc.mFromMap((graphSize, graphSize),
              LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)

        val mTmRes = hc.mTm(sr, mA, mB)

        val resScala = mult(nA, nB)
        //         println(mA)
        //         println(mB)
        //         println(mTmRes)
        // //        println(toArray(resScala).deep.mkString("\n"))
        assert(
          toArray(LagContext.vectorOfVectorFromMap((nv.toLong, nv.toLong),
                                                   hc.mToMap(mTmRes)._1,
                                                   sparseValue)).deep == toArray(
            resScala).deep)
      }
    }
  }

// ********
  test("LagDstrContext.mTm3NSQ") {
//  def LagDstrContext_mTm3NSQ(sc: SparkContext): Unit = {
    val add_mul = LagSemiring.plus_times[Double]
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    val sr = LagSemiring.plus_times[Double]
    for (graphSizeRequested <- denseGraphSizes) {
      for (nblock <- nblocks) {
        for (colshift <- List(1, 2, 3,
            graphSizeRequested - 2, graphSizeRequested - 2, graphSizeRequested)) {
          for (negate <- List(true, false)) {
            val nr = graphSizeRequested
            val nc = if (negate) {graphSizeRequested - colshift}
                else graphSizeRequested + colshift
            if (nc > scala.math.min(nblock, nr) && nr > scala.math.min(nblock, nc)) {
              if (DEBUG) println("LagDstrContext.mTm3NSQ", nr, nc, nblock)
              val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
              val nA = Vector.tabulate(nr, nc)((r, c) => r * nc + c + 1.0)
              val nB = Vector.tabulate(nc, nr)((r, c) => r * nc + c + 101.0)
              val sparseValue = 0.0
              val mA =
                hc.mFromMap((nr, nc),
                    LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
              val mB =
                hc.mFromMap((nc, nr),
                    LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)

              val mTmRes = hc.mTm(sr, mA, mB)

              val resScala = mult(nA, nB)
              //         println(mTmRes)
              // //        println(toArray(resScala).deep.mkString("\n"))
              assert(
                toArray(LagContext.vectorOfVectorFromMap((nr.toLong, nr.toLong),
                                                         hc.mToMap(mTmRes)._1,
                                                         sparseValue)).deep == toArray(
                  resScala).deep)
            }
          }
        }
      }
    }
  }

  // ********
  test("LagDstrContext.mTv3") {
    //  def LagDstrContext_mTv3(sc: SparkContext) = {
    val add_mul = LagSemiring.plus_times[Double]
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        if (DEBUG) println("LagDstrContext.mTv", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        // vector
        val sparseValue = 0.0
        val vMap = Map((0 until graphSize).map { r =>
          (r.toLong, r.toDouble)
        }: _*)
        val v = hc.vFromMap[Double](graphSize, vMap, sparseValue)

        // matrix
        val mMap = Map((0 until graphSize).map { r =>
          (0 until graphSize).map { c =>
            ((r.toLong, c.toLong), (r * graphSize + c).toDouble)
          }
        }.flatten: _*)
        val m = hc.mFromMap[Double]((graphSize, graphSize), mMap, sparseValue)

        // multiply
        val u = hc.mTv(add_mul, m, v)

        // back to map
        val (uvm, uvmSparseValue) = hc.vToMap(u)
        assert(uvmSparseValue == sparseValue)

        // compare
        val uvma = LagContext.vectorFromMap(graphSize, uvm, sparseValue)

        val mva = Vector.tabulate(graphSize, graphSize)((r, c) => (r * graphSize + c).toDouble)
        val vva = Vector.tabulate(graphSize, 1)((r, c) => r.toDouble)
        val mua = mult(mva, vva)
        val muat = mua.transpose
        val ua = muat(0)
        assert(ua.corresponds(uvma)(_ == _))
        //  }
      }
    }
  }
  // ********
  test("LagDstrContext.mTv3NSQ") {
    //  def LagDstrContext_mTv3NSQ(sc: SparkContext): Unit = {
    val add_mul = LagSemiring.plus_times[Double]
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSizeRequested <- denseGraphSizes) {
      for (nblock <- nblocks) {
        for (colshift <- List(1, 2, 3,
            graphSizeRequested - 2, graphSizeRequested - 2, graphSizeRequested)) {
          for (negate <- List(true, false)) {
            val nr = graphSizeRequested
            val nc = if (negate) {graphSizeRequested - colshift}
                else graphSizeRequested + colshift
            if (nc > scala.math.min(nblock, nr) && nr > scala.math.min(nblock, nc)) {
              if (DEBUG) println("LagDstrContext.mTv3NSQ", nr, nc, nblock)
              val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
              // vector
              val sparseValue = 0.0
              val vMap = Map((0 until nc).map { r =>
                (r.toLong, r.toDouble)
              }: _*)
              val v = hc.vFromMap[Double](nc, vMap, sparseValue)

              // matrix
              val mMap = Map((0 until nr).map { r =>
                (0 until nc).map { c =>
                  ((r.toLong, c.toLong), (r * nc + c).toDouble)
                }
              }.flatten: _*)
              val m = hc.mFromMap[Double]((nr, nc), mMap, sparseValue)

              // multiply
              val u = hc.mTv(add_mul, m, v)

              // back to map
              val (uvm, uvmSparseValue) = hc.vToMap(u)
              assert(uvmSparseValue == sparseValue)

              // compare
              val uvma = LagContext.vectorFromMap(nr, uvm, sparseValue)

              val mva = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
              val vva = Vector.tabulate(nc, 1)((r, c) => r.toDouble)
              val mua = mult(mva, vva)
              val muat = mua.transpose
              val ua = muat(0)
              assert(ua.corresponds(uvma)(_ == _))
            }
          }
        }
      }
    }
  }
  test("LagDstrContext.vIndices3") {
    //  def LagDstrContext_vIndices3(sc: SparkContext) = {
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        if (DEBUG) println("LagDstrContext.vIndices", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        val start = 100
        val end = start + graphSize
        val v = hc.vIndices(graphSize, start)
        val vRes = hc.vToVector(v)
        assert(v.size == graphSize)
        //        assert(vRes.isInstanceOf[Vector[Int]])
        assert(vRes.size == (end - start))
        (start until end.toInt).map { r =>
          assert(vRes(r - start) == r)
        }
      }
    }
  }
  // ********
  test("LagDstrContext.vZip3") {
    //  def LagDstrContext_vZip3(sc: SparkContext) = {
    //    val denseGraphSizes = List(1 << 4, 1 << 5)
    //    //  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 29, 1 << 30)
    //    val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 26, 1 << 27)
    //    //  val nblocks = List(1 << 0, 1 << 1, 1 << 2, 1 << 3)
    //    val nblocks = List(1 << 0, 1 << 1, 1 << 2)
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    val sparseValueDouble: Double = 0.0
    val sparseValueInt: Int = 0
    val offset = 100
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        if (DEBUG) println("LagDstrContext.vZip", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        // vectors
        val u = hc.vIndices(graphSize, 0)
        val v = hc.vIndices(graphSize, offset)
        val f = (a: Long, b: Long) => (a + b).toDouble
        val w = hc.vZip(f, u, v)
        val (wResMap, waSparse) = hc.vToMap(w)
        assert(waSparse == sparseValueDouble)
        val wRes = LagContext.vectorFromMap(graphSize, wResMap, waSparse)
        val vScala = (0 until graphSize).map { a =>
          (a + a + offset).toDouble
        }.toVector
        assert(wRes.size == graphSize)
        assert(vScala.corresponds(wRes)(_ == _))
      }
    }
  }
  // ********
  test("LagDstrContext.mZipWithIndex3") {
    //  def LagDstrContext_mZipWithIndex3(sc: SparkContext) = {
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList

    val sparseValueDouble: Double = -99.0
    val f = (a: Double, b: (Long, Long)) => Tuple2(a, b)
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      val nc = graphSize
      for (nblock <- nblocks) {
        if (DEBUG) println("LagDstrContext.mZipWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        val mA = hc.mFromMap((graphSize, graphSize),
          LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                     sparseValueDouble),
          sparseValueDouble)

        val (mAres, mAresSparse) = hc.mToMap(mA)
        //        println("mAresSparse: >%s<".format(mAresSparse))
        //        println("mA: >%s<".format(LagContext.vectorOfVectorFromMap(
        //          mAres, mAresSparse, (nr, nc))))

        val mZipWithIndex = hc.mZipWithIndex(f, mA)

        val (mZipWithIndexResMap, mZipWithIndexResSparse) =
          hc.mToMap(mZipWithIndex)

        val mZipWithIndexResVector =
          LagContext.vectorOfVectorFromMap((nr, nc), mZipWithIndexResMap, mZipWithIndexResSparse)

        var mZipWithIndexActualMap = Map[(Long, Long), (Double, (Long, Long))]()
        (0L until nr).map { r =>
          (0L until nc).map { c =>
            {
              val v = r * nc + c
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(v.toDouble,
                                                                                        (r, c)))
            }
          }
        }
        val mZipWithIndexActualVector =
          LagContext.vectorOfVectorFromMap((nr, nc), mZipWithIndexActualMap, mZipWithIndexResSparse)
        assert(mZipWithIndexResVector.corresponds(mZipWithIndexActualVector)(_ == _))
      }
    }
  }
}
// scalastyle:on println

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
  def mTm[A](
      a: Vector[Vector[A]],
      b: Vector[Vector[A]])(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    for (row <- a)
      yield
        for (col <- b.transpose)
          yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def vToMrow[A](m: Vector[Vector[A]],
                 mask: Vector[Boolean],
                 v: Vector[A])(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    for (row <- m zip mask)
      yield if (row._2) v else row._1
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }
  // ********
  test("LagDstrContext.vIndicesV3") {
    //  def LagDstrContext_vIndicesV3(sc: SparkContext) = {
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
  test("LagDstrContext.vZipV3") {
    //  def LagDstrContext_vZipV3(sc: SparkContext) = {
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
  test("LagDstrContext.mZipWithIndexV3") {
    //  def LagDstrContext_mZipWithIndexV3(sc: SparkContext) = {
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
  // ********
  // ********
  // ********
  test("LagDstrContext.mTmV3") {
//  def LagDstrContext_mTmV3(sc: SparkContext): Unit = {
//    val DEBUG = true
    val sr = LagSemiring.plus_times[Long]
    val nblocks = List(1, 2, 3, 7, 8, 9)
    val graphSizes = List(1, 2, 3, 10, 11, 12)
    val sparseValue = 0L
    for (nblock <- nblocks) {
      for (l <- graphSizes) {
        for (m <- graphSizes) {
          for (n <- graphSizes) {
            val mindim = List(nblock, l, m, n).min
            if (l > mindim && m > mindim && n > mindim) {
              if (DEBUG) println(
                  "LagDstrContext.mTmV3: nblock: >%s<, l: >%s<, m: >%s<, n: >%s<".format(
                  nblock, l, m, n))

              // ref
              val refA = Vector.tabulate(l, m)((r, c) => r * m + c + 1L)
              val refB = Vector.tabulate(m, n)((r, c) => r * n + c + 101L)
              val refC = mTm(refA, refB)
//              if (DEBUG) {
//                println("refA")
//                println(refA)
//                println("refB")
//                println(refB)
//                println("refC")
//                println(refC)
//              }

              // lagraph
              val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)

              val lagA =
                hc.mFromMap((l, m),
                    LagContext.mapFromSeqOfSeq(refA, sparseValue), sparseValue)
              val lagB =
                hc.mFromMap((m, n),
                    LagContext.mapFromSeqOfSeq(refB, sparseValue), sparseValue)
//            if (DEBUG) {
//              println("lagA")
//              println(lagA)
//              println("lagB")
//              println(lagB)
//            }
              val lagC = hc.mTm(sr, lagA, lagB)
//            if (DEBUG) {
//              println("lagC")
//              println(lagC)
//            }

              // compare
              assert(
                toArray(LagContext.vectorOfVectorFromMap((l.toLong, n.toLong),
                                                         hc.mToMap(lagC)._1,
                                                         sparseValue)).deep == toArray(
                  refC).deep)

            }
          }
        }
      }
    }
  }

  // ********
  test("LagDstrContext.mTvV3") {
//  def LagDstrContext_mTvV3(sc: SparkContext): Unit = {
//    val DEBUG = true
    val sr = LagSemiring.plus_times[Long]
    val nblocks = List(1, 2, 3, 7, 8, 9)
    val graphSizes = List(1, 2, 3, 10, 11, 12)
    val sparseValue = 0L
    for (nblock <- nblocks) {
      for (l <- graphSizes) {
        for (m <- graphSizes) {
          val mindim = List(nblock, l, m).min
          if (l > mindim && m > mindim) {
            if (DEBUG) println("LagDstrContext.mTvV3: nblock: >%s<, l: >%s<, m: >%s<".format(
                nblock, l, m))

            val refA = Vector.tabulate(l, m)((r, c) => r * m + c + 1L)
            val refv = Vector.tabulate(m, 1)((r, c) => r + 101L)
            val refu = mTm(refA, refv)

//            if (DEBUG) {
//              println("refA")
//              println(refA)
//              println("refv")
//              println(refv)
//              println("refu")
//              println(refu)
//            }

            // lagraph
            val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
            val lagA =
              hc.mFromMap((l, m),
                  LagContext.mapFromSeqOfSeq(refA, sparseValue), sparseValue)
            val lagv = hc.vFromSeq(refv.flatten, sparseValue)
//            if (DEBUG) {
//              println("lagA")
//              println(lagA)
//              println("lagv")
//              println(lagv)
//            }
            val lagu = hc.mTv(sr, lagA, lagv)
//            if (DEBUG) {
//              println("lagu")
//              println(lagu)
//            }

              assert(refu.flatten.corresponds( hc.vToVector(lagu))(_ == _))
          }
        }
      }
    }
  }

  // ********
  test("LagDstrContext.vToMrowV3") {
//  def LagDstrContext_vToMrowV3(sc: SparkContext): Unit = {
//    val DEBUG = true
    val nblocks = List(1, 2, 3, 7, 8, 9)
    val graphSizes = List(1, 2, 3, 10, 11, 12)
    for (nblock <- nblocks) {
      for (l <- graphSizes) {
        for (m <- graphSizes) {
          val mindim = List(nblock, l, m).min
          if (l > mindim && m > mindim) {
            if (DEBUG) println("LagDstrContext.vToMrowV3: nblock: >%s<, l: >%s<, m: >%s<".format(
                nblock, l, m))

            // ref
            val refA = Vector.tabulate(l, m)((r, c) => r * m + c + 1.0)
            val maskRows = Vector(l/2, l/2 - 1, l/2 + 1)
            val refMask = Vector.tabulate(l)(
                r => if (maskRows.contains(r)) true else false)
            val refv = Vector.tabulate(m)(r => r + 101.0)
            val refB = vToMrow(refA, refMask, refv)

//            if (DEBUG) {
//              println("refA")
//              println(refA)
//              println("refMask")
//              println(refMask)
//              println("refv")
//              println(refv)
//              println("refB")
//              println(refB)
//            }

            // lagraph
            val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
            val sparseValue = 0.0

            val lagA =
              hc.mFromMap((l, m),
                  LagContext.mapFromSeqOfSeq(refA, sparseValue), sparseValue)
            val lagMask =
              hc.vFromSeq(refMask, false)
            val lagV =
              hc.vFromSeq(refv, sparseValue)
            val lagB = hc.vToMrow(lagA, lagMask, lagV)
//            if (DEBUG) {
//              println("lagA")
//              println(lagA)
//              println("lagMask")
//              println(lagMask)
//              println("lagV")
//              println(lagV)
//              println("lagB")
//              println(lagB)
//            }

            assert(
              toArray(LagContext.vectorOfVectorFromMap((l.toLong, m.toLong),
                                                       hc.mToMap(lagB)._1,
                                                       sparseValue)).deep == toArray(
                refB).deep)

          }
        }
      }
    }
  }
}
// scalastyle:on println

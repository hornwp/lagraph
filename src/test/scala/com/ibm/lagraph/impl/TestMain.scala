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

import scala.reflect.{ ClassTag, classTag }

import scala.collection.mutable.{ Map => MMap, ArrayBuffer }

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import com.ibm.lagraph._

object TestMain {
  def mTm[A](
    a: Vector[Vector[A]],
    b: Vector[Vector[A]])(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    for (row <- a)
      yield for (col <- b.transpose)
      yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def vToMrow[A](
    m:    Vector[Vector[A]],
    mask: Vector[Boolean],
    v:    Vector[A])(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    for (row <- m zip mask)
      yield if (row._2) v else row._1
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  object TestStr extends Serializable {

    /** Numeric for BfSemiringType */
    type BfSemiringType = String

    /** Ordering for BfSemiringType */
    trait BfSemiringTypeOrdering extends Ordering[BfSemiringType] {
      def compare(ui: BfSemiringType, vi: BfSemiringType): Int = {
        if (ui.size < vi.size) -1
        else if (ui.size == vi.size) 0
        else 1
      }
    }
    trait BfSemiringTypeAsNumeric
      extends LagSemiringAsNumeric[BfSemiringType]
      with BfSemiringTypeOrdering {
      def plus(ui: BfSemiringType, vi: BfSemiringType): BfSemiringType = {
        ui + "+" + vi
      }
      def times(x: BfSemiringType, y: BfSemiringType): BfSemiringType = {
        x + "*" + y
      }
      def fromInt(x: Int): BfSemiringType = x match {
        case 0 => "ZERO"
        case 1 => "ONE"
        case other =>
          throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
      }
    }
    implicit object BfSemiringTypeAsNumeric extends BfSemiringTypeAsNumeric
    val BfSemiring = LagSemiring.plus_times[BfSemiringType]
  }

  // ***************************************************************************
  // ***************************************************************************
  // ***************************************************************************
  // ***************************************************************************
  // ***************************************************************************
  import com.ibm.lagraph.{ LagContext, LagSemigroup, LagSemiring, LagVector }
  def fundamentalPrimsForDebug(sc: SparkContext): Unit = {
    // ********
    // Setup some utility functions
    // some imports ...

    // for verbose printing
    import scala.reflect.classTag
    def float2Str(f: Float): String = {
      if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
      else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
      else "%6.3f".format(f)
    }
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    def primType2Str(d: (Float, Long)): String = {
      val d1 = float2Str(d._1)
      val d2 = long2Str(d._2)
      "(%s,%s)".format(d1, d2)
    }

    // ********
    // Create a Simple Graph from an RDD
    // define graph
    val numv = 5L
    val houseEdges = List(
      ((1L, 0L), 20.0F),
      ((2L, 0L), 10.0F),
      ((3L, 1L), 15.0F),
      ((3L, 2L), 30.0F),
      ((4L, 2L), 50.0F),
      ((4L, 3L), 5.0F))

    // Undirected
    val rcvGraph = sc
      .parallelize(houseEdges)
      .flatMap { x =>
        List(((x._1._1, x._1._2), x._2), ((x._1._2, x._1._1), x._2))
      }
      .distinct()

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val mAdjIn = hc.mFromRcvRdd((numv, numv), rcvGraph, 0.0F)

    println("mAdjIn: >\n%s<".format(hc.mToString(mAdjIn, float2Str)))

    // ********
    // Prim's: Path-augmented Semiring: Initialization

    type PrimSemiringType = Tuple2[Float, Long]

    // some handy constants
    val FloatInf = LagSemigroup.infinity(classTag[Float])
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val FloatMinf = LagSemigroup.minfinity(classTag[Float])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val NodeNil: Long = -1L

    // Ordering for PrimSemiringType
    trait PrimSemiringTypeOrdering extends Ordering[PrimSemiringType] {
      def compare(ui: PrimSemiringType, vi: PrimSemiringType): Int = {
        val w1 = ui._1; val p1 = ui._2
        val w2 = vi._1; val p2 = vi._2
        if (w1 < w2) -1
        else if ((w1 == w2) && (p1 < p2)) -1
        else if ((w1 == w2) && (p1 == p2)) 0
        else 1
      }
    }

    // Numeric for PrimSemiringType
    trait PrimSemiringTypeAsNumeric
      extends com.ibm.lagraph.LagSemiringAsNumeric[PrimSemiringType]
      with PrimSemiringTypeOrdering {
      def plus(ui: PrimSemiringType, vi: PrimSemiringType): PrimSemiringType =
        throw new RuntimeException(
          "PrimSemiring has nop for addition: ui: >%s<, vi: >%s<".format(ui, vi))
      def times(x: PrimSemiringType, y: PrimSemiringType): PrimSemiringType =
        min(x, y)
      def fromInt(x: Int): PrimSemiringType = x match {
        case 0 => ((0.0).toFloat, NodeNil)
        case 1 => (FloatInf, LongInf)
        case other =>
          throw new RuntimeException(
            "PrimSemiring: fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object PrimSemiringTypeAsNumeric extends PrimSemiringTypeAsNumeric
    val PrimSemiring =
      LagSemiring.nop_min[PrimSemiringType](Tuple2(FloatInf, LongInf), Tuple2(FloatMinf, LongMinf))

    // ****
    // Need a nop_min semiring Float so add proper behavior for infinity
    type FloatWithInfType = Float

    // Ordering for FloatWithInfType
    trait FloatWithInfTypeOrdering extends Ordering[FloatWithInfType] {
      def compare(ui: FloatWithInfType, vi: FloatWithInfType): Int = {
        compare(ui, vi)
      }
    }
    // Numeric for FloatWithInfType
    trait FloatWithInfTypeAsNumeric
      extends com.ibm.lagraph.LagSemiringAsNumeric[FloatWithInfType]
      with FloatWithInfTypeOrdering {
      def plus(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
        if (ui == FloatInf || vi == FloatInf) FloatInf
        else ui + vi
      }
      def times(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
        if (ui == FloatInf || vi == FloatInf) FloatInf
        else ui + vi
      }
    }

    // ********
    // Algebraic Prim's

    // initialize adjacency matrix
    def mInit(v: Float, rc: (Long, Long)): PrimSemiringType =
      if (rc._1 == rc._2) PrimSemiring.zero
      else if (v != 0.0F) Tuple2(v, rc._1)
      else Tuple2(FloatInf, NodeNil)

    val mAdj = hc.mZipWithIndex(mInit, mAdjIn)
    println("mAdj: >\n%s<".format(hc.mToString(mAdj, primType2Str)))

    val weight_initial = 0

    // arbitrary vertex to start from
    val source = 0L

    // initial membership in spanning tree set
    val s_initial = hc.vSet(hc.vReplicate(numv, 0.0F), source, FloatInf)
    println("s_initial: >\n%s<".format(hc.vToString(s_initial, float2Str)))

    // initial membership in spanning tree set
    val s_final_test = hc.vReplicate(numv, FloatInf)
    println("s_final_test: >\n%s<".format(hc.vToString(s_final_test, float2Str)))
    s_final_test
      .asInstanceOf[LagDstrVector[PrimSemiringType]]
      .dstrBvec
      .vecRdd
      .collect
      .foreach {
        case (k, v) =>
          println("s_final_test: (%s,%s): %s".format(k._1, k._2, v))
      }

    val d_initial = hc.vFromMrow(mAdj, 0)
    println("d_initial: >\n%s<".format(hc.vToString(d_initial, primType2Str)))

    val pi_initial = hc.vReplicate(numv, NodeNil)
    println("pi_initial: >\n%s<".format(hc.vToString(pi_initial, long2Str)))

    def iterate(
      weight: Float,
      d:      LagVector[PrimSemiringType],
      s:      LagVector[Float],
      pi:     LagVector[Long]): (Float, LagVector[PrimSemiringType], LagVector[Float], LagVector[Long]) =
      if (hc.vEquiv(s, s_final_test)) {
        println("DONE")
        println("s: >\n%s<".format(hc.vToString(s, float2Str)))
        println("s_final_test: >\n%s<".format(hc.vToString(s_final_test, float2Str)))
        (weight, d, s, pi)
      } else {
        println("  iterate ****************************************")
        val pt1 = hc.vMap({ wp: PrimSemiringType =>
          wp._1
        }, d)
        println("    pt1: >\n%s<".format(hc.vToString(pt1, float2Str)))
        //        val pt2 = hc.vZip(LagSemiring.nop_plus[LongWithInfType].multiplication, pt1, s)
        val pt2 =
          hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication, pt1, s)
        println("    pt2: >\n%s<".format(hc.vToString(pt2, float2Str)))
        val u = hc.vArgmin(pt2)
        println("    u: >%d<".format(u._2))
        val s2 = hc.vSet(s, u._2, FloatInf)
        println("    s_i: >\n%s<".format(hc.vToString(s2, float2Str)))
        s2.asInstanceOf[LagDstrVector[PrimSemiringType]]
          .dstrBvec
          .vecRdd
          .collect
          .foreach {
            case (k, v) => println("s_i: (%s,%s): %s".format(k._1, k._2, v))
          }
        val wp = hc.vEle(d, u._2)
        val weight2 = weight + wp._1.get._1
        println("    w_i: >%f<".format(weight2))
        val pi2 = hc.vSet(pi, u._2, wp._1.get._2)
        println("    pi_i: >\n%s<".format(hc.vToString(pi2, long2Str)))
        val aui = hc.vFromMrow(mAdj, u._2)
        println("    aui: >\n%s<".format(hc.vToString(aui, primType2Str)))
        val d2 = hc.vZip(PrimSemiring.multiplication, d, aui)
        println("    d_i: >\n%s<".format(hc.vToString(d2, primType2Str)))
        iterate(weight2, d2, s2, pi2)
      }

    val (weight_final, d_final, s_final, pi_final) =
      iterate(weight_initial, d_initial, s_initial, pi_initial)

    println("weight_final: >%f<".format(weight_final))
    println("d_final: >\n%s<".format(hc.vToString(d_final, primType2Str)))
    println("s_final: >\n%s<".format(hc.vToString(s_final, float2Str)))
    println("pi_final: >\n%s<".format(hc.vToString(pi_final, long2Str)))

  }

  // ***************************************************************************
  import com.ibm.lagraph.{ LagContext, LagSemigroup, LagSemiring, LagVector }
  def fundamentalPrimsForPub(sc: SparkContext): Unit = {
    // ********
    // Setup some utility functions
    // some imports ...

    // for verbose printing
    import scala.reflect.classTag
    def float2Str(f: Float): String = {
      if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
      else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
      else "%6.3f".format(f)
    }
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    def primType2Str(d: (Float, Long)): String = {
      val d1 = float2Str(d._1)
      val d2 = long2Str(d._2)
      "(%s,%s)".format(d1, d2)
    }

    // ********
    // Create a Simple Graph from an RDD
    // define graph
    val numv = 5L
    val houseEdges = List(
      ((1L, 0L), 20.0F),
      ((2L, 0L), 10.0F),
      ((3L, 1L), 15.0F),
      ((3L, 2L), 30.0F),
      ((4L, 2L), 50.0F),
      ((4L, 3L), 5.0F))

    // Undirected
    val rcvGraph = sc
      .parallelize(houseEdges)
      .flatMap { x =>
        List(((x._1._1, x._1._2), x._2), ((x._1._2, x._1._1), x._2))
      }
      .distinct()

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val mAdjIn = hc.mFromRcvRdd((numv, numv), rcvGraph, 0.0F)

    println("mAdjIn: >\n%s<".format(hc.mToString(mAdjIn, float2Str)))

    // ********
    // Prim's: Path-augmented Semiring: Initialization

    type PrimSemiringType = Tuple2[Float, Long]

    // some handy constants
    val FloatInf = LagSemigroup.infinity(classTag[Float])
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val FloatMinf = LagSemigroup.minfinity(classTag[Float])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val NodeNil: Long = -1L

    // Ordering for PrimSemiringType
    trait PrimSemiringTypeOrdering extends Ordering[PrimSemiringType] {
      def compare(ui: PrimSemiringType, vi: PrimSemiringType): Int = {
        val w1 = ui._1; val p1 = ui._2
        val w2 = vi._1; val p2 = vi._2
        if (w1 < w2) -1
        else if ((w1 == w2) && (p1 < p2)) -1
        else if ((w1 == w2) && (p1 == p2)) 0
        else 1
      }
    }

    // Numeric for PrimSemiringType
    trait PrimSemiringTypeAsNumeric
      extends com.ibm.lagraph.LagSemiringAsNumeric[PrimSemiringType]
      with PrimSemiringTypeOrdering {
      def plus(ui: PrimSemiringType, vi: PrimSemiringType): PrimSemiringType =
        throw new RuntimeException(
          "PrimSemiring has nop for addition: ui: >%s<, vi: >%s<".format(ui, vi))
      def times(x: PrimSemiringType, y: PrimSemiringType): PrimSemiringType =
        min(x, y)
      def fromInt(x: Int): PrimSemiringType = x match {
        case 0 => ((0.0).toFloat, NodeNil)
        case 1 => (FloatInf, LongInf)
        case other =>
          throw new RuntimeException(
            "PrimSemiring: fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object PrimSemiringTypeAsNumeric extends PrimSemiringTypeAsNumeric
    val PrimSemiring =
      LagSemiring.nop_min[PrimSemiringType](Tuple2(FloatInf, LongInf), Tuple2(FloatMinf, LongMinf))

    // ****
    // Need a nop_min semiring Float so add proper behavior for infinity
    type FloatWithInfType = Float

    // Ordering for FloatWithInfType
    trait FloatWithInfTypeOrdering extends Ordering[FloatWithInfType] {
      def compare(ui: FloatWithInfType, vi: FloatWithInfType): Int = {
        compare(ui, vi)
      }
    }
    // Numeric for FloatWithInfType
    trait FloatWithInfTypeAsNumeric
      extends com.ibm.lagraph.LagSemiringAsNumeric[FloatWithInfType]
      with FloatWithInfTypeOrdering {
      def plus(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
        if (ui == FloatInf || vi == FloatInf) FloatInf
        else ui + vi
      }
      def times(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
        if (ui == FloatInf || vi == FloatInf) FloatInf
        else ui + vi
      }
    }

    // ********
    // Algebraic Prim's

    // initialize adjacency matrix
    def mInit(v: Float, rc: (Long, Long)): PrimSemiringType =
      if (rc._1 == rc._2) PrimSemiring.zero
      else if (v != 0.0F) Tuple2(v, rc._1)
      else Tuple2(FloatInf, NodeNil)

    val mAdj = hc.mZipWithIndex(mInit, mAdjIn)
    println("mAdj: >\n%s<".format(hc.mToString(mAdj, primType2Str)))

    val weight_initial = 0

    // arbitrary vertex to start from
    val source = 0L

    // initial membership in spanning tree set
    val s_initial = hc.vSet(hc.vReplicate(numv, 0.0F), source, FloatInf)

    // initial membership in spanning tree set
    val s_final_test = hc.vReplicate(numv, FloatInf)

    val d_initial = hc.vFromMrow(mAdj, 0)

    val pi_initial = hc.vReplicate(numv, NodeNil)

    def iterate(
      weight: Float,
      d:      LagVector[PrimSemiringType],
      s:      LagVector[Float],
      pi:     LagVector[Long]): (Float, LagVector[PrimSemiringType], LagVector[Float], LagVector[Long]) =
      if (hc.vEquiv(s, s_final_test)) (weight, d, s, pi)
      else {
        println("  iterate ****************************************")
        val u = hc.vArgmin(hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication, hc.vMap({
          wp: PrimSemiringType =>
            wp._1
        }, d), s))
        val wp = hc.vEle(d, u._2)
        val aui = hc.vFromMrow(mAdj, u._2)
        iterate(
          weight + wp._1.get._1,
          hc.vZip(PrimSemiring.multiplication, d, aui),
          hc.vSet(s, u._2, FloatInf),
          hc.vSet(pi, u._2, wp._1.get._2))
      }

    val (weight_final, d_final, s_final, pi_final) =
      iterate(weight_initial, d_initial, s_initial, pi_initial)

    println("weight_final: >%f<".format(weight_final))
    println("d_final: >\n%s<".format(hc.vToString(d_final, primType2Str)))
    println("s_final: >\n%s<".format(hc.vToString(s_final, float2Str)))
    println("pi_final: >\n%s<".format(hc.vToString(pi_final, long2Str)))

  }

  //  test("LagDstrContext.vEquivV3") {
  def LagDstrContext_vEquivV3(sc: SparkContext): Unit = {
    import scala.reflect.classTag
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    val denseGraphSizes = (2 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vEquiv 01", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        val LongInf = LagSemigroup.infinity(classTag[Long])
        val source = 0L
        val s_1 = hc.vSet(hc.vReplicate(graphSize, 0L), source, LongInf)
        val s_2 = hc.vReplicate(graphSize, LongInf)
        //        println("s_1: >\n%s<".format(hc.vToString(s_1, long2Str)))
        //        println("s_2: >\n%s<".format(hc.vToString(s_2, long2Str)))
        require(!hc.vEquiv(s_1, s_2))
      }
    }
  }
  // ********
  //  test("LagDstrContext.vZipWithIndexV3") {
  def LagDstrContext_vZipWithIndexV3(sc: SparkContext): Unit = {
    val denseGraphSizes = (2 until 16).toList
    val nblocks = (1 until 24).toList
    val sparseValueInt = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vZipWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        val u = hc.vIndices(graphSize, 0)
        val f = (a: Long, b: Long) => (a, b)
        val w = hc.vZipWithIndex(f, u)
        val wRes = hc.vToVector(w)
        val vScala = (0 until graphSize).map { a =>
          Tuple2(a, a)
        }.toVector
        assert(wRes.size == graphSize)
        assert(vScala.corresponds(wRes)(_ == _))
      }
    }
  }
  // ********
  //  test("LagDstrContext.vZipWithIndexSparseV3") {
  def LagDstrContext_vZipWithIndexSparseV3(sc: SparkContext): Unit = {
    val denseGraphSizes = (2 until 16).toList
    val nblocks = (1 until 24).toList
    val sparseValueInt = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vZipWithIndexSparse", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        val u = hc.vReplicate(graphSize, 0L)
        val f = (a: Long, b: Long) => (a, b)
        val w = hc.vZipWithIndex(f, u)
        val wRes = hc.vToVector(w)
        val vScala = (0 until graphSize).map { a =>
          Tuple2(0L, a)
        }.toVector
        assert(wRes.size == graphSize)
        assert(vScala.corresponds(wRes)(_ == _))
      }
    }
  }
  // ********
  //  test("LagDstrContext.mZipWithIndexSparseV3") {
  def LagDstrContext_mZipWithIndexSparseV3(sc: SparkContext): Unit = {
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 7).toList

    val (sparseNr, sparseNc) = (1, 1)
    val sparseValueDouble: Double = -99.0
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      val nc = graphSize
      for (nblock <- nblocks) {
        println("LagDstrContext.mZipWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
        val mA = hc.mFromMap((graphSize, graphSize), LagContext.mapFromSeqOfSeq(
          Vector.tabulate(sparseNr, sparseNc)((r, c) => (r * nc + c).toDouble),
          sparseValueDouble),
          sparseValueDouble)

        val (mAres, mAresSparse) = hc.mToMap(mA)
        //        println("mAresSparse: >%s<".format(mAresSparse))
        //        println("mA: >%s<".format(LagContext.vectorOfVectorFromMap(
        //          mAres, mAresSparse, (nr, nc))))

        val f = (a: Double, b: (Long, Long)) => Tuple2(a, b)
        val mZipWithIndex = hc.mZipWithIndex(f, mA)
        //        println("mZipWithIndex: >\n%s<".format(hc.mToString(mZipWithIndex,
        //          {v:(Double,(Long, Long)) => "%s".format(v)})))
        //
        val (mZipWithIndexResMap, mZipWithIndexResSparse) =
          hc.mToMap(mZipWithIndex)

        val mZipWithIndexResVector =
          LagContext.vectorOfVectorFromMap((nr, nc), mZipWithIndexResMap, mZipWithIndexResSparse)

        var mZipWithIndexActualMap = Map[(Long, Long), (Double, (Long, Long))]()
        (0L until nr).map { r =>
          (0L until nc).map { c =>
            {
              val v = r * nc + c
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(
                sparseValueDouble,
                (r, c)))
            }
          }
        }
        (0L until sparseNr).map { r =>
          (0L until sparseNc).map { c =>
            {
              val v = r * nc + c
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(
                v.toDouble,
                (r, c)))
            }
          }
        }
        val mZipWithIndexActualVector =
          LagContext.vectorOfVectorFromMap((nr, nc), mZipWithIndexActualMap, mZipWithIndexResSparse)
        //        println("mZipWithIndexResVector: >%s<".format(toArray(mZipWithIndexActualVector).
        //          deep.mkString("\n")))
        assert(mZipWithIndexResVector.corresponds(mZipWithIndexActualVector)(_ == _))
      }
    }
  }
  // ********
  // ********
  // ********
  //  test("LagDstrContext.mTmV3") {
  def LagDstrContext_mTmV3(sc: SparkContext): Unit = {
    val DEBUG = true
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
                hc.mFromMap(
                  (l, m),
                  LagContext.mapFromSeqOfSeq(refA, sparseValue), sparseValue)
              val lagB =
                hc.mFromMap(
                  (m, n),
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
                toArray(LagContext.vectorOfVectorFromMap(
                  (l.toLong, n.toLong),
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
  //  test("LagDstrContext.mTvV3") {
  def LagDstrContext_mTvV3(sc: SparkContext): Unit = {
    val DEBUG = true
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
              hc.mFromMap(
                (l, m),
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

            assert(refu.flatten.corresponds(hc.vToVector(lagu))(_ == _))
          }
        }
      }
    }
  }

  // ********
  //  test("LagDstrContext.vToMrowV3") {
  def LagDstrContext_vToMrowV3(sc: SparkContext): Unit = {
    val DEBUG = true
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
            val maskRows = Vector(l / 2, l / 2 - 1, l / 2 + 1)
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
              hc.mFromMap(
                (l, m),
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
              toArray(LagContext.vectorOfVectorFromMap(
                (l.toLong, m.toLong),
                hc.mToMap(lagB)._1,
                sparseValue)).deep == toArray(
                refB).deep)

          }
        }
      }
    }
  }

  // ********
  //  test("LagDstrContext.transposeV3") {
  def LagDstrContext_transposeV3(sc: SparkContext): Unit = {
    val DEBUG = true
    val nblocks = List(1, 2, 3, 7, 8, 9)
    val graphSizes = List(1, 2, 3, 10, 11, 12)
    for (nblock <- nblocks) {
      for (l <- graphSizes) {
        for (m <- graphSizes) {
          val mindim = List(nblock).min
          if (l > mindim && m > mindim) {
            if (DEBUG) println("LagDstrContext.transposeV3: nblock: >%s<, l: >%s<, m: >%s<".format(
              nblock, l, m))

            // ref
            val refA = Vector.tabulate(l, m)((r, c) => r * m + c + 1.0)
            val refB = refA.transpose
            //            if (DEBUG) {
            //              println("refA")
            //              println(refA)
            //              println("refB")
            //              println(refB)
            //            }

            // lagraph
            val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
            val sparseValue = 0.0

            val lagA =
              hc.mFromMap(
                (l, m),
                LagContext.mapFromSeqOfSeq(refA, sparseValue), sparseValue)

            val lagB = lagA.transpose
            //            if (DEBUG) {
            //              println("lagA")
            //              println(lagA)
            //              println("lagB")
            //              println(lagB)
            //            }

            assert(
              toArray(LagContext.vectorOfVectorFromMap(
                (m.toLong, l.toLong),
                hc.mToMap(lagB)._1,
                sparseValue)).deep == toArray(
                refB).deep)

          }
        }
      }
    }
  }

  // ********
  // ********
  // ********
  //  test("LagDstrContext.wolf2015task") {
  def LagDstrContext_wolf2015task(sc: SparkContext): Unit = {
    val DEBUG = true
    // ********
    // Graph from Worked Example in wolf2015task

    // some handy constants
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])

    // for verbose printing
    def long2Str(l: Long): String = {
      if (l == LongInf) " inf"
      else if (l == LongInf - 1L) "inf1"
      else "%4d".format(l)
    }

    def wolfType2Str(d: (Long, Long, Long)): String = {
      val d1 = long2Str(d._1)
      val d2 = long2Str(d._2)
      val d3 = long2Str(d._3)
      "(%s,%s,%s)".format(d1, d2, d3)
    }

    // ********
    // Wolf's: wolf2015task Semiring: Initialization

    type WolfSemiringType = Tuple3[Long, Long, Long]

    // Ordering for PrimSemiringType
    trait WolfSemiringTypeOrdering extends Ordering[WolfSemiringType] {
      def compare(u: WolfSemiringType, v: WolfSemiringType): Int = {
        if (u._1 < v._1) -1
        else if (u._1 > v._1) 1
        else if (u._2 < v._2) -1
        else if (u._2 > v._2) 1
        else if (u._3 < v._3) -1
        else if (u._3 > v._3) 1
        else 0
      }
    }

    // Numeric for WolfSemiringType
    trait WolfSemiringTypeAsNumeric
      extends com.ibm.lagraph.LagSemiringAsNumeric[WolfSemiringType]
      with WolfSemiringTypeOrdering {
      def plus(u: WolfSemiringType, v: WolfSemiringType): WolfSemiringType = {
        if (u == fromInt(0)) v
        else if (v == fromInt(0)) u
        else if ((u._1 == v._1) && (u._2 == v._3) && (u._3 == v._2)) (-u._1, -u._2, -u._3)
        else fromInt(0)
      }
      def times(x: WolfSemiringType, y: WolfSemiringType): WolfSemiringType = {
        if ((x == fromInt(0)) || (y == fromInt(0))) fromInt(0)
        else if (x == fromInt(1)) y
        else if (y == fromInt(1)) x
        else (y._1, y._2, x._3)
      }
      def fromInt(x: Int): WolfSemiringType = x match {
        case 0 => (LongInf, LongInf, LongInf)
        case 1 => (LongInf - 1L, LongInf - 1L, LongInf - 1L)
        case other =>
          throw new RuntimeException(
            "WolfSemiring: fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object WolfSemiringTypeAsNumeric extends WolfSemiringTypeAsNumeric
    val WolfSemiring =
      LagSemiring.plus_times[WolfSemiringType]

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)

    // ****
    // initialize graph edges
    val numv = 5L
    val exampleEdges = List(
      (0L, 4L),
      (1L, 4L),
      (2L, 4L),
      (3L, 1L),
      (3L, 2L),
      (4L, 3L))
    val E = sc.parallelize(exampleEdges)

    // ****
    // define adjacency matrix
    val A = E
      .flatMap { e =>
        List(
          ((e._1, e._2), (e._1, e._2, LongInf)),
          ((e._2, e._1), (e._2, e._1, LongInf)))
      }

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Aadj = hc.mFromRcvRdd((numv, numv), A, WolfSemiring.zero)
    println("Aadj: >\n%s<".format(hc.mToString(Aadj, wolfType2Str)))

    // define incidence matrix
    val B = E
      .map { e => if (e._1 < e._2) (e._1, e._2) else (e._2, e._1) }
      .zipWithIndex()
      .flatMap { ei =>
        List(
          ((ei._1._1, ei._2), (LongInf, LongInf, ei._1._2)),
          ((ei._1._2, ei._2), (LongInf, LongInf, ei._1._1)))
      }

    val nume = B.max()(
      new Ordering[Tuple2[Tuple2[Long, Long], Tuple3[Long, Long, Long]]]() {
        override def compare(
          x: ((Long, Long), (Long, Long, Long)),
          y: ((Long, Long), (Long, Long, Long))): Int =
          Ordering[Long].compare(x._1._2, y._1._2)
      })._1._2 + 1L
    println("nume: >%s<".format(nume))

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Binc = hc.mFromRcvRdd((numv, nume), B, WolfSemiring.zero)
    println("Binc: >\n%s<".format(hc.mToString(Binc, wolfType2Str)))

    val Ctri = hc.mTm(WolfSemiring, Aadj, Binc)
    println("Ctri: >\n%s<".format(hc.mToString(Ctri, wolfType2Str)))

    def collapse(a: WolfSemiringType): (Long) = {
      if (a._1 < 0) 1L else 0L
    }
    if (DEBUG) {
      val CtriCount = Ctri.map(collapse)
      println("CtriCount: >\n%s<".format(hc.mToString(CtriCount, long2Str)))
    }
    val sr = LagSemiring.plus_times[Long]
    val count = hc.mTv(
      sr,
      Ctri.map(collapse),
      hc.vReplicate(nume, 1L, Option(0L))).
      reduce(sr.addition, sr.addition, 0L) / 3L

    println("Triangle count: >%s<".format(count))

  }
  // ********
  //  test("LagDstrContext.truss") {
  def LagDstrContext_samsi2017static_algo4_truss(sc: SparkContext): Unit = {
    val DEBUG = true
    // ********
    // Graph from samsi2017static

    // some handy constants
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val LongOne = 1L
    val LongZero = 0L

    // for verbose printing
    def long2Str(l: Long): String = {
      if (l == LongInf) " inf"
      else if (l == LongInf - 1L) "inf1"
      else "%4d".format(l)
    }
    def boolean2Str(l: Boolean): String = {
      if (l) " true" else "false"
    }

    def trussType2Str(d: (Long, Long, Long)): String = {
      val d1 = long2Str(d._1)
      val d2 = long2Str(d._2)
      val d3 = long2Str(d._3)
      "(%s,%s,%s)".format(d1, d2, d3)
    }

    //    // ********
    //    // Wolf's: wolf2015task Semiring: Initialization
    //
    //    type WolfSemiringType = Tuple3[Long, Long, Long]
    //
    //    // Ordering for PrimSemiringType
    //    trait WolfSemiringTypeOrdering extends Ordering[WolfSemiringType] {
    //      def compare(u: WolfSemiringType, v: WolfSemiringType): Int = {
    //        if (u._1 < v._1) -1
    //        else if (u._1 > v._1) 1
    //        else if (u._2 < v._2) -1
    //        else if (u._2 > v._2) 1
    //        else if (u._3 < v._3) -1
    //        else if (u._3 > v._3) 1
    //        else 0
    //      }
    //    }
    //
    //    // Numeric for WolfSemiringType
    //    trait WolfSemiringTypeAsNumeric
    //        extends com.ibm.lagraph.LagSemiringAsNumeric[WolfSemiringType]
    //        with WolfSemiringTypeOrdering {
    //      def plus(u: WolfSemiringType, v: WolfSemiringType): WolfSemiringType = {
    //        if ( u == fromInt(0) ) v
    //        else if ( v == fromInt(0) ) u
    //        else if ( (u._1 == v._1) && (u._2 == v._3) && (u._3 == v._2)) (-u._1, -u._2, -u._3)
    //        else fromInt(0)
    //      }
    //      def times(x: WolfSemiringType, y: WolfSemiringType): WolfSemiringType = {
    //        if ((x == fromInt(0)) || (y == fromInt(0))) fromInt(0)
    //        else if (x == fromInt(1)) y
    //        else if (y == fromInt(1)) x
    //        else (y._1, y._2, x._3)
    //      }
    //      def fromInt(x: Int): WolfSemiringType = x match {
    //        case 0 => (LongInf, LongInf, LongInf)
    //        case 1 => (LongInf-1L, LongInf-1L, LongInf-1L)
    //        case other =>
    //          throw new RuntimeException(
    //            "WolfSemiring: fromInt for: >%d< not implemented".format(other))
    //      }
    //    }
    //
    //    implicit object WolfSemiringTypeAsNumeric extends WolfSemiringTypeAsNumeric
    //    val WolfSemiring =
    //      LagSemiring.plus_times[WolfSemiringType]

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)
    //  (12,3)
    //      |1  1  1| (0L, 0L),
    //      |3  1  1| (2L, 0L),
    //      |5  1  1| (4L, 0L),
    //      |1  2  1| (0L, 1L),
    //      |2  2  1| (1L, 1L),
    //      |6  2  1| (5L, 1L),
    //      |2  3  1| (1L, 2L),
    //      |4  3  1| (3L, 2L),
    //      |5  3  1| (4L, 2L),
    //      |3  4  1| (2L, 3L),
    //      |4  4  1| (3L, 3L),
    //      |6  5  1| (5L, 4L),
    // ****
    // truss
    val k = 3
    println("computing k-truss for k: >%s<".format(k))
    // initialize graph edges
    val numv = 6L
    val exampleEdges = List(
      (0L, 0L),
      (2L, 0L),
      (4L, 0L),
      (0L, 1L),
      (1L, 1L),
      (5L, 1L),
      (1L, 2L),
      (3L, 2L),
      (4L, 2L),
      (2L, 3L),
      (3L, 3L),
      (5L, 4L))
    val Ein = sc.parallelize(exampleEdges)

    // ****
    // define adjacency matrix
    val A = Ein
      .flatMap { e =>
        List(
          ((e._1, e._2), LongOne),
          ((e._2, e._1), LongOne))
      }

    // conventional plus_times semiring for Long
    val sr = LagSemiring.plus_times[Long]

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Aadj = hc.mFromRcvRdd((numv, numv), A, LongZero) // WolfSemiring.zero)
    println("Aadj: >\n%s<".format(hc.mToString(Aadj, long2Str)))

    // define incidence matrix
    // E = sparse( ii(:,1), ii(:,2), ii(:,3) );
    val Einc = Ein
      .map { e => ((e._1, e._2), LongOne) }

    val nume = Einc.max()(
      new Ordering[Tuple2[Tuple2[Long, Long], Long]]() {
        override def compare(
          x: ((Long, Long), (Long)),
          y: ((Long, Long), (Long))): Int =
          Ordering[Long].compare(x._1._2, y._1._2)
      })._1._2 + 1L
    println("nume: >%s<".format(nume))

    // some handy constants
    val e0 = hc.vReplicate(nume, 0L, Option(0L))
    val e1 = hc.vReplicate(nume, 1L, Option(0L))

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val E = hc.mFromRcvRdd((numv, nume), Einc, LongZero)
    println("E: >\n%s<".format(hc.mToString(E, long2Str)))

    //    val d = hc.mTv(
    //        sr,
    //        E,
    //        hc.vReplicate(nume, 1L, Option(0L)))
    //    println("d: >\n%s<".format(hc.vToString(d, long2Str)))

    // tmp = E.'*E;
    val tmp = E.transpose.tM(sr, E)
    println("tmp: >\n%s<".format(hc.mToString(tmp, long2Str)))

    // R = E* (tmp-diag(diag(tmp)));
    def diag(v: Long, k: (Long, Long)): Long =
      if (k._1 == k._2) 0L else v
    val tmpd = tmp.zipWithIndex(diag)
    println("tmpd: >\n%s<".format(hc.mToString(tmpd, long2Str)))

    val R = hc.mTm(sr, E, tmpd)
    println("R: >\n%s<".format(hc.mToString(R, long2Str)))

    // s = sum(double(R==2),2);
    val R2 = R.map(e => if (e == 2) 1L else 0L)
    println("R2: >\n%s<".format(hc.mToString(R2, long2Str)))
    val s = hc.mTv(sr, R2, e1)
    println("s: >\n%s<".format(hc.vToString(s, long2Str)))

    // functor for determing the number of non-zeros
    def nnz(s: LagVector[Long]): Long = {
      // number of non-zeros
      s.reduce(sr.addition, sr.addition, 0L)
    }

    //    def any(a: Long, b: Long): Long = {
    //      val aa = if (a != 0L) 1L else 0L
    //      val bb = if (b != 0L) 1L else 0L
    //      aa + bb
    //    }

    // while nnz(xc) ~= nnz(any(E,2))
    def recurse(xc: LagVector[Long], E: LagMatrix[Long]): LagMatrix[Long] =
      if (nnz(xc) == nnz(E.tV(
        sr,
        hc.vReplicate(nume, 1L, Option(0L))).map { v => if (v != 0) 1L else 0L })) { E } else {
        //   E(not(xc),:) = 0;
        val xcNot = xc.map(x => if (x != 0L) false else true)
        println("xcNot: >\n%s<".format(hc.vToString(xcNot, boolean2Str)))
        val EnotXc = E.vToRow(xcNot, hc.vReplicate(nume, 0L, Option(0L)))
        println("EnotXc: >\n%s<".format(hc.mToString(EnotXc, long2Str)))
        // tmp = E.'*E;
        val tmp = EnotXc.transpose.tM(sr, EnotXc)
        println("tmp: >\n%s<".format(hc.mToString(tmp, long2Str)))
        //   R = E* (tmp-diag(diag(tmp)));
        val R = hc.mTm(sr, E, tmpd)
        println("R: >\n%s<".format(hc.mToString(R, long2Str)))
        //   s = sum(double(R==2),2);
        val R2 = R.map(e => if (e == 2) 1L else 0L)
        println("R2: >\n%s<".format(hc.mToString(R2, long2Str)))
        val s = hc.mTv(sr, R2, e1)
        println("s: >\n%s<".format(hc.vToString(s, long2Str)))
        //    xc = s >= k-2;
        val xcNext = gekm2(s)
        println("xcNext: >\n%s<".format(hc.vToString(xcNext, long2Str)))
        recurse(xcNext, EnotXc)
      }
    // xc = s >= k-2;
    // functor for " >= k-2 "
    def gekm2(s: LagVector[Long]): LagVector[Long] = {
      // xc = s >= k-2;
      def f(x: Long) = if (x >= k - 2) 1L else 0L
      s.map(f)
    }
    val xc = gekm2(s)
    println("xc: >\n%s<".format(hc.vToString(xc, long2Str)))
    val Efinal = recurse(xc, E)
    println("Efinal: >\n%s<".format(hc.mToString(Efinal, long2Str)))
  }
  // ********
  //  test("LagDstrContext.samsi2017static_algo1_tricnt") {
  def LagDstrContext_samsi2017static_algo1_tricnt(sc: SparkContext): Unit = {
    val DEBUG = true
    // ********
    // Graph from tsv file

    // some handy constants
    val LongOne = 1L
    val LongZero = 0L
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])

    // for verbose printing
    def long2Str(l: Long): String = {
      if (l == LongInf) " inf"
      else if (l == LongInf - 1L) "inf1"
      else "%4d".format(l)
    }

    // ********
    // Algo1 Semiring

    val sr = LagSemiring.plus_times[Long]

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)

    // ****
    // file stuff
    val root = "/Users/billhorn/git/" +
      "GraphChallenge/SubgraphIsomorphism/data/"
    val file = "A"

    // ****
    // read and initialize adjacency matrix
    val fspecAdj = root + file + "_adj.tsv"
    val rcRddAdj = LagUtils.fileToRcRdd(sc, fspecAdj)
    val numv = LagUtils.rcMaxIndex(rcRddAdj)
    val A = rcRddAdj.map { e => ((e._1, e._2), LongOne) }

    // read and initialize incidence matrix
    val fspecInc = root + file + "_inc.tsv"
    val rcRddInc = LagUtils.fileToRcRdd(sc, fspecInc)
    val B = rcRddInc.flatMap { e =>
      // filter vertices not in adjacency
      if (e._1 < numv && e._2 < numv) List((e._1, e._2)) else None
    }.zipWithIndex().flatMap { ei =>
      List(
        ((ei._1._1, ei._2), 1L),
        ((ei._1._2, ei._2), 1L))
    }

    val nume = B.max()(
      new Ordering[Tuple2[Tuple2[Long, Long], Long]]() {
        override def compare(
          x: ((Long, Long), Long),
          y: ((Long, Long), Long)): Int =
          Ordering[Long].compare(x._1._2, y._1._2)
      })._1._2 + 1L
    println("numv: >%s<, nume: >%s<".format(numv, nume))

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Aadj = hc.mFromRcvRdd((numv, numv), A, 0L)
    if (DEBUG) println("Aadj: >\n%s<".format(hc.mToString(Aadj, long2Str)))

    // same for incidence matrix

    // some handy constants
    val e0 = hc.vReplicate(nume, 0L, Option(0L))
    val e1 = hc.vReplicate(nume, 1L, Option(0L))

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Binc = hc.mFromRcvRdd((numv, nume), B, 0L)
    if (DEBUG) println("Binc: >\n%s<".format(hc.mToString(Binc, long2Str)))

    // C = A*B;
    val Ctri = hc.mTm(sr, Aadj, Binc)
    if (DEBUG) println("Ctri: >\n%s<".format(hc.mToString(Ctri, long2Str)))

    // functor for "nnz( C==2 )"
    def ceq2(a: Long): (Long) = {
      if (a == 2L) 1L else 0L
    }
    // numTriangles = nnz( C==2 ) / 3;
    val count = Ctri.map(ceq2).tV(sr, e1).reduce(sr.addition, sr.addition, 0L) / 3L

    if (DEBUG) println("Ctri.map(ceq2): >\n%s<".format(hc.mToString(Ctri.map(ceq2), long2Str)))

    println("Triangle count: >%s<".format(count))

  }
  // ********
  //  test("LagDstrContext.samsi2017static_algo1_tricnt_debug") {
  def LagDstrContext_samsi2017static_algo1_tricnt_debug(sc: SparkContext): Unit = {
    val DEBUG = true
    // ********
    // Graph from Worked Example in wolf2015task

    // some handy constants
    val LongOne = 1L
    val LongZero = 0L
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])

    // for verbose printing
    def long2Str(l: Long): String = {
      if (l == LongInf) " inf"
      else if (l == LongInf - 1L) "inf1"
      else "%4d".format(l)
    }

    // ********
    // Algo1 Semiring

    val sr = LagSemiring.plus_times[Long]

    // obtain a distributed context for Spark environment
    val nblock = 4 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)

    // ****
    // initialize graph edges
    val numv = 5L
    val exampleEdges = List(
      (0L, 4L),
      (1L, 4L),
      (2L, 4L),
      (3L, 1L),
      (3L, 2L),
      (4L, 3L))
    val E = sc.parallelize(exampleEdges)
    // ****
    // define adjacency matrix
    val A = E
      .flatMap { e =>
        List(
          ((e._1, e._2), LongOne),
          ((e._2, e._1), LongOne))
      }

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Aadj = hc.mFromRcvRdd((numv, numv), A, 0L)
    println("Aadj: >\n%s<".format(hc.mToString(Aadj, long2Str)))

    // define incidence matrix
    val B = E
      .map { e => if (e._1 < e._2) (e._1, e._2) else (e._2, e._1) }
      .zipWithIndex()
      .flatMap { ei =>
        List(
          ((ei._1._1, ei._2), 1L),
          ((ei._1._2, ei._2), 1L))
      }

    val nume = B.max()(
      new Ordering[Tuple2[Tuple2[Long, Long], Long]]() {
        override def compare(
          x: ((Long, Long), Long),
          y: ((Long, Long), Long)): Int =
          Ordering[Long].compare(x._1._2, y._1._2)
      })._1._2 + 1L
    println("nmv: >%s<, nume: >%s<".format(numv, nume))

    // some handy constants
    val e0 = hc.vReplicate(nume, 0L, Option(0L))
    val e1 = hc.vReplicate(nume, 1L, Option(0L))

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Binc = hc.mFromRcvRdd((numv, nume), B, 0L)
    println("Binc: >\n%s<".format(hc.mToString(Binc, long2Str)))

    // C = A*B;
    val Ctri = hc.mTm(sr, Aadj, Binc)
    println("Ctri: >\n%s<".format(hc.mToString(Ctri, long2Str)))

    // functor for "nnz( C==2 )"
    def ceq2(a: Long): (Long) = {
      if (a == 2L) 1L else 0L
    }
    // numTriangles = nnz( C==2 ) / 3;
    val count = Ctri.map(ceq2).tV(sr, e1).reduce(sr.addition, sr.addition, 0L) / 3L

    println("Ctri.map(ceq2): >\n%s<".format(hc.mToString(Ctri.map(ceq2), long2Str)))

    println("Triangle count: >%s<".format(count))

  }
  // ********
  //  test("LagDstrContext.samsi2017static_algo2_tricnt") {
  def LagDstrContext_samsi2017static_algo2_tricnt(sc: SparkContext): Unit = {
    val DEBUG = true
    // ********
    // Graph from Worked Example in wolf2015task

    // some handy constants
    val LongOne = 1L
    val LongZero = 0L
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])

    // for verbose printing
    def long2Str(l: Long): String = {
      if (l == LongInf) " inf"
      else if (l == LongInf - 1L) "inf1"
      else "%4d".format(l)
    }

    // ********
    // Algo2 Semiring

    val sr = LagSemiring.plus_times[Long]

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)

    // ****
    // initialize graph edges
    val numv = 5L
    val exampleEdges = List(
      (0L, 4L),
      (1L, 4L),
      (2L, 4L),
      (3L, 1L),
      (3L, 2L),
      (4L, 3L))
    val E = sc.parallelize(exampleEdges)

    // ****
    // define adjacency matrix
    val A = E
      .flatMap { e =>
        List(
          ((e._1, e._2), LongOne),
          ((e._2, e._1), LongOne))
      }

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Aadj = hc.mFromRcvRdd((numv, numv), A, 0L)
    println("Aadj: >\n%s<".format(hc.mToString(Aadj, long2Str)))

    //    val vZero = hc.vReplicate(numv, 0L, Option(0L))
    val vOne = hc.vReplicate(numv, 1L, Option(0L))

    //    // define incidence matrix
    //    val B = E
    //      .map{e => if (e._1 < e._2) (e._1, e._2) else (e._2, e._1)}
    //      .zipWithIndex()
    //      .flatMap{ei => List(
    //          ((ei._1._1, ei._2), 1L),
    //          ((ei._1._2, ei._2), 1L))}
    //
    //
    //    val nume = B.max()(
    //        new Ordering[Tuple2[Tuple2[Long, Long], Long]]() {
    //      override def compare(
    //          x: ((Long, Long), Long),
    //          y: ((Long, Long), Long)): Int =
    //          Ordering[Long].compare(x._1._2, y._1._2)
    //    })._1._2 + 1L
    //    println("nume: >%s<".format(nume))

    //    // some handy constants
    //    val e0 = hc.vReplicate(nume, 0L, Option(0L))
    //    val e1 = hc.vReplicate(nume, 1L, Option(0L))
    //
    //    // use distributed context-specific utility to convert from RDD to
    //    // adjacency LagMatrix
    //    val Binc = hc.mFromRcvRdd((numv, nume), B, 0L)
    //    println("Binc: >\n%s<".format(hc.mToString(Binc, long2Str)))

    // C = A*B;
    val Ctri = hc.mTm(sr, Aadj, Aadj).hM(sr, Aadj)
    println("Ctri: >\n%s<".format(hc.mToString(Ctri, long2Str)))

    //    // functor for "nnz( C==2 )"
    //    def ceq2(a: Long): (Long) = {
    //      if (a == 2L) 1L else 0L
    //    }
    // numTriangles = nnz( C==2 ) / 3;
    val count = Ctri.tV(sr, vOne).reduce(sr.addition, sr.addition, 0L) / 6L

    if (DEBUG) {
      println("Ctri.tV(sr, vOne): >\n%s<".format(hc.vToString(Ctri.tV(sr, vOne), long2Str)))
    }

    println("Triangle count: >%s<".format(count))

  }
  // ********
  //  test("GpiOps.gpi_m_times_m") {
  def GpiOps_gpi_m_times_m(): Unit = {
    val DEBUG = true
    val graphSizes = List(1, 2, 3, 10, 11, 12)
    for (l <- graphSizes) {
      for (m <- graphSizes) {
        for (n <- graphSizes) {
          if (DEBUG) println(
            "GpiOps.gpi_m_times_m: l: >%s<, m: >%s<, n: >%s<".format(
              l, m, n))

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

          val gpiA = GpiSparseRowMatrix.fromVector(refA, 0L)
          val gpiBt = GpiSparseRowMatrix.fromVector(refB.transpose, 0L)

          //            if (DEBUG) {
          //              println("gpiA")
          //              println(gpiA)
          //              println("gpiBt")
          //              println(gpiBt)
          //            }
          def f(x: Long, y: Long): Long = { x + y }
          def g(x: Long, y: Long): Long = { x * y }

          // the gpi result
          // start w/ transpose end up w/ transpose
          val gpiC = GpiOps.gpi_m_times_m(f, g, f, 0L, gpiA, gpiBt, Option(0L), Option(0L))
          //            if (DEBUG) {
          //              println("gpiC")
          //              println(gpiC)
          //            }

          // compare
          val resGpiT = GpiSparseRowMatrix.toVector(gpiC).transpose
          assert(toArray(resGpiT).deep == toArray(refC).deep)
        }
      }
    }
  }
  // ********
  //  test("gpizip:  sradd: etc") {
  def gpizip_sradd_etc(): Unit = {
    val DEBUG = true
    val sparseValue = 0
    // operations for traditional semiring w/ multiplication and addition
    val sr = LagSemiring.plus_times[Int]
    val srmul = sr.multiplication
    val sradd = sr.addition
    if (true) {
      val v2Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
      val zv4 = GpiAdaptiveVector.gpi_zip(sradd, v2Sparse, v2Sparse, sparseValue)
      if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv4)
      val mv3e = (List(2), List(2))
    }
    if (true) {
      val v2Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
      val v1Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
      val r = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Sparse, sparseValue)
      if (DEBUG) println("r: >%s<".format(r))
      val re = (List(2), List(1))
    }
    if (true) {
      val v2Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
      val v1Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
      val r = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Sparse, sparseValue)
      if (DEBUG) println("r: >%s<".format(r))
      val re = (List(2), List(1))
    }
    if (true) {
      val v2Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 1, 0, 0), sparseValue)
      val v1Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 1, 0, 1), sparseValue)
      val r = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Sparse, sparseValue, Option(0.4))
      if (DEBUG) println("r: >%s<".format(r))
      val re = (List(2, 7, 9), List(1, 2, 1))
    }
    
    if (true) {
      val v2Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 1, 0, 0), sparseValue)
      val v1Sparse =
        GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 1, 0, 1), sparseValue)
      val r1 = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Sparse, sparseValue, Option(0.5))
      if (DEBUG) println("r: >%s<, densecount: >%s<".format(r1, r1.denseCount))
      val re = (List(2, 7, 9), List(1, 2, 1))
    }
  }

  // ********
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // settings from environment
    val spark_home = scala.util.Properties
      .envOrElse("SPARK_HOME", "/home/hduser/spark-1.4.1-bin-hadoop2.6")
    println("spark_home: >%s<".format(spark_home))
    val master = "local[1]"
    println("master: >%s<".format(master))
    val sc = new SparkContext(master, "TestMain")
    //    TestMain.LagDstrContext_vZipWithIndexV3(sc)
    //    TestMain.LagDstrContext_vZipWithIndexSparseV3(sc)
    //    TestMain.LagDstrContext_mZipWithIndexSparseV3(sc)
    //    TestMain.LagDstrContext_mTmV3(sc)
    //    TestMain.LagDstrContext_mTvV3(sc)
    //    TestMain.LagDstrContext_vToMrowV3(sc)
    //    TestMain.LagDstrContext_transposeV3(sc)
    //    TestMain.LagDstrContext_wolf2015task(sc)
    //    TestMain.LagDstrContext_samsi2017static_algo4_truss(sc)
    //    TestMain.LagDstrContext_samsi2017static_algo1_tricnt(sc)
    //    TestMain.LagDstrContext_samsi2017static_algo2_tricnt(sc)
    //    TestMain.GpiOps_gpi_m_times_m()
    TestMain.gpizip_sradd_etc()
  }
}
// scalastyle:on println

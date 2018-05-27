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

import scala.reflect.{ClassTag, classTag}

import scala.collection.mutable.{Map => MMap, ArrayBuffer}

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import com.ibm.lagraph._
object TestMain {
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
  import com.ibm.lagraph.{LagContext, LagSemigroup, LagSemiring, LagVector}
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
    val houseEdges = List(((1L, 0L), 20.0F),
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

    def iterate(weight: Float,
                d: LagVector[PrimSemiringType],
                s: LagVector[Float],
                pi: LagVector[Long])
      : (Float, LagVector[PrimSemiringType], LagVector[Float], LagVector[Long]) =
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
  import com.ibm.lagraph.{LagContext, LagSemigroup, LagSemiring, LagVector}
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
    val houseEdges = List(((1L, 0L), 20.0F),
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

    def iterate(weight: Float,
                d: LagVector[PrimSemiringType],
                s: LagVector[Float],
                pi: LagVector[Long])
      : (Float, LagVector[PrimSemiringType], LagVector[Float], LagVector[Long]) =
      if (hc.vEquiv(s, s_final_test)) (weight, d, s, pi)
      else {
        println("  iterate ****************************************")
        val u = hc.vArgmin(hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication, hc.vMap({
          wp: PrimSemiringType =>
            wp._1
        }, d), s))
        val wp = hc.vEle(d, u._2)
        val aui = hc.vFromMrow(mAdj, u._2)
        iterate(weight + wp._1.get._1,
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

  //  test("LagDstrContext.vEquiv3") {
  def LagDstrContext_vEquiv3(sc: SparkContext): Unit = {
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
  //  test("LagDstrContext.vZipWithIndex3") {
  def LagDstrContext_vZipWithIndex3(sc: SparkContext): Unit = {
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
  //  test("LagDstrContext.vZipWithIndexSparse3") {
  def LagDstrContext_vZipWithIndexSparse3(sc: SparkContext): Unit = {
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
  //  test("LagDstrContext.mZipWithIndexSparse3") {
  def LagDstrContext_mZipWithIndexSparse3(sc: SparkContext): Unit = {
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
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(v.toDouble,
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
//  test("LagDstrContext.mTm3NSQ") {
  def LagDstrContext_mTm3NSQ(sc: SparkContext): Unit = {
    val DEBUG = true
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
              println(nA)
              println(nB)
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
//  test("LagDstrContext.mTm3NSQ2") {
  def LagDstrContext_mTm3NSQ2(sc: SparkContext): Unit = {
    val DEBUG = true
    val add_mul = LagSemiring.plus_times[Double]
    val nblocks = List(1) // (1 until 12).toList
    val nrs = List(5) // (1 until 16).toList
    val ncs = List(6) // (1 until 16).toList
    val sr = LagSemiring.plus_times[Double]
    for (nblock <- nblocks) {
      for (nr <- nrs) {
        for (nc <- ncs) {
          if (DEBUG) println("LagDstrContext.mTm3NSQ2", nr, nc, nblock)
          val hc: LagContext = LagContext.getLagDstrContext(sc, nblock)
          val nA = Vector.tabulate(nr, nr)((r, c) => r * nc + c + 1.0)
          val nB = Vector.tabulate(nr, nc)((r, c) => r * nc + c + 101.0)
          println(nA)
          println(nB)
          val sparseValue = 0.0
          val mA =
            hc.mFromMap((nr, nr),
                LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
          val mB =
            hc.mFromMap((nr, nc),
                LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)

          val mTmRes = hc.mTm(sr, mA, mB)

          val resScala = mult(nA, nB)
          //         println(mTmRes)
          // //        println(toArray(resScala).deep.mkString("\n"))
          assert(
            toArray(LagContext.vectorOfVectorFromMap((nr.toLong, nc.toLong),
                                                     hc.mToMap(mTmRes)._1,
                                                     sparseValue)).deep == toArray(
              resScala).deep)
        }
      }
    }
  }

  // ********
//  test("LagDstrContext.mTv3NSQ") {
  def LagDstrContext_mTv3NSQ(sc: SparkContext): Unit = {
    val DEBUG = true
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

  // ********
//  test("LagDstrContext.wolf2015task") {
  def LagDstrContext_wolf2015task(sc: SparkContext): Unit = {

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
        if ( u == fromInt(0) ) v
        else if ( v == fromInt(0) ) u
        else if ( (u._1 == v._1) && (u._2 == v._3) && (u._3 == v._2)) (-u._1, -u._2, -u._3)
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
        case 1 => (LongInf-1L, LongInf-1L, LongInf-1L)
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
    val exampleEdges = List((0L, 4L),
                            (1L, 4L),
                            (2L, 4L),
                            (3L, 1L),
                            (3L, 2L),
                            (4L, 3L))
    val E = sc.parallelize(exampleEdges)

    // ****
    // define adjacency matrix
    val A = E
      .flatMap{e => List(
          ((e._1, e._2), (e._1, e._2, LongInf)),
          ((e._2, e._1), (e._2, e._1, LongInf))) }

    // use distributed context-specific utility to convert from RDD to
    // adjacency LagMatrix
    val Aadj = hc.mFromRcvRdd((numv, numv), A, WolfSemiring.zero)
    println("Aadj: >\n%s<".format(hc.mToString(Aadj, wolfType2Str)))

    // define incidence matrix
    val B = E
      .map{e => if (e._1 < e._2) (e._1, e._2) else (e._2, e._1)}
      .zipWithIndex()
      .flatMap{ei => List(
          ((ei._1._1, ei._2), (LongInf, LongInf, ei._1._2)),
          ((ei._1._2, ei._2), (LongInf, LongInf, ei._1._1)))}


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

//    A.collect
//      .foreach {kv => println("k: (%s,%s), v: (%s,%s)"
//      .format(kv._1._1, kv._1._2, kv._2._1, kv._2._2))}
//      .foreach {
//        case (x) => println("c: (%s,%s)".format(x._1, x._2))
//      }
//      .foreach {
//        case (k, v) => println("c_i: (%s, (%s,%s))".format(k, v._1, v._2))
//      }

//    B.collect
//      .foreach {
//        case (k, v) => println("k: (%s,%s), v: (%s,%s,%s))".format(
//            k._1, k._2, v._1, v._2, v._3))
//      }
//    B.collect
//      .foreach {
//        case (x) => println("c: (%s,%s)".format(x._1, x._2))
//      }

//    // Undirected
//    val rcvGraph = sc
//      .parallelize(houseEdges)
//      .flatMap { x =>
//        List(((x._1._1, x._1._2), x._2), ((x._1._2, x._1._1), x._2))
//      }
//      .distinct()
//
//    // obtain a distributed context for Spark environment
//    val nblock = 1 // set parallelism (blocks on one axis)
//    val hc = LagContext.getLagDstrContext(sc, numv, nblock)
//
//    // use distributed context-specific utility to convert from RDD to
//    // adjacency LagMatrix
//    val mAdjIn = hc.mFromRcvRdd(rcvGraph, 0.0F)
//    czczczc
//    println("mAdjIn: >\n%s<".format(hc.mToString(mAdjIn, float2Str)))
//
//    val add_mul = LagSemiring.plus_times[Double]
//    val denseGraphSizes = (1 until 16).toList
//    val nblocks = (1 until 12).toList
//    val sr = LagSemiring.plus_times[Double]
//    for (graphSize <- denseGraphSizes) {
//      for (nblock <- nblocks) {
//        println("LagDstrContext.mTm", graphSize, nblock)
//        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
//
//        val nv = graphSize
//
//        val nr = nv
//        val nc = nv
//        val nA = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 1.0)
//        val nB = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 101.0)
//        val sparseValue = 0.0
//        val mA =
//          hc.mFromMap(LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
//        val mB =
//          hc.mFromMap(LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)
//
//        val mTmRes = hc.mTm(sr, mA, mB)
//
//        val resScala = mult(nA, nB)
//        //         println(mA)
//        //         println(mB)
//        //         println(mTmRes)
//        // //        println(toArray(resScala).deep.mkString("\n"))
//        assert(
//          toArray(LagContext.vectorOfVectorFromMap(hc.mToMap(mTmRes)._1,
//                                                   sparseValue,
//                                                   (nv.toLong, nv.toLong))).deep == toArray(
//            resScala).deep)
//      }
//    }
  }

  def isolate(sc: SparkContext): Unit = {
    val numv = 5L
    val nblock = 2 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, nblock)
    val s_final_test = hc.vReplicate(numv, 99)
    val s_i = hc.vSet(hc.vReplicate(numv, 99), 4, -99)
    val test = hc.vEquiv(s_final_test, s_i)
    println("test: >%s<".format(test))
  }
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
    //    TestMain.testit(sc)
    //    TestMain.LagDstrContext_mTv(sc)
    //    TestMain.LagDstrContext_mTv3(sc)
    //    TestMain.LagDstrContext_mTm3(sc)
    //    TestMain.LagDstrContext_vFromMap_Dense(sc)
    //    TestMain.LagDstrContext_vIndices(sc)
    //    TestMain.LagDstrContext_vIndices2(sc)
    //    TestMain.LagDstrContext_vIndices3(sc)
    //    TestMain.LagDstrContext_vZip(sc)
    //        TestMain.LagDstrContext_vZip3(sc)
    //    TestMain.LagSmpContext_vReduceWithIndex(sc)
    //    TestMain.fundamentalPrimsForPub(sc)
    //        isolate(sc)
    //        TestMain.LagDstrContext_vEquiv3(sc)
    //    TestMain.LagDstrContext_vZipWithIndex3(sc)
    //    TestMain.LagDstrContext_vZipWithIndexSparse3(sc)
    //    TestMain.LagDstrContext_mZipWithIndexSparse3(sc)
    //    TestMain.LagDstrContext_mTm3NSQ(sc)
//        TestMain.LagDstrContext_mTm3NSQ2(sc)
    //    TestMain.LagDstrContext_mTv3NSQ(sc)
    TestMain.LagDstrContext_wolf2015task(sc)
  }
}
// scalastyle:on println

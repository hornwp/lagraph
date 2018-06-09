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

import scala.reflect.ClassTag
import scala.{specialized => spec}
import com.ibm.lagraph.LagUtils

import scala.collection.mutable.{Map => MMap}

object GpiOps {

  // GPI base
  /**
    * Creates an vector where each element is set equal to a specified value
    *
    *  @param T type of the new vector.
    *  @param size length of the new vector.
    *  @param x specified value.
    *  @param sparseValue determines the sparsity of the new vector.
    */
  def gpi_replicate[T: ClassTag](
      size: Long,
      x: T,
      sparseValueOpt: Option[T] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T] = {
    if (sparseValueOpt.isDefined) {
      GpiAdaptiveVector.fromSeq(Vector.fill(size.toInt)(x), sparseValueOpt.get)
    } else {
      GpiAdaptiveVector.fillWithSparse(size.toInt)(x)
    }
  }

  /**
    * Creates an vector of type Longs with range [start;start+size) and a step value of 1
    *
    *  @param size length of the new vector.
    *  @param start the start of the range
    *  @param end the end of the range
    */
  def gpi_indices(size: Long,
                  start: Long,
                  stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[Long] = {
    require((size + start) < Int.MaxValue, "Dimension violation")
    GpiAdaptiveVector.fromSeq((start until start + size), 0)
  }

  /**
    * Applies a binary operator to a start value and all elements of this
    *  vector, going left to right.
    *
    *  @tparam T1 the input vector type.
    *  @tparam T2 the result type of the binary operator.
    *  @param z the start value.
    *  @param f the binary operator.
    */
  def gpi_reduce[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag](
      f: (T1, T2) => T2,
      c: (T2, T2) => T2,
      z: T2,
      u: GpiAdaptiveVector[T1],
      stats: Option[GpiAdaptiveVector.Stat] = None): T2 = {
    //      val t0 = System.nanoTime()
    //      println("GpiOps: gpi_reduce: start")
    val res = GpiAdaptiveVector.gpi_reduce(f, c, z, u, stats)
    //      val t1 = System.nanoTime()
    //      val t01 = Utils.tt(t0, t1)
    //      println("GpiOps: gpi_reduce: complete: >%.3f< s".format(t01))
    res
  }

  /**
    * Creates a new vector by applying a unary operator to all elements of the input vector.
    *
    *  @tparam T1 the input vector type.
    *  @tparam T2 the output vector type.
    *  @param f the unary operator.
    *  @param u the input vector
    *
    */
  def gpi_map[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag](
      f: (T1) => T2,
      u: GpiAdaptiveVector[T1],
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T2] = {
    val threshold = u.threshold
    val t0 = System.nanoTime()
    // c    println("GpiOps: gpi_map: start")
    // infer sparseValue
    val sparseValueT2 = f(u.sparseValue)
    val res =
      GpiAdaptiveVector.gpi_map(f, u, stats)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // c    println("GpiOps: gpi_map: complete: >%.3f< s".format(t01))
    res
  }

  /**
    * Creates a new vector by applying a binary operator to pairs formed by
    *  combining two input vector
    *
    *  @tparam T1 first input vector type.
    *  @tparam T2 second input vector type.
    *  @tparam T3 output vector type.
    *  @param f the binary operator.
    *  @param u first input vector.
    *  @param v second input vector.
    *  @param sparseValue determines sparsity of new vector.
    */
  def gpi_zip[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag, @spec(Int) T3: ClassTag](
      f: (T1, T2) => T3,
      u: GpiAdaptiveVector[T1],
      v: GpiAdaptiveVector[T2],
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    val threshold = u.threshold
    //      val t0 = System.nanoTime()
    //      println("GpiOps: gpi_zip: start")
    // infer sparseValue
    val sparseValueT3 = f(u.sparseValue, v.sparseValue)
//    println("WHAT2!",u,v)
    val res = GpiAdaptiveVector.gpi_zip(f, u, v, sparseValueT3, Option(threshold), stats)
    //      val t1 = System.nanoTime()
    //      val t01 = LagUtils.tt(t0, t1)
    //      println("GpiOps: gpi_zip: complete: >%.3f< s".format(t01))
    res
  }
  def gpi_zip_with_index_vector_special[@spec(Int) T1: ClassTag, @spec(Int) T3: ClassTag](
      f: (T1, Long) => T3,
      u: GpiAdaptiveVector[T1],
      base: Long = 0L,
      sparseValueT3Opt: Option[T3] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    val threshold = u.threshold
    //      val t0 = System.nanoTime()
    //      println("GpiOps: gpi_zip: start")
    // infer sparseValue
    //    val sparseValueT3 = sparseValueT3Opt.getOrElse(f(u.sparseValue, v.sparseValue))
    val res = GpiAdaptiveVector.gpi_zip_with_index_special(f,
                                                           u,
                                                           base,
                                                           Option(u.sparseValue),
                                                           sparseValueT3Opt,
                                                           stats)
    //    (f, u, v, sparseValueT3, Option(threshold), stats)
    //      val t1 = System.nanoTime()
    //      val t01 = LagUtils.tt(t0, t1)
    //      println("GpiOps: gpi_zip: complete: >%.3f< s".format(t01))
    res
  }
  def gpi_zip_with_index_vector[@spec(Int) T1: ClassTag, @spec(Int) T3: ClassTag](
      f: (T1, Long) => T3,
      u: GpiAdaptiveVector[T1],
      base: Long = 0L,
      sparseValueT3Opt: Option[T3] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    val threshold = u.threshold
    //      val t0 = System.nanoTime()
    //      println("GpiOps: gpi_zip: start")
    // infer sparseValue
    //    val sparseValueT3 = sparseValueT3Opt.getOrElse(f(u.sparseValue, v.sparseValue))
    val res =
      GpiAdaptiveVector.gpi_zip_with_index(f, u, base, sparseValueT3Opt, stats)
    //    (f, u, v, sparseValueT3, Option(threshold), stats)
    //      val t1 = System.nanoTime()
    //      val t01 = LagUtils.tt(t0, t1)
    //      println("GpiOps: gpi_zip: complete: >%.3f< s".format(t01))
    res
  }
  def gpi_zip_with_index_matrix_special[@spec(Int) T1: ClassTag, @spec(Int) T3: ClassTag](
      f: (T1, (Long, Long)) => T3,
      u: GpiAdaptiveVector[T1],
      rowIndex: Long,
      base: Long = 0L,
      visitDiagonalsOpt: Option[T1] = None,
      sparseValueT3Opt: Option[T3] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    val threshold = u.threshold
    //      val t0 = System.nanoTime()
    //      println("GpiOps: gpi_zip: start")
    // infer sparseValue
    //    val sparseValueT3 = sparseValueT3Opt.getOrElse(f(u.sparseValue, v.sparseValue))
    val res = GpiAdaptiveVector.gpi_zip_with_index_matrix_special(f,
                                                                  u,
                                                                  rowIndex,
                                                                  base,
                                                                  visitDiagonalsOpt,
                                                                  Option(u.sparseValue),
                                                                  sparseValueT3Opt,
                                                                  stats)
    //    (f, u, v, sparseValueT3, Option(threshold), stats)
    //      val t1 = System.nanoTime()
    //      val t01 = LagUtils.tt(t0, t1)
    //      println("GpiOps: gpi_zip: complete: >%.3f< s".format(t01))
    res
  }
  def gpi_zip_with_index_matrix[@spec(Int) T1: ClassTag, @spec(Int) T3: ClassTag](
      f: (T1, (Long, Long)) => T3,
      u: GpiAdaptiveVector[T1],
      rowIndex: Long,
      base: Long = 0L,
      sparseValueT3Opt: Option[T3] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    val threshold = u.threshold
    //      val t0 = System.nanoTime()
    //      println("GpiOps: gpi_zip: start")
    // infer sparseValue
    //    val sparseValueT3 = sparseValueT3Opt.getOrElse(f(u.sparseValue, v.sparseValue))
    val res = GpiAdaptiveVector.gpi_zip_with_index_matrix(f,
                                                          u,
                                                          rowIndex,
                                                          base,
                                                          sparseValueT3Opt,
                                                          stats)
    //    (f, u, v, sparseValueT3, Option(threshold), stats)
    //      val t1 = System.nanoTime()
    //      val t01 = LagUtils.tt(t0, t1)
    //      println("GpiOps: gpi_zip: complete: >%.3f< s".format(t01))
    res
  }

  def gpi_transpose[T: ClassTag](
      a: GpiAdaptiveVector[GpiAdaptiveVector[T]],
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[GpiAdaptiveVector[T]] = {
    //    val atv = GpiSparseRowMatrix.toVector(a).transpose
    //    GpiSparseRowMatrix.fromVector(atv, a(0).sparseValue)
    GpiSparseRowMatrix.transpose(a)
  }

  // ********
  // GPI derived

  /**
    * Compute the inner product between two vector
    *
    *  @tparam T1 first input vector type
    *  @tparam T2 second input vector type
    *  @tparam T3 output type of semiring multiplication
    *  @tparam T4 output type of semiring addition
    *  @param f semiring addition (commutative monoid with identity element)
    *  @param g semiring multiplication (a monoid)
    *  @param zero identity element for semiring addition
    *  @param u first input vector.
    *  @param v second input vector.
    *  @param sparseValue determines sparsity for output of semiring multiplication
    *
    */
  def gpi_innerp[@spec(Int) T1: ClassTag,
                 @spec(Int) T2: ClassTag,
                 @spec(Int) T3: ClassTag,
                 @spec(Int) T4: ClassTag](f: (T3, T4) => T4,
                                          g: (T1, T2) => T3,
                                          c: (T4, T4) => T4,
                                          zero: T4,
                                          u: GpiAdaptiveVector[T1],
                                          v: GpiAdaptiveVector[T2],
                                          stats: Option[GpiAdaptiveVector.Stat] = None): T4 = {
    val threshold = u.threshold
    gpi_reduce(f, c, zero, gpi_zip(g, u, v, stats), stats)
  }
  // TODO too many parameters
  // scalastyle:off parameter.number
  /**
    * Matrix vector multiplication
    *
    *  @tparam T1 matrix element type
    *  @tparam T2 vector element type
    *  @tparam T3 output type of semiring multiplication
    *  @tparam T4 output type of semiring addition
    *  @param f semiring addition (commutative monoid with identity element)
    *  @param g semiring multiplication (a monoid)
    *  @param zero identity element for semiring addition
    *  @param a the matrix
    *  @param u the vector
    *  @param sparseValue determines sparsity for output of semiring multiplication
    *  @param sparseValue determines sparsity for output of semiring addition
    *
    */
  def gpi_m_times_v[@spec(Int) T1: ClassTag,
                    @spec(Int) T2: ClassTag,
                    @spec(Int) T3: ClassTag,
                    @spec(Int) T4: ClassTag](
      f: (T3, T4) => T4,
      g: (T2, T1) => T3,
      c: (T4, T4) => T4,
      zero: T4,
      a: GpiAdaptiveVector[GpiAdaptiveVector[T1]],
      u: GpiAdaptiveVector[T2],
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T4] = {
    val innerpThreshold = u.threshold
    val mapThreshold = a.threshold
    val defdstats = stats.isEmpty
    val activeStats =
      if (stats.isDefined) stats.get else GpiAdaptiveVector.Stat.Stat()
    val t0 = System.nanoTime()
    // c    println("GpiOps: gpi_m_times_v: start")
    val res = gpi_map(
      gpi_innerp(f,
                 g,
                 c,
                 zero,
                 u,
                 _: GpiAdaptiveVector[T1],
                 Option(activeStats)),
      a,
      Option(activeStats)
    )
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    val utype = u match {
      case _: GpiSparseVector[_] => "sparse"
      case _: GpiDenseVector[_] => "dense"
    }
    val vtype = res match {
      case _: GpiSparseVector[_] => "sparse"
      case _: GpiDenseVector[_] => "dense"
    }
    // c    println("GpiOps: gpi_m_times_v: complete: >%s< -> >%s<: time: >%.3f< s, %s".
    //        format(utype, vtype, t01, activeStats))
    res
  }
  // scalastyle:on parameter.number

  // TODO too many parameters
  // scalastyle:off parameter.number
  /**
    * Matrix matrix multiplication
    *
    *  @tparam T1 LH matrix element type
    *  @tparam T2 RH matrix element type
    *  @tparam T3 output type of semiring multiplication
    *  @tparam T4 output type of semiring addition
    *  @param f semiring addition (commutative monoid with identity element)
    *  @param g semiring multiplication (a monoid)
    *  @param zero identity element for semiring addition
    *  @param a the LH matrix
    *  @param u the RH matrix
    *  @param sparseValue determines sparsity for output of semiring multiplication
    *  @param sparseValue determines sparsity for output of semiring addition
    *
    */
  def gpi_m_times_m[@spec(Int) T1: ClassTag,
                    @spec(Int) T2: ClassTag,
                    @spec(Int) T3: ClassTag,
                    @spec(Int) T4: ClassTag](f: (T3, T4) => T4,
                                             g: (T2, T1) => T3,
                                             c: (T4, T4) => T4,
                                             zero: T4,
                                             a: GpiAdaptiveVector[GpiAdaptiveVector[T1]],
                                             u: GpiAdaptiveVector[GpiAdaptiveVector[T2]],
                                             stats: Option[GpiAdaptiveVector.Stat] = None)
    : GpiAdaptiveVector[GpiAdaptiveVector[T4]] = { // : GpiAdaptiveVector[Any] = { //
    val defdstats = stats.isEmpty
    val activeStats =
      if (stats.isDefined) stats.get else GpiAdaptiveVector.Stat.Stat()
    val t0 = System.nanoTime()
//    val res: GpiAdaptiveVector[GpiAdaptiveVector[T4]] = gpi_map(
//      gpi_m_times_v(f,
//                    g,
//                    c,
//                    zero,
//                    a,
//                    _: GpiAdaptiveVector[T2],
//                    Option(activeStats)),
//      u,
//      Option(activeStats)
//    )
    //    val t1 = System.nanoTime()
    //    val t01 = LagUtils.tt(t0, t1)
    //    val utype = u match {
    //      case _: GpiSparseVector[_] => "sparse"
    //      case _: GpiDenseVector[_] => "dense"
    //    }
    //    val vtype = res match {
    //      case _: GpiSparseVector[_] => "sparse"
    //      case _: GpiDenseVector[_] => "dense"
    //    }
    // c    println("GpiOps: gpi_m_times_m: complete: >%s< -> >%s<: time: >%.3f< s, %s"
    //        .format(utype, vtype, t01, activeStats))
//    res
    
    val atype = a match {
      case _: GpiSparseVector[_] => "sparse"
      case _: GpiDenseVector[_] => "dense"
    }
    val utype = u match {
      case _: GpiSparseVector[_] => "sparse"
      case _: GpiDenseVector[_] => "dense"
    }
    
    println("atype: >%s<, utype: >%s<".format(atype, utype))
    val result = u match {
      case ua: GpiSparseVector[GpiAdaptiveVector[T2]] => {
        val rv = ua.rv
//        val sparseValueT1 = a.sparseValue.sparseValue
//        val sparseValueT2 = sparseValue.sparseValue
//        val sparseValueT3 = g(sparseValueT2, sparseValueT1)
        
        val len = rv._1.length
        val rs = Array.ofDim[Int](len)
//        val vs = Array.ofDim[B](len)
        val vs = Array.ofDim[GpiAdaptiveVector[T4]](len)
        var i = 0
        var j = 0
        val k = len
        while (i < k) {
          vs(j) = gpi_m_times_v(f,
                                g,
                                c,
                                zero,
                                a,
                                rv._2(i): GpiAdaptiveVector[T2],
                                Option(activeStats))
//          vs(j) = f(rv._2(i))
          if (vs(j) != ua.sparseValue) {
            rs(j) = rv._1(i)
            j += 1
          }
          i += 1
        }
        val t1 = System.nanoTime()
        val t01 = LagUtils.tt(t0, t1)
        // x    println("GpiBuffer: gpiMapSparseBuffersToSparseBuffers: time: >%.3f< s".format(t01))
        val vSparse = GpiAdaptiveVector.fillWithSparse(a.size)(zero) // TODO zero mayb wrong
        val bi = GpiBuffer(rs, j)
        val bv = GpiBuffer(vs, j)
        val t = ua.threshold
        GpiSparseVector((bi, bv), vSparse, ua.size, ua.threshold)
//        "sparse"
      }
      case ua: GpiDenseVector[GpiAdaptiveVector[T2]] => {
      //  def gpiMapDenseBufferToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B](
//      dbs: GpiBuffer[A],
//      sparseValue: B,
//      f: A => B): (GpiBuffer[B], Int, Int) = {
    //    f: A => B)(implicit eA: ClassTag[A],  eB:ClassTag[B]): (GpiBuffer[B], Int, Int) = {
//    val xA = classTag[A]
//    val xB = classTag[B]
//    val t0 = System.nanoTime()
//    val bs = Array.ofDim[B](dbs.length)
        val dbs = ua.iseq
        val sparseValue = ua.sparseValue
        val bs = Array.ofDim[GpiAdaptiveVector[T4]](dbs.length)
    var i = 0
    val k = dbs.length
    var newDenseCount = 0
    while (i < k) {
          bs(i) = gpi_m_times_v(f,
                                g,
                                c,
                                zero,
                                a,
                                dbs(i): GpiAdaptiveVector[T2],
                                Option(activeStats))
//      bs(i) = f(dbs(i))
      if (bs(i) != sparseValue) newDenseCount += 1
      i += 1
    }
    val vSparse = GpiAdaptiveVector.fillWithSparse(a.size)(zero) // TODO zero mayb wrong
    GpiDenseVector(GpiBuffer(bs), vSparse, newDenseCount, ua.threshold)
//    (GpiBuffer(bs), newDenseCount, k)
//        "dense"
      }
    }
//  def gpiMapDenseBufferToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
//      //  def gpiMapDenseBufferToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B](
//      dbs: GpiBuffer[A],
//      sparseValue: B,
//      f: A => B): (GpiBuffer[B], Int, Int) = {
//    //    f: A => B)(implicit eA: ClassTag[A],  eB:ClassTag[B]): (GpiBuffer[B], Int, Int) = {
//    val xA = classTag[A]
//    val xB = classTag[B]
//    val t0 = System.nanoTime()
//    val bs = Array.ofDim[B](dbs.length)
//    var i = 0
//    val k = dbs.length
//    var newDenseCount = 0
//    val ta = System.nanoTime()
//    val t0a = LagUtils.tt(t0, ta)
//    var xMax = 0.0
//    var xMin = 9999999.0
//    while (i < k) {
//      val xt0 = System.nanoTime()
//      bs(i) = f(dbs(i))
//      if (bs(i) != sparseValue) newDenseCount += 1
//      i += 1
//      val xt1 = System.nanoTime()
//      val xt01 = LagUtils.tt(xt0, xt1)
//      if (xt01 > xMax) xMax = xt01
//      if (xt01 < xMin) xMin = xt01
//    }
//    // x    println("    GpiBuffer:
//    //      gpiMapDenseBufferToDenseBuffer: t0a: >%.3f<, k: >%d<, newDenseCount: >%d<, A:>%s<,
//    //      B:>%s<, xMin: >%.6f<, xMax: >%.6f<".format(t0a, k, newDenseCount, xA, xB, xMin, xMax))
//    val t1 = System.nanoTime()
//    val t01 = LagUtils.tt(t0, t1)
//    // x    println("GpiBuffer: gpiMapDenseBufferToDenseBuffer: time: >%.3f< s".format(t01))
//    (GpiBuffer(bs), newDenseCount, k)
//  }

//  def gpiMapSparseBuffersToSparseBuffers[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
//      rv: (GpiBuffer[Int], GpiBuffer[A]),
//      sparseValue: B,
//      f: A => B): (GpiBuffer[Int], GpiBuffer[B], Int) = {
//    val t0 = System.nanoTime()
//    val len = rv._1.length
//    val rs = Array.ofDim[Int](len)
//    val vs = Array.ofDim[B](len)
//    var i = 0
//    var j = 0
//    val k = len
//    while (i < k) {
//      vs(j) = f(rv._2(i))
//      if (vs(j) != sparseValue) {
//        rs(j) = rv._1(i)
//        j += 1
//      }
//      i += 1
//    }
//    val t1 = System.nanoTime()
//    val t01 = LagUtils.tt(t0, t1)
//    // x    println("GpiBuffer: gpiMapSparseBuffersToSparseBuffers: time: >%.3f< s".format(t01))
//    (GpiBuffer(rs, j), GpiBuffer(vs, j), k)
//  }

//    val vSparse = GpiAdaptiveVector.fillWithSparse(a.size)(zero)
//    val mSparse = GpiAdaptiveVector.fillWithSparse(u.size)(vSparse)
//    mSparse
    result

  }
  def gpi_equiv[T: ClassTag](u: GpiAdaptiveVector[T], v: GpiAdaptiveVector[T]): Boolean = {
    GpiAdaptiveVector.gpi_equiv(u, v)
  }
  // scalastyle:on parameter.number
}

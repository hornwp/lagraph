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
    GpiAdaptiveVector.gpi_inner_product(f, g, c, zero, u, v, Option(threshold), stats)
  }
  def gpi_innerp_old[@spec(Int) T1: ClassTag,
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
    val result = a match {
      case aa: GpiSparseVector[GpiAdaptiveVector[T1]] => {
        val rv = aa.rv

        val len = rv._1.length
        val rs = Array.ofDim[Int](len)
        val vs = Array.ofDim[T4](len)
        var i = 0
        var j = 0
        val k = len
        while (i < k) {
          vs(j) = gpi_innerp(f,
                             g,
                             c,
                             zero,
                             u,
                             rv._2(i): GpiAdaptiveVector[T1],
                             stats)
//          vs(j) = zero
          if (vs(j) != zero) {
            rs(j) = rv._1(i)
            j += 1
          }
          i += 1
        }
        val bi = GpiBuffer(rs, j)
        val bv = GpiBuffer(vs, j)
        GpiSparseVector((bi, bv), zero, aa.size, aa.threshold)
      }
      case aa: GpiDenseVector[GpiAdaptiveVector[T1]] => {
        val dbs = aa.iseq
        val bs = Array.ofDim[T4](dbs.length)
        var i = 0
        val k = dbs.length
        var newDenseCount = 0
        while (i < k) {
          bs(i) = gpi_innerp(f,
                             g,
                             c,
                             zero,
                             u,
                             dbs(i): GpiAdaptiveVector[T1],
                             stats)
//          bs(i) = zero // DEBUG turn off
          if (bs(i) != zero) newDenseCount += 1
          i += 1
        }
        if (newDenseCount < dbs.length * aa.threshold) {
          GpiSparseVector(
            GpiAdaptiveVector.toSparseBuffers(GpiBuffer(bs), zero, newDenseCount),
            zero,
            dbs.length,
            aa.threshold)
        } else {
          GpiDenseVector(GpiBuffer(bs), zero, newDenseCount, aa.threshold)
        }
//        GpiAdaptiveVector.fillWithSparse(a.size)(zero) // DEBUG turn off
      }
    }
    result
  }
  def gpi_m_times_v_old[@spec(Int) T1: ClassTag,
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
  def gpi_m_times_m_opt[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag, @spec(Int) T3: ClassTag, @spec(Int) T4: ClassTag](
      f: (T3, T4) => T4,
      g: (T2, T1) => T3,
      c: (T4, T4) => T4,
      zero: T4,
      a: GpiAdaptiveVector[GpiAdaptiveVector[T1]],
      u: GpiAdaptiveVector[GpiAdaptiveVector[T2]],
      statsOption: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[GpiAdaptiveVector[T4]] = {
    
//    def intersect[T](ths: Array[Int], thsV: Array[T], that: Array[Int], thatV: Array[T]): List[(T,T)] = {
//      val occ = occCounts(that.seq)
//      val b = new scala.collection.mutable.ListBuffer[(T,T)]() // newBuilder
//      for (x <- ths) {
//        val ox = occ(x)  // Avoid multiple map lookups
//        if (ox > 0) {
////          b += x
//          b += Tuple2(thsV(x), thatV(x))
//          occ(x) = ox - 1
//        }
//      }
//      b.result()
//    }
//    def intersect[T](ths: Array[Int], that: Array[Int]): List[(Int, Int)] = {
//      val occ = occCounts(that.seq)
//      val b = new scala.collection.mutable.ListBuffer[(Int, Int)]() // newBuilder
//      for (x <- ths.zipWithIndex) {
//        val ox = occ(x._1)  // Avoid multiple map lookups
//        if (ox > -1) {
//          b += Tuple2(x._2, ox)
//          occ(x._1) = -1
//        }
//      }
//      b.result()
//    }
    def intersect[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag, @spec(Int) T3: ClassTag, @spec(Int) T4: ClassTag](
      f: (T3, T4) => T4,
      g: (T2, T1) => T3,
      zero: T4,
      ths: Array[Int], thsV: Array[T1], that: Array[Int], thatV: Array[T2]): T4 = {
      val occ = occCounts(that.seq)
      var res = zero
      for (x <- ths.zipWithIndex) {
        val ox = occ(x._1)  // Avoid multiple map lookups
        if (ox > -1) {
          res = f(g(thatV(ox), thsV(x._2)), res)
          occ(x._1) = -1
        }
      }
      res
    }
  
//    def occCounts[B](sq: Seq[B]): scala.collection.mutable.Map[B, Int] = {
//      val occ = new scala.collection.mutable.HashMap[B, Int] { override def default(k: B) = 0 }
//      for (y <- sq) occ(y) += 1
//      occ
//    }
    def occCounts[B, T](sq: Seq[B]): scala.collection.mutable.Map[B, Int] = {
      val occ = new scala.collection.mutable.HashMap[B, Int] { override def default(k: B) = -1 }
      for (y <- sq.zipWithIndex) occ(y._1) = y._2
      occ
    }
    var SS = 0
    var SD = 0
    var DD = 0
    val stats = statsOption.get
    var opsTot = 0
    // ********
//    val zero = 0
    val Ain = a
    val Bin = u
    val vSparse = GpiAdaptiveVector.fillWithSparse(Ain.size)(zero)
    val A = Ain.asInstanceOf[GpiDenseVector[GpiAdaptiveVector[T1]]]
    val B = Bin.asInstanceOf[GpiDenseVector[GpiAdaptiveVector[T2]]]
    val t0 = System.nanoTime()
    val dbs = B.iseq
    val dbsx = A.iseq
    val kx = dbsx.length
    //              val sparseValue = B.sparseValue
    //            println("XXXXX: A.sparse: (%s,%s) >%s< >%s<".format(r,c, A(0).length, A.sparseValue.length))
    //            println("XXXXX: B.sparse: (%s,%s) >%s< >%s<".format(r,c, B(0).length, B.sparseValue.length))
    //              println("XXXXX: A.sparse: >%s<".format(A.sparseValue))
    //              println("XXXXX: B.sparse: >%s<".format(B.sparseValue))
    //              println("dbs.length:>%s<, dbsx.length:>%s<".format(dbs.length, dbsx.length)) //(6431,1024)
    val bs = Array.ofDim[GpiAdaptiveVector[T4]](dbs.length)
    var i = 0
    val k = dbs.length
    var newDenseCount = 0
    while (i < k) {
      // gpi_m_times_v
      val u = dbs(i)
      var ix = 0
      var newDenseCountx = 0
      val bsx = Array.ofDim[T4](dbsx.length) // careful, with this!
      while (ix < kx) {
        //  gpi_innerp
        // gpi_inner_product
        val v = dbsx(ix)
        bsx(ix) =
          (u, v) match {
            case (uSparse: GpiSparseVector[T1], vSparse: GpiSparseVector[T2]) => {
              SS += 1
              //****
//              val occ = occCounts(vSparse.rv._1.elems)
//              var res = zero
//              for (x <- uSparse.rv._1.elems.zipWithIndex) {
//                val ox = occ(x._1)  // Avoid multiple map lookups
//                if (ox > -1) {
//                  res = f(g(vSparse.rv._2.elems(ox), uSparse.rv._2.elems(x._2)), res)
////                  occ(x._1) = -1
//                }
//              }
//              res
              //****
              var res = zero
              if (uSparse.rv._1.elems.size < vSparse.rv._1.elems.size) {
                for (x <- uSparse.rv._1.elems.zipWithIndex) {
                  val ox = vSparse.rv._1.asmap(x._1)
                  if (ox > -1) {
                    res = f(g(uSparse.rv._2.elems(x._2), vSparse.rv._2.elems(ox)), res)
                    opsTot += 1
                  }
                }
              } else {
                for (x <- vSparse.rv._1.elems.zipWithIndex) {
                  val ox = uSparse.rv._1.asmap(x._1)
                  if (ox > -1) {
                    res = f(g(uSparse.rv._2.elems(ox), vSparse.rv._2.elems(x._2)), res)
                    opsTot += 1
                  }
                }
              }
              res
              //****
//              val occ = occCounts(uSparse.rv._1.elems)
////              print("S%s-%s".format(uSparse.rv._1.elems.size,vSparse.rv._1.elems.size))
//              var res = zero
//              for (x <- vSparse.rv._1.elems.zipWithIndex) {
//                val ox = occ(x._1)  // Avoid multiple map lookups
//                if (ox > -1) {
//                  res = f(g(uSparse.rv._2.elems(ox), vSparse.rv._2.elems(x._2)), res)
//                  opsTot += 1
////                  occ(x._1) = -1
//                }
//              }
//              res
              //****
//              var res = zero
//              val test = vSparse.rv._1.elems.intersect(uSparse.rv._1.elems)
////              val test = uSparse.rv._1.elems.intersect(vSparse.rv._1.elems)
//              for (e <- test) {
//                opsTot += 1
//              }
//              res
              //****
//              val res = intersect(f, g, zero, uSparse.rv._1.elems, uSparse.rv._2.elems, vSparse.rv._1.elems, vSparse.rv._2.elems)
//              var res = zero
//              val intersection = intersect(uSparse.rv._1.elems, vSparse.rv._1.elems)
//              val intersection = intersect(vSparse.rv._1.elems, uSparse.rv._1.elems)
//              for (e <- intersection) {
////                res = f(g(uSparse.rv._2.elems(e._1), vSparse.rv._2.elems(e._2)), res)
//                res = f(g(uSparse.rv._2.elems(e._2), vSparse.rv._2.elems(e._1)), res)
//                opsTot += 1
////                res = zero
////                res = f(g(uSparse.rv._2(e), vSparse.rv._2(e)), res)
//              }
//              res
              //****
//              // GpiBuffer.gpiZipSparseSparseToSparseReduce
//              val lenA = uSparse.rv._1.length
//              val lenB = vSparse.rv._1.length
//              if (lenA < lenB) {
//                var res = zero
//                var iiA = 0
//                var iiB = 0
//                while (iiA < lenA && iiB < lenB) {
//                  if (uSparse.rv._1(iiA) < vSparse.rv._1(iiB)) {
//                    iiA += 1
//                  } else if (uSparse.rv._1(iiA) > vSparse.rv._1(iiB)) {
//                    iiB += 1
//                  } else {
//                    res = f(g(uSparse.rv._2(iiA), vSparse.rv._2(iiB)), res)
////                    res += uSparse.rv._2(iiA) * vSparse.rv._2(iiB)
//                    opsTot += 1
//                    iiA += 1
//                    iiB += 1
//                  }
//                }
//                //      (res, iiO)
//                res
//              } else {
//                var res = zero
//                var iiA = 0
//                var iiB = 0
//                while (iiA < lenA && iiB < lenB) {
//                  if (vSparse.rv._1(iiB) < uSparse.rv._1(iiA)) {
//                    iiB += 1
//                  } else if (vSparse.rv._1(iiB) > uSparse.rv._1(iiA)) {
//                    iiA += 1
//                  } else {
//                    res = f(g(uSparse.rv._2(iiA), vSparse.rv._2(iiB)), res)
////                    res += uSparse.rv._2(iiA) * vSparse.rv._2(iiB)
//                    opsTot += 1
//                    iiA += 1
//                    iiB += 1
//                  }
//                }
//                //      (res, iiO)
//                res
//              }
            }
            case (uSparse: GpiSparseVector[T4], vDense: GpiDenseVector[T4]) => {
              SD += 1
              // GpiBuffer.gpiZipSparseDenseToSparseReduce
              var res = zero
              val lenA = uSparse.rv._1.length
              //                        val lenB = vDense.iseq.length
              var iiA = 0
              //                        var iB = 0
              while (iiA < lenA) {
                res = f(g(uSparse.rv._2(iiA), vDense.iseq(uSparse.rv._1(iiA))), res)
//                res += uSparse.rv._2(iiA) * vDense.iseq(uSparse.rv._1(iiA))
                iiA += 1
              }
              opsTot += lenA
              res
            }
            case (uDense: GpiDenseVector[T4], vSparse: GpiSparseVector[T4]) => {
              SD += 1
              // GpiBuffer.gpiZipDenseSparseToSparseReduce
              var res = zero
              //                        val lenA = uDense.iseq.length
              val lenB = vSparse.rv._1.length
              //                        var iA = 0
              var iiB = 0
              while (iiB < lenB) {
                res = f(g(uDense.iseq(vSparse.rv._1(iiB)), vSparse.rv._2(iiB)), res)
//                res += uDense.iseq(vSparse.rv._1(iiB)) * vSparse.rv._2(iiB)
                iiB += 1
              }
              opsTot += lenB
              res
            }
            case (uDense: GpiDenseVector[T4], vDense: GpiDenseVector[T4]) => {
              DD += 1
              // GpiBuffer.gpiZipDenseDenseToDenseReduce
              var res = zero
              val lenA = uDense.iseq.length
              val lenB = vDense.iseq.length
              //                        var iA = 0
              //                        var iB = 0
              var iC = 0
              //    while (iC < len) {
              while (iC < lenA) {
                res = f(g(uDense.iseq(iC), vDense.iseq(iC)), res)
//                res += uDense.iseq(iC) * vDense.iseq(iC)
                iC += 1
                //                          iA += 1
                //                          iB += 1
              }
              opsTot += lenA
              res
            }
          }
        // ****
        //                    bsx(ix) = zero // DEBUG turn off
        if (bsx(ix) != zero) newDenseCountx += 1
        ix += 1
      }
      bs(i) = // addded
        if (newDenseCountx < dbsx.length * A.threshold) {
          val x = GpiSparseVector(
            GpiAdaptiveVector.toSparseBuffers(GpiBuffer(bsx), zero, newDenseCountx),
            zero,
            dbsx.length,
            A.threshold)
          //                    print("S(%s,%s)%s".format(r, c, kx))
          x
        } else {
          val x = GpiDenseVector(GpiBuffer(bsx), zero, newDenseCountx, A.threshold)
          //                    print("D%s".format(x.length))
          x
        }
      //          bs(i) = vSparse // DEBUG turn off mTv
      if (bs(i) != vSparse) newDenseCount += 1
      //                println("B%s".format(bs(i).length))
      i += 1
    }
    if (statsOption.isDefined) {
      statsOption.get.increment(f, opsTot)
      statsOption.get.increment(g, opsTot)
      statsOption.get.incrementSS(SS)
      statsOption.get.incrementSD(SD)
      statsOption.get.incrementDD(DD)
    }
    val res = GpiDenseVector(GpiBuffer(bs), vSparse, newDenseCount, A.threshold)
    // ********
    res.asInstanceOf[GpiAdaptiveVector[GpiAdaptiveVector[T4]]]
    //    }

  }
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
  def gpi_m_times_m_opt_keep[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag, @spec(Int) T3: ClassTag, @spec(Int) T4: ClassTag](
      f: (T3, T4) => T4,
      g: (T2, T1) => T3,
      c: (T4, T4) => T4,
      zero: T4,
      a: GpiAdaptiveVector[GpiAdaptiveVector[T1]],
      u: GpiAdaptiveVector[GpiAdaptiveVector[T2]],
      statsOption: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[GpiAdaptiveVector[T4]] = {
    var SS = 0
    var SD = 0
    var DD = 0
    val stats = statsOption.get
    var opsTot = 0
    // ********
//    val zero = 0
    val Ain = a
    val Bin = u
    val vSparse = GpiAdaptiveVector.fillWithSparse(Ain.size)(zero)
    val A = Ain.asInstanceOf[GpiDenseVector[GpiAdaptiveVector[T1]]]
    val B = Bin.asInstanceOf[GpiDenseVector[GpiAdaptiveVector[T2]]]
    val t0 = System.nanoTime()
    val dbs = B.iseq
    val dbsx = A.iseq
    val kx = dbsx.length
    //              val sparseValue = B.sparseValue
    //            println("XXXXX: A.sparse: (%s,%s) >%s< >%s<".format(r,c, A(0).length, A.sparseValue.length))
    //            println("XXXXX: B.sparse: (%s,%s) >%s< >%s<".format(r,c, B(0).length, B.sparseValue.length))
    //              println("XXXXX: A.sparse: >%s<".format(A.sparseValue))
    //              println("XXXXX: B.sparse: >%s<".format(B.sparseValue))
    //              println("dbs.length:>%s<, dbsx.length:>%s<".format(dbs.length, dbsx.length)) //(6431,1024)
    val bs = Array.ofDim[GpiAdaptiveVector[T4]](dbs.length)
    var i = 0
    val k = dbs.length
    var newDenseCount = 0
    while (i < k) {
      // gpi_m_times_v
      val u = dbs(i)
      var ix = 0
      var newDenseCountx = 0
      val bsx = Array.ofDim[T4](dbsx.length) // careful, with this!
      while (ix < kx) {
        //  gpi_innerp
        // gpi_inner_product
        val v = dbsx(ix)
        bsx(ix) =
          (u, v) match {
            case (uSparse: GpiSparseVector[T4], vSparse: GpiSparseVector[T4]) => {
              SS += 1
              // GpiBuffer.gpiZipSparseSparseToSparseReduce
              val lenA = uSparse.rv._1.length
              val lenB = vSparse.rv._1.length
              if (lenA < lenB) {
                var res = zero
                var iiA = 0
                var iiB = 0
                while (iiA < lenA && iiB < lenB) {
                  if (uSparse.rv._1(iiA) < vSparse.rv._1(iiB)) {
                    iiA += 1
                  } else if (uSparse.rv._1(iiA) > vSparse.rv._1(iiB)) {
                    iiB += 1
                  } else {
                    res = f(g(uSparse.rv._2(iiA), vSparse.rv._2(iiB)), res)
//                    res += uSparse.rv._2(iiA) * vSparse.rv._2(iiB)
                    opsTot += 1
                    iiA += 1
                    iiB += 1
                  }
                }
                //      (res, iiO)
                res
              } else {
                var res = zero
                var iiA = 0
                var iiB = 0
                while (iiA < lenA && iiB < lenB) {
                  if (vSparse.rv._1(iiB) < uSparse.rv._1(iiA)) {
                    iiB += 1
                  } else if (vSparse.rv._1(iiB) > uSparse.rv._1(iiA)) {
                    iiA += 1
                  } else {
                    res = f(g(uSparse.rv._2(iiA), vSparse.rv._2(iiB)), res)
//                    res += uSparse.rv._2(iiA) * vSparse.rv._2(iiB)
                    opsTot += 1
                    iiA += 1
                    iiB += 1
                  }
                }
                //      (res, iiO)
                res
              }
            }
            case (uSparse: GpiSparseVector[T4], vDense: GpiDenseVector[T4]) => {
              SD += 1
              // GpiBuffer.gpiZipSparseDenseToSparseReduce
              var res = zero
              val lenA = uSparse.rv._1.length
              //                        val lenB = vDense.iseq.length
              var iiA = 0
              //                        var iB = 0
              while (iiA < lenA) {
                res = f(g(uSparse.rv._2(iiA), vDense.iseq(uSparse.rv._1(iiA))), res)
//                res += uSparse.rv._2(iiA) * vDense.iseq(uSparse.rv._1(iiA))
                iiA += 1
              }
              opsTot += lenA
              res
            }
            case (uDense: GpiDenseVector[T4], vSparse: GpiSparseVector[T4]) => {
              SD += 1
              // GpiBuffer.gpiZipDenseSparseToSparseReduce
              var res = zero
              //                        val lenA = uDense.iseq.length
              val lenB = vSparse.rv._1.length
              //                        var iA = 0
              var iiB = 0
              while (iiB < lenB) {
                res = f(g(uDense.iseq(vSparse.rv._1(iiB)), vSparse.rv._2(iiB)), res)
//                res += uDense.iseq(vSparse.rv._1(iiB)) * vSparse.rv._2(iiB)
                iiB += 1
              }
              opsTot += lenB
              res
            }
            case (uDense: GpiDenseVector[T4], vDense: GpiDenseVector[T4]) => {
              DD += 1
              // GpiBuffer.gpiZipDenseDenseToDenseReduce
              var res = zero
              val lenA = uDense.iseq.length
              val lenB = vDense.iseq.length
              //                        var iA = 0
              //                        var iB = 0
              var iC = 0
              //    while (iC < len) {
              while (iC < lenA) {
                res = f(g(uDense.iseq(iC), vDense.iseq(iC)), res)
//                res += uDense.iseq(iC) * vDense.iseq(iC)
                iC += 1
                //                          iA += 1
                //                          iB += 1
              }
              opsTot += lenA
              res
            }
          }
        // ****
        //                    bsx(ix) = zero // DEBUG turn off
        if (bsx(ix) != zero) newDenseCountx += 1
        ix += 1
      }
      bs(i) = // addded
        if (newDenseCountx < dbsx.length * A.threshold) {
          val x = GpiSparseVector(
            GpiAdaptiveVector.toSparseBuffers(GpiBuffer(bsx), zero, newDenseCountx),
            zero,
            dbsx.length,
            A.threshold)
          //                    print("S(%s,%s)%s".format(r, c, kx))
          x
        } else {
          val x = GpiDenseVector(GpiBuffer(bsx), zero, newDenseCountx, A.threshold)
          //                    print("D%s".format(x.length))
          x
        }
      //          bs(i) = vSparse // DEBUG turn off mTv
      if (bs(i) != vSparse) newDenseCount += 1
      //                println("B%s".format(bs(i).length))
      i += 1
    }
    if (statsOption.isDefined) {
      statsOption.get.increment(f, opsTot)
      statsOption.get.increment(g, opsTot)
      statsOption.get.incrementSS(SS)
      statsOption.get.incrementSD(SD)
      statsOption.get.incrementDD(DD)
    }
    val res = GpiDenseVector(GpiBuffer(bs), vSparse, newDenseCount, A.threshold)
    // ********
    res.asInstanceOf[GpiAdaptiveVector[GpiAdaptiveVector[T4]]]
    //    }

  }
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
  def gpi_m_times_m_opt2[@spec(Int) T1: ClassTag, @spec(Int) T2: ClassTag, @spec(Int) T3: ClassTag, @spec(Int) T4: ClassTag](
      f: (T3, T4) => T4,
      g: (T2, T1) => T3,
      c: (T4, T4) => T4,
      zero: T4,
      a: GpiAdaptiveVector[GpiAdaptiveVector[T1]],
      u: GpiAdaptiveVector[GpiAdaptiveVector[T2]],
      statsOption: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[GpiAdaptiveVector[T4]] = {
    val stats = statsOption.get
    var opsTot = 0
    // ********
    val zero = 0
    val Ain = a
    val Bin = u
    val vSparse = GpiAdaptiveVector.fillWithSparse(Ain.size)(zero)
    val A = Ain.asInstanceOf[GpiDenseVector[GpiAdaptiveVector[Int]]]
    val B = Bin.asInstanceOf[GpiDenseVector[GpiAdaptiveVector[Int]]]
    val t0 = System.nanoTime()
    val dbs = B.iseq
    val dbsx = A.iseq
    val kx = dbsx.length
    //              val sparseValue = B.sparseValue
    //            println("XXXXX: A.sparse: (%s,%s) >%s< >%s<".format(r,c, A(0).length, A.sparseValue.length))
    //            println("XXXXX: B.sparse: (%s,%s) >%s< >%s<".format(r,c, B(0).length, B.sparseValue.length))
    //              println("XXXXX: A.sparse: >%s<".format(A.sparseValue))
    //              println("XXXXX: B.sparse: >%s<".format(B.sparseValue))
    //              println("dbs.length:>%s<, dbsx.length:>%s<".format(dbs.length, dbsx.length)) //(6431,1024)
    val bs = Array.ofDim[GpiAdaptiveVector[Int]](dbs.length)
    var i = 0
    val k = dbs.length
    var newDenseCount = 0
    while (i < k) {
      // gpi_m_times_v
      val u = dbs(i)
      var ix = 0
      var newDenseCountx = 0
      val bsx = Array.ofDim[Int](dbsx.length) // careful, with this!
      while (ix < kx) {
        //  gpi_innerp
        // gpi_inner_product
        val v = dbsx(ix)
        bsx(ix) =
          (u, v) match {
            case (uSparse: GpiSparseVector[Int], vSparse: GpiSparseVector[Int]) => {
              // GpiBuffer.gpiZipSparseSparseToSparseReduce
              val lenA = uSparse.rv._1.length
              val lenB = vSparse.rv._1.length
              if (lenA < lenB) {
                var res = zero
                var iiA = 0
                var iiB = 0
                while (iiA < lenA && iiB < lenB) {
                  if (uSparse.rv._1(iiA) < vSparse.rv._1(iiB)) {
                    iiA += 1
                  } else if (uSparse.rv._1(iiA) > vSparse.rv._1(iiB)) {
                    iiB += 1
                  } else {
                    //          res = f(g(uSparse.rv._2(iiA), vSparse.rv._2(iiB)), res)
                    res += uSparse.rv._2(iiA) * vSparse.rv._2(iiB)
                    opsTot += 1
                    iiA += 1
                    iiB += 1
                  }
                }
                //      (res, iiO)
                res
              } else {
                var res = zero
                var iiA = 0
                var iiB = 0
                while (iiA < lenA && iiB < lenB) {
                  if (vSparse.rv._1(iiB) < uSparse.rv._1(iiA)) {
                    iiB += 1
                  } else if (vSparse.rv._1(iiB) > uSparse.rv._1(iiA)) {
                    iiA += 1
                  } else {
                    //          res = f(g(uSparse.rv._2(iiA), vSparse.rv._2(iiB)), res)
                    res += uSparse.rv._2(iiA) * vSparse.rv._2(iiB)
                    opsTot += 1
                    iiA += 1
                    iiB += 1
                  }
                }
                //      (res, iiO)
                res
              }
            }
            case (uSparse: GpiSparseVector[Int], vDense: GpiDenseVector[Int]) => {
              // GpiBuffer.gpiZipSparseDenseToSparseReduce
              var res = zero
              val lenA = uSparse.rv._1.length
              //                        val lenB = vDense.iseq.length
              var iiA = 0
              //                        var iB = 0
              while (iiA < lenA) {
                //      res = f(g(uSparse.rv._2(iiA), vDense.iseq(uSparse.rv._1(iiA))), res)
                res += uSparse.rv._2(iiA) * vDense.iseq(uSparse.rv._1(iiA))
                iiA += 1
              }
              opsTot += lenA
              res
            }
            case (uDense: GpiDenseVector[Int], vSparse: GpiSparseVector[Int]) => {
              // GpiBuffer.gpiZipDenseSparseToSparseReduce
              var res = zero
              //                        val lenA = uDense.iseq.length
              val lenB = vSparse.rv._1.length
              //                        var iA = 0
              var iiB = 0
              while (iiB < lenB) {
                //      res = f(g(uDense.iseq(vSparse.rv._1(iiB)), vSparse.rv._2(iiB)), res)
                res += uDense.iseq(vSparse.rv._1(iiB)) * vSparse.rv._2(iiB)
                iiB += 1
              }
              opsTot += lenB
              res
            }
            case (uDense: GpiDenseVector[Int], vDense: GpiDenseVector[Int]) => {
              // GpiBuffer.gpiZipDenseDenseToDenseReduce
              var res = zero
              val lenA = uDense.iseq.length
              val lenB = vDense.iseq.length
              //                        var iA = 0
              //                        var iB = 0
              var iC = 0
              //    while (iC < len) {
              while (iC < lenA) {
                //      res = f(g(uDense.iseq(iA), vDense.iseq(iB)), res)
                res += uDense.iseq(iC) * vDense.iseq(iC)
                iC += 1
                //                          iA += 1
                //                          iB += 1
              }
              opsTot += lenA
              res
            }
          }
        // ****
        //                    bsx(ix) = zero // DEBUG turn off
        if (bsx(ix) != zero) newDenseCountx += 1
        ix += 1
      }
      bs(i) = // addded
        if (newDenseCountx < dbsx.length * A.threshold) {
          val x = GpiSparseVector(
            GpiAdaptiveVector.toSparseBuffers(GpiBuffer(bsx), zero, newDenseCountx),
            zero,
            dbsx.length,
            A.threshold)
          //                    print("S(%s,%s)%s".format(r, c, kx))
          x
        } else {
          val x = GpiDenseVector(GpiBuffer(bsx), zero, newDenseCountx, A.threshold)
          //                    print("D%s".format(x.length))
          x
        }
      //          bs(i) = vSparse // DEBUG turn off mTv
      if (bs(i) != vSparse) newDenseCount += 1
      //                println("B%s".format(bs(i).length))
      i += 1
    }
    if (statsOption.isDefined) {
      statsOption.get.increment(f, opsTot)
      statsOption.get.increment(g, opsTot)
    }
    val res = GpiDenseVector(GpiBuffer(bs), vSparse, newDenseCount, A.threshold)
    // ********
    res.asInstanceOf[GpiAdaptiveVector[GpiAdaptiveVector[T4]]]
    //    }

  }
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
                    @spec(Int) T4: ClassTag](
      f: (T3, T4) => T4,
      g: (T2, T1) => T3,
      c: (T4, T4) => T4,
      zero: T4,
      a: GpiAdaptiveVector[GpiAdaptiveVector[T1]],
      u: GpiAdaptiveVector[GpiAdaptiveVector[T2]],
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[GpiAdaptiveVector[T4]] = {
    val defdstats = stats.isEmpty
    val activeStats =
      if (stats.isDefined) stats.get else GpiAdaptiveVector.Stat.Stat()
//    val t0 = System.nanoTime()
//    val t0Add = activeStats.getAdd
//    val t0Mul = activeStats.getMul

//    val atype = a match {
//      case _: GpiSparseVector[_] => "sparse"
//      case _: GpiDenseVector[_] => "dense"
//    }
//    val utype = u match {
//      case _: GpiSparseVector[_] => "sparse"
//      case _: GpiDenseVector[_] => "dense"
//    }
//
//    println("atype: >%s<, utype: >%s<".format(atype, utype))
    val vSparse = GpiAdaptiveVector.fillWithSparse(a.size)(zero)
    val result = u match {
      case ua: GpiSparseVector[GpiAdaptiveVector[T2]] => {
        val rv = ua.rv
        val len = rv._1.length
        val rs = Array.ofDim[Int](len)
        val vs = Array.ofDim[GpiAdaptiveVector[T4]](len)
        var i = 0
        var j = 0
        val k = len
        while (i < k) {
          vs(j) = gpi_m_times_v(
            f,
            g,
            c,
            zero,
            a,
            rv._2(i): GpiAdaptiveVector[T2],
            Option(activeStats))
          if (vs(j) != ua.sparseValue) {
            rs(j) = rv._1(i)
            j += 1
          }
          i += 1
        }
        val bi = GpiBuffer(rs, j)
        val bv = GpiBuffer(vs, j)
        val t = ua.threshold
        GpiSparseVector((bi, bv), vSparse, ua.size, ua.threshold)
      }
      case ua: GpiDenseVector[GpiAdaptiveVector[T2]] => {
        val dbs = ua.iseq
        val sparseValue = ua.sparseValue
        val bs = Array.ofDim[GpiAdaptiveVector[T4]](dbs.length)
        var i = 0
        val k = dbs.length
        var newDenseCount = 0
        while (i < k) {
          //              dbs(i) match {
          //                case _: GpiSparseVector[_] => print ("s%s".format(dbs(i).denseCount))
          //                case _: GpiDenseVector[_] => print ("d%s".format(dbs(i).denseCount))
          //              }
          bs(i) = gpi_m_times_v(
            f,
            g,
            c,
            zero,
            a,
            dbs(i): GpiAdaptiveVector[T2],
            Option(activeStats))
//          bs(i) = vSparse // DEBUG turn off mTv
          if (bs(i) != vSparse) newDenseCount += 1
          i += 1
        }
        GpiDenseVector(GpiBuffer(bs), vSparse, newDenseCount, ua.threshold)
        //        GpiAdaptiveVector.fillWithSparse(u.size)(vSparse)
      }
    }
    // above adapted from:
    //  def gpiMapDenseBufferToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
    //  def gpiMapSparseBuffersToSparseBuffers[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](

    //    val vSparse = GpiAdaptiveVector.fillWithSparse(a.size)(zero)
    //    val mSparse = GpiAdaptiveVector.fillWithSparse(u.size)(vSparse)
    //    mSparse
//    val t01 = (System.nanoTime() - t0) * 1.0e-9
//    val t01Add = activeStats.getAdd - t0Add
//    val t01Mul = activeStats.getMul - t0Mul
//    
//    
//    val mflops =  (t01Add + t01Mul).toDouble / t01 * 1.0e-6
//    println ("mTm: t: >%.3f<, mflops: >%.3f<, adds: >(%s,%s), muls: >(%s,%s)".format(
//        t01, mflops, t01Add, LagUtils.pow2Bound(t01Add), t01Mul, LagUtils.pow2Bound(t01Mul)))
    //        println("GpiOps: DENSE: time: >%.3f<, count: >%s<".format(t01, newDenseCount))
    result

  }
  def gpi_m_times_m_old[@spec(Int) T1: ClassTag,
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
    val res: GpiAdaptiveVector[GpiAdaptiveVector[T4]] = gpi_map(
      gpi_m_times_v(f,
                    g,
                    c,
                    zero,
                    a,
                    _: GpiAdaptiveVector[T2],
                    Option(activeStats)),
      u,
      Option(activeStats)
    )
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

//    val atype = a match {
//      case _: GpiSparseVector[_] => "sparse"
//      case _: GpiDenseVector[_] => "dense"
//    }
//    val utype = u match {
//      case _: GpiSparseVector[_] => "sparse"
//      case _: GpiDenseVector[_] => "dense"
//    }
//
//    println("atype: >%s<, utype: >%s<".format(atype, utype))
    res
  }
  def gpi_equiv[T: ClassTag](u: GpiAdaptiveVector[T], v: GpiAdaptiveVector[T]): Boolean = {
    GpiAdaptiveVector.gpi_equiv(u, v)
  }
  // scalastyle:on parameter.number
}

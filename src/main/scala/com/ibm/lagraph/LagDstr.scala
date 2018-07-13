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
package com.ibm.lagraph

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
// import collection.immutable._
import scala.collection.mutable.{Map => MMap, ArrayBuffer}
import com.ibm.lagraph.impl.{
  GpiBlockMatrixPartitioner,
  GpiDstr,
  GpiBvec,
  GpiBmat,
  GpiAdaptiveVector,
  GpiOps,
  GpiDstrMatrixBlocker,
  GpiDstrVectorBlocker,
  GpiDstrBmat,
  GpiDstrBvec,
  GpiSparseRowMatrix,
  LagDstrVector,
  LagDstrMatrix
}

/**
  * The Distributed LAG context
  *
  *  The context is used for creating, importing, exporting, and manipulating
  *  matrices and vectors in a distributed (i.e., Spark) environment.
  *
  *  The distributed LAG context contains the requested blocking for vectors and matrices.
  *
  *  A LAG context may be used for any number of matrices and vectors.
  *
  *  Matrices and vectors in one context cannot be mixed with matrices and vectors
  *  in a different context.
  */
final case class LagDstrContext(@transient sc: SparkContext,
                                nblockRequested: Int,
                                DEBUG: Boolean = false)
    extends LagContext {
  val dstr = GpiDstr(sc, nblockRequested)

  /**
    * Create a matrix from an RDD.
    *
    *  @tparam T the type of the elements in the matrix
    *
    *  @param rcvRdd RDD[(row,col),value[T]] representation of a matrix.
    *  @param sparseValue sparse value for non-specified (row, col) pairs.
    *  @return a matrix created from the input map
    */
  def mFromRcvRdd[T: ClassTag](size: (Long, Long),
                               rcvRdd: RDD[((Long, Long), T)],
                               sparseValue: T,
                               nblockRequested: Option[Int] = None): LagMatrix[T] = {
    require(size._1 > 0 && size._2 > 0,
        "matrix size: >(%s, %s)< must be > (0, 0)".format(size._1, size._2))
    val (dstrRdd, nBlocksLoaded, times) =
      dstr.dstrBmatAdaptiveFromRcvRdd(sc, size, rcvRdd, sparseValue)
    LagDstrMatrix(this, rcvRdd, dstrRdd)
  }

  // *****
  // import / export
  // these are require specialized contexts may offer additional methods
  override def vFromMap[T: ClassTag](nrow: Long, m: Map[Long, T], sparseValue: T): LagVector[T] = {
    require(nrow > 0, "vector nrow: >%s< must be > 0".format(nrow))
    LagDstrVector(this, dstr.dstrBvecFromMap(sc, nrow, m.map {
      case (r, v) => (r.toLong, v)
    }, sparseValue))
  }
  override def vFromSeq[T: ClassTag]( // TODO not recommended for large scale
                                     v: Seq[T],
                                     sparseValue: T): LagVector[T] = {
    LagDstrVector(this,
                  dstr.dstrBvecFromMap(sc,
                                       v.size.toLong,
                                       v.zipWithIndex.map {
                                         case (v, r) => (r.toLong, v)
                                       }.toMap, // TODO LONG ISSUE
                                       sparseValue))
  }
  private[lagraph] override def vToMap[T: ClassTag](v: LagVector[T]): (Map[Long, T], T) = v match {
    case va: LagDstrVector[T] =>
      //      (dstr.dstrBvecToRvRdd(va.dstrBvec).collect().map
      //        { case (k, v) => (k, v) }(collection.breakOut): Map[Long, T], // TODO LONG ISSUE
      (dstr.dstrBvecToRvRdd(va.dstrBvec).collect().toMap, va.dstrBvec.sparseValue)
    //      (dstr.dstrBvecToRvRdd(va.dstrBvec).collectAsMap.toMap.map
    //        { case (k, v) => (k.toInt, v) }, // TODO LONG ISSUE
    //      va.dstrBvec.sparseValue)
  }
  private[lagraph] override def vToVector[T: ClassTag](v: LagVector[T]): Vector[T] = v match {
    case va: LagDstrVector[T] => { // TODO drop?
      val cl = ArrayBuffer.fill[T](v.size.toInt)(va.dstrBvec.sparseValue)
      //      dstr.dstrBvecToRvRdd(va.dstrBvec).collectAsMap.toMap.map
      //        { case (k, v) => cl(k.toInt) = v }
      dstr.dstrBvecToRvRdd(va.dstrBvec).collect().map {
        case (k, v) => cl(k.toInt) = v
      } // TODO LONG ISSUE
      //      dstr.dstrBvecToRvRdd(va.dstrBvec).collectAsMap.toMap.map
      //        { case (k, v) => cl(k.toInt) = v }
      cl.toVector
    }
  }

  /**
    * Convert a distributed vector to an RDD.
    *
    * Only relevant to a LagDstrContext
    *
    *  @tparam T the type of the elements in the vector
    *
    *  @param v input vector.
    *  @param sparseValue a value to be considered sparse
    *  @return a Tuple2: (RDD[row[Long], value[T]] representation of the matrix, sparse value)
    */
  def vToRvRdd[T: ClassTag](v: LagVector[T]): (RDD[(Long, T)], T) = v match {
    case va: LagDstrVector[T] => { // TODO drop?
      val vecRdd = va.dstrBvec.vecRdd
      (dstr.dstrBvecToRvRdd(va.dstrBvec), va.dstrBvec.sparseValue)
    }
  }

  override def mFromMap[T: ClassTag](size: (Long, Long),
                                     mMap: Map[(Long, Long), T],
                                     sparseValue: T): LagMatrix[T] = {
    require(size._1 > 0 && size._2 > 0,
        "matrix size: >(%s, %s)< must be > (0, 0)".format(size._1, size._2))
    val (nrow, ncol) = size
    val rcv = mMap.view.map { case (k, v) => ((k._1, k._2), v) }.toList
    val blocker = GpiDstrMatrixBlocker(size, dstr.nblockRequested)
    val rcvRdd = sc.parallelize(rcv, blocker.partitions)
    val (dstrRdd, nBlocksLoaded, times) =
      dstr.dstrBmatAdaptiveFromRcvRdd(sc, size, rcvRdd, sparseValue)
    LagDstrMatrix(this, rcvRdd, dstrRdd)
  }

  private[lagraph] override def mToMap[T: ClassTag](m: LagMatrix[T]): (Map[(Long, Long), T], T) =
    m match {
      case ma: LagDstrMatrix[T] => {
        val rstridel = ma.dstrBmat.blocker.rStride.toLong
        val cstridel = ma.dstrBmat.blocker.cStride.toLong
        val cl = Seq.newBuilder[((Long, Long), T)]
        ma.dstrBmat.matRdd.collect.map {
          case (k, v) => {
            //        cl.sizeHint(aa.size) TODO
            val ar = k._1
            val ac = k._2
            v.loadMapBuilder(cl, ar.toLong * rstridel, ac.toLong * cstridel)
            //          v match {
            //            case avga: GpiBmatAdaptive[T] => {
            //              val itr = avga.a.denseIterator
            //              while (itr.hasNext) {
            //                val (ir, rv) = itr.next()
            //                val itc = rv.denseIterator
            //                while (itc.hasNext) {
            //                  val (ic, v) = itc.next()
            //                  cl += Tuple2(((ar * dstr.stride + ir).toInt,
            //                        (ac * dstr.stride + ic).toInt), v) // LONG ISSUE
            //                }
            //              }
            //            }
            //          }
          }
        }
        (cl.result.toMap, ma.dstrBmat.sparseValue)
      }
    }

  private[lagraph] def mToRcvRdd[T](m: LagMatrix[T]): (RDD[((Long, Long), T)], T) = m match {
    case ma: LagDstrMatrix[T] => {
      def f(input: ((Int, Int), GpiBmat[T])): List[((Long, Long), T)] = {
        val rblockl = input._1._1.toLong
        val cblockl = input._1._2.toLong
        val rstridel = ma.dstrBmat.blocker.rStride.toLong
        val cstridel = ma.dstrBmat.blocker.cStride.toLong
        val bmata = input._2
        //        val bmata = bmat
        //        bmat match {
        //          case bmata: GpiBmatAdaptive[T] => {
        //            val stridel = dstr.stride.toLong
        bmata.a.denseIterator.toList.flatMap {
          case (lr, r) => {
            r.denseIterator.toList.map {
              case (lc, v) => {
                val gr = (rblockl * rstridel) + lr.toLong
                val gc = (cblockl * cstridel) + lc.toLong
                ((gr, gc), v)
              }
            }
          }
        }
        //          }
        //        }
      }
      (ma.dstrBmat.matRdd.flatMap(f), ma.dstrBmat.sparseValue)
    }
  }
  // ****************
  // ****************
  // ****************
  // ****************
  // ****************
  // factory
//  override def vIndices(start: Long = 0L,
// sparseValueOpt: Option[Long] = None): LagVector[Long] = {
  override def vIndices(nrow: Long,
                        start: Long = 0L, sparseValueOpt: Option[Long] = None): LagVector[Long] = {
    require(nrow > 0, "vector nrow: >%s< must be > 0".format(nrow))
    //    val end = start + graphSize
    LagDstrVector(this, dstr.dstr_indices(sc, nrow, start, sparseValueOpt))
  }
  override def vReplicate[T: ClassTag](nrow: Long,
                                       x: T, sparseValueOpt: Option[T] = None): LagVector[T] = {
    require(nrow > 0, "vector nrow: >%s< must be > 0".format(nrow))
    //    LagDstrVector(dstr.dstr_replicate(sc, graphSize, x))
    LagDstrVector(this, dstr.dstr_replicate(sc, nrow, x, sparseValueOpt))
  }
  // experimental
  override def mIndices(size: (Long, Long), start: (Long, Long)): LagMatrix[(Long, Long)] = {
    require(size._1 > 0 && size._2 > 0,
        "matrix size: >(%s, %s)< must be > (0, 0)".format(size._1, size._2))
    require(start._1 > 0)
    val (nrow, ncol) = size
    val end = (start._1 + nrow, start._2 + ncol)
    mFromMap[(Long, Long)](size, Map((start._1 until end._1).map { r =>
      (start._2 until end._2).map { c =>
        ((r - start._1, c - start._1), (r, c))
      }
    }.flatten: _*), (0, 0))
  }
  // experimental
  override def mReplicate[T: ClassTag](size: (Long, Long), x: T): LagMatrix[T] = {
    require(size._1 > 0 && size._2 > 0,
        "matrix size: >(%s, %s)< must be > (0, 0)".format(size._1, size._2))
    val (nrow, ncol) = size
    mFromMap[T](size, Map((0L until size._1).map { r =>
      (0L until size._2).map { c =>
        ((r, c), x)
      }
    }.flatten: _*), x)
  }
  // experimental
  override def vToMrow[T: ClassTag](
      m: LagMatrix[T], maskV: LagVector[Boolean], u: LagVector[T]): LagMatrix[T] =
    (m, maskV, u) match {
      case (ldm: LagDstrMatrix[T], ldk: LagDstrVector[Boolean], ldu: LagDstrVector[T]) => {
        require(
          ldm.dstrBmat.sparseValue == ldu.dstrBvec.sparseValue,
          "vToMrow does not currently support disparate sparsities:" +
          " matrix sparseValue: >%s<, vector sparseValue: >%s<"
            .format(ldm.dstrBmat.sparseValue, ldu.dstrBvec.sparseValue)
        )
        val mSparse = ldu.dstrBvec.sparseValue
        val A = ldm.dstrBmat
        val k = ldk.dstrBvec
        val u = ldu.dstrBvec
        require(A.blocker.nblock == u.blocker.nblock,
            "current design precludes disparate blocks: " +
            "M.block: >%s< != u.block: >%s<, " +
            "try reducing nblock to: >%s<".
            format(A.blocker.nblock, u.blocker.nblock,
                scala.math.min(A.blocker.nblock, u.blocker.nblock)))
        require(A.blocker.nblock == k.blocker.nblock,
            "current design precludes disparate blocks: " +
            "M.block: >%s< != k.block: >%s<, " +
            "try reducing nblock to: >%s<".
            format(A.blocker.nblock, k.blocker.nblock,
                scala.math.min(A.blocker.nblock, k.blocker.nblock)))
        val nblock = A.blocker.nblock
        // ********
        // distribution functor for k
        def kDistributeVector(
            kv: ((Int, Int), GpiBvec[Boolean])): List[((Int, Int), (Int, GpiBvec[Boolean]))] = {
          val empty = List[((Int, Int), (Int, GpiBvec[Boolean]))]()
          val (r, c) = kv._1
          val gxa = kv._2.asInstanceOf[GpiBvec[Boolean]]
          def recurse(rdls: List[((Int, Int), (Int, GpiBvec[Boolean]))],
                      targetBlock: Int): List[((Int, Int), (Int, GpiBvec[Boolean]))] =
            if (targetBlock == k.blocker.clipN + 1) rdls
            else {
              recurse(((r, targetBlock), (c, gxa)) :: rdls, targetBlock + 1)
            }
          recurse(empty, 0)
        }
        val ksrb = k.vecRdd.flatMap(kDistributeVector)
        // ********
        // distribution functor for u
        def distributeVector(
            kv: ((Int, Int), GpiBvec[T])): List[((Int, Int), (Int, GpiBvec[T]))] = {
          val empty = List[((Int, Int), (Int, GpiBvec[T]))]()
          val (r, c) = kv._1
          val gxa = kv._2.asInstanceOf[GpiBvec[T]]
          def recurse(rdls: List[((Int, Int), (Int, GpiBvec[T]))],
                      targetBlock: Int): List[((Int, Int), (Int, GpiBvec[T]))] =
            if (targetBlock == u.blocker.clipN + 1) rdls
            else {
              recurse(((targetBlock, r), (c, gxa)) :: rdls, targetBlock + 1)
            }
          recurse(empty, 0)
        }
        val srb = u.vecRdd.flatMap(distributeVector)
        // ********
        // calculation functor
        def calculate(kv: ((Int, Int), (
            Iterable[GpiBmat[T]],
            Iterable[(Int, GpiBvec[Boolean])],
            Iterable[(Int, GpiBvec[T])]))): ((Int, Int), GpiBmat[T]) = {
          val r = kv._1._1
          val c = kv._1._2
          // matrix
          val cAa = kv._2._1.iterator.next().a
          // mask vector
          val kstride = k.blocker.getVectorStride(r, c)
          val (kvclipn, kvstride, kvclipstride) = k.blocker.getVectorStrides(r)
          val ksortedU = Array.fill[GpiAdaptiveVector[Boolean]](kvclipn + 1)(null)
          val kit = kv._2._2.iterator
          while (kit.hasNext) {
            val (k, v) = kit.next()
            ksortedU(k) = v.u
          }
          val kxxx = GpiAdaptiveVector.concatenateToDense(ksortedU)
          // value vector
          val stride = u.blocker.getVectorStride(r, c)
          val (vclipn, vstride, vclipstride) = u.blocker.getVectorStrides(c)
          val sortedU = Array.fill[GpiAdaptiveVector[T]](vclipn + 1)(null)
          val it = kv._2._3.iterator
          while (it.hasNext) {
            val (k, v) = it.next()
            sortedU(k) = v.u
          }
          val xxx = GpiAdaptiveVector.concatenateToSparse(sortedU)
          // perform assignment
          val rStride = if (r == A.blocker.clipN) A.blocker.rClipStride else A.blocker.rStride
          val cStride = if (c == A.blocker.clipN) A.blocker.cClipStride else A.blocker.cStride
          val vSparse = GpiAdaptiveVector.fillWithSparse[T](cStride)(mSparse)
          val rBuffer = ArrayBuffer.fill(rStride)(vSparse)
          def assign(i: Int,
              rb: ArrayBuffer[GpiAdaptiveVector[T]]): ArrayBuffer[GpiAdaptiveVector[T]] = {
            if (i == cAa.size) rb else {
              if (kxxx(i)) rb(i) = xxx else rb(i) = cAa(i)
              assign(i + 1, rb)
            }
          }
          assign(0, rBuffer)
          val cAb = GpiAdaptiveVector.fromSeq(rBuffer, vSparse)
          ((r, c), GpiBmat(cAb))
        }
        val newMatRdd = A.matRdd.cogroup(ksrb, srb).map(calculate)
        // ********
        // TODO replace w/ bmatToRcvRdd ?
        def toRcv(kv: ((Int, Int), GpiBmat[T])) = {
          val a = kv._2.a
          val arOff = kv._1._1.toLong * A.blocker.rStride.toLong
          val acOff = kv._1._2.toLong * A.blocker.cStride.toLong
          val ac = kv._1._2
          var res = List[((Long, Long), T)]()
          val itr = a.denseIterator
          while (itr.hasNext) {
            val (ir, rv) = itr.next()
            val itc = rv.denseIterator
            while (itc.hasNext) {
              val (ic, v) = itc.next()
              res = ((arOff + ir.toLong, acOff + ic.toLong), v) :: res
            }
          }
          res
        }
        val newRcvRdd = newMatRdd.flatMap(toRcv)
        val B = GpiDstrBmat(A.dstr,
                            A.blocker,
                            newMatRdd,
                            A.nrow,
                            A.ncol,
                            A.sparseValue)
        LagDstrMatrix(ldm.hc, newRcvRdd, B)
      }
  }

  // *****
  // vector function
  private[lagraph] override def vReduce[T1: ClassTag, T2: ClassTag](f: (T1, T2) => T2,
                                                                    c: (T2, T2) => T2,
                                                                    z: T2,
                                                                    u: LagVector[T1]): T2 =
    u match {
      case ua: LagDstrVector[T1] =>
        dstr.dstr_reduce(f, c, z, ua.dstrBvec)
    }
  private[lagraph] override def vMap[T1: ClassTag, T2: ClassTag](f: (T1) => T2,
                                                                 u: LagVector[T1]): LagVector[T2] =
    u match {
      case ua: LagDstrVector[T1] =>
        LagDstrVector(this, dstr.dstr_map(f, ua.dstrBvec))
    }
  private[lagraph] override def vZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      u: LagVector[T1],
      v: LagVector[T2]): LagVector[T3] = (u, v) match {
    case (ua: LagDstrVector[T1], va: LagDstrVector[T2]) =>
      LagDstrVector(this, dstr.dstr_zip(f, ua.dstrBvec, va.dstrBvec))
  }
  private[lagraph] override def vZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, Long) => T2,
      u: LagVector[T1],
      sparseValueOpt: Option[T2] = None): LagVector[T2] = u match {
    case ua: LagDstrVector[_] =>
      // TODO use buildin
      LagDstrVector(this, dstr.dstr_zip(f, ua.dstrBvec,
                    dstr.dstr_indices(sc, ua.size, 0L)))
    //      LagDstrVector(dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0, u.size)))
  }
  // TODO needs to be optimized
  private[lagraph] override def vReduceWithIndex[T1: ClassTag, T2: ClassTag](
      f: ((T1, Long), T2) => T2,
      c: (T2, T2) => T2,
      z: T2,
      u: LagVector[T1]): T2 = u match {
    case ua: LagDstrVector[_] => {
      def f2(a: T1, b: Long): (T1, Long) = { (a, b) }
      //      val xxx = dstr.dstr_indices(sc, 0L)
      //      xxx.vecRdd.collect().foreach { case (k, v) =>
      //        println("INDS: (%s,%s): %s".format(k._1, k._2, v)) }
      val uinds = dstr.dstr_zip(f2, ua.dstrBvec, dstr.dstr_indices(sc, ua.size, 0L))
      //      uinds.vecRdd.collect().foreach { case (k, v) =>
      //        println("UINDS: (%s,%s): %s".format(k._1, k._2, v)) }
      dstr.dstr_reduce(f, c, z, uinds)
      //      // TODO use buildin
      //      LagDstrVector(this, dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0L)))
      //      LagDstrVector(dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0, u.size)))
    }
    //    case ua: LagSmpVector[T1] => {
    //      def f2(a:T1,b:Long):(T1, Long) = {(a,b)}
    //      val uinds = GpiOps.gpi_zip(f2, ua.v, GpiOps.gpi_indices(u.size, 0))
    //      GpiOps.gpi_reduce(f, z, uinds)
    //    }
  }
  // *****
  private[lagraph] def bmatToRcvRdd[T](
      blocker: GpiDstrMatrixBlocker,
      matRdd: RDD[((Int, Int), GpiBmat[T])]): RDD[((Long, Long), T)] = {
//        // *****
//        val rstridel = blocker.rStride.toLong
//        val cstridel = blocker.cStride.toLong
//        def perblock(input: ((Int, Int), GpiBmat[T])): List[((Long, Long), T)] = {
//          val rblockl = input._1._1.toLong
//          val cblockl = input._1._2.toLong
//          val mata = input._2
//          //          val mata = mato
//          //          mato match {
//          //            case mata: GpiBmatAdaptive[T3] => {
//          if (mata.a.denseCount == 0) {
//            List()
//          } else {
//            val xxx = mata.a.denseIterator.toList.map {
//              case (ri: Int, r) => {
//                r.denseIterator.toList.map {
//                  case (ci: Int, v) => {
//                    ((rblockl * rstridel + ri.toLong, cblockl * cstridel + ci.toLong), v)
//                  }
//                }
//              }
//            }
//            xxx.reduceLeft(_ ::: _)
//          }
//          //            }
//          //          }
//        }
//        matRdd.flatMap(perblock)
//      }
        // ********
    def toRcv(kv: ((Int, Int), GpiBmat[T])) = {
      val a = kv._2.a
      val arOff = kv._1._1.toLong * blocker.rStride.toLong
      val acOff = kv._1._2.toLong * blocker.cStride.toLong
      val ac = kv._1._2
      var res = List[((Long, Long), T)]()
      val itr = a.denseIterator
      while (itr.hasNext) {
        val (ir, rv) = itr.next()
        val itc = rv.denseIterator
        while (itc.hasNext) {
          val (ic, v) = itc.next()
          res = ((arOff + ir.toLong, acOff + ic.toLong), v) :: res
        }
      }
      res
    }
    matRdd.flatMap(toRcv)
  }

  // *****
  private[lagraph] override def mMap[T1: ClassTag, T2: ClassTag](
      f: (T1) => T2,
      m: LagMatrix[T1]): LagMatrix[T2] = (m) match {
    case (ma: LagDstrMatrix[T1]) => {
      val sparseValue = f(ma.dstrBmat.sparseValue)
      val dstrBmat = {
        def perblock(bmat: GpiBmat[T1]): GpiBmat[T2] = {
          def perrow(mv: GpiAdaptiveVector[T1]): GpiAdaptiveVector[T2] = {
            GpiOps.gpi_map(f, mv)
          }
          val vovMap = GpiOps.gpi_map(perrow, bmat.a)
          GpiBmat(vovMap)
        }
        val matRdd = ma.dstrBmat.matRdd.mapValues {
          mb => perblock(mb)
        }
        GpiDstrBmat(ma.dstrBmat.dstr, ma.dstrBmat.blocker,
                    matRdd, ma.dstrBmat.nrow, ma.dstrBmat.ncol, sparseValue)
      }
      val rcvRdd = bmatToRcvRdd(ma.dstrBmat.blocker, dstrBmat.matRdd)
      LagDstrMatrix(m.hc, rcvRdd, dstrBmat)

    }
  }
  private[lagraph] override def mZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      m: LagMatrix[T1],
      n: LagMatrix[T2]): LagMatrix[T3] = (m, n) match {
    case (ma: LagDstrMatrix[T1], na: LagDstrMatrix[T2]) => {
      require(ma.dstrBmat.blocker == na.dstrBmat.blocker)
      val sparseValue = f(ma.dstrBmat.sparseValue, na.dstrBmat.sparseValue)
      val dstrBmat = {
        def perblock(bmats: (GpiBmat[T1], GpiBmat[T2])): GpiBmat[T3] = {
          val (bmatma, bmatna) = bmats
          //          val maBmat = bmats._1
          //          val naBmat = bmats._2
          //          val (bmatma, bmatna) = (maBmat, naBmat)
          //          (maBmat, naBmat) match {
          //            case (bmatma: GpiBmatAdaptive[T1], bmatna: GpiBmatAdaptive[T2]) => {
          def perrow(mv: GpiAdaptiveVector[T1],
                     nv: GpiAdaptiveVector[T2]): GpiAdaptiveVector[T3] = {
            GpiOps.gpi_zip(f, mv, nv)
          }
          val vovZip = GpiOps.gpi_zip(perrow, bmatma.a, bmatna.a)
          //              GpiBmatAdaptive(vovZip)
          GpiBmat(vovZip)
          //            }
          //          }
        }
        val matRdd = ma.dstrBmat.matRdd.join(na.dstrBmat.matRdd).mapValues {
          case (mb, nb) => perblock(mb, nb)
        }
        GpiDstrBmat(ma.dstrBmat.dstr, ma.dstrBmat.blocker,
                    matRdd, ma.dstrBmat.nrow, ma.dstrBmat.ncol, sparseValue)
      }
      val rcvRdd = bmatToRcvRdd(ma.dstrBmat.blocker, dstrBmat.matRdd)
      LagDstrMatrix(m.hc, rcvRdd, dstrBmat)

    }
  }
  private[lagraph] override def mSparseZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, (Long, Long)) => T2,
      m: LagMatrix[T1],
      targetSparseValue: T2,
      visitDiagonalsOpt: Option[T1] = None): LagMatrix[T2] = m match {
    case ma: LagDstrMatrix[T1] => {
      val rcvRdd = if (visitDiagonalsOpt.isDefined) {
        val diagRdd =
          sc.parallelize((0L until m.size._1), ma.dstrBmat.blocker.partitions).map { d =>
            ((d, d), visitDiagonalsOpt.get)
          }
        ma.rcvRdd.flatMap {
          case (k, v) => if (k._1 != k._2) Some((k, v)) else None
        } ++ diagRdd
      } else ma.rcvRdd
      mFromRcvRdd(ma.size,
                  rcvRdd.mapPartitions({ iter: Iterator[((Long, Long), T1)] =>
        for (i <- iter) yield (i._1, f(i._2, i._1))
      }, preservesPartitioning = false), targetSparseValue)
    }
  }
  private[lagraph] override def mZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, (Long, Long)) => T2,
      m: LagMatrix[T1]): LagMatrix[T2] = m match {
    case ma: LagDstrMatrix[T1] => {
      val msSparse = f(ma.dstrBmat.sparseValue, (0, 0))

      def perblock(
          index: Int,
          iter: Iterator[((Int, Int), GpiBmat[T1])]): Iterator[((Int, Int), GpiBmat[T2])] = {
        val (k, v) = iter.next()
        //        require (k == dstr.matrixPartitionIndexToKey(index))
        val (r, c) = k
        val rBase =
          if (r >= ma.dstrBmat.blocker.clipN)
            {ma.dstrBmat.blocker.clipN.toLong * ma.dstrBmat.blocker.rStride.toLong}
          else
            {r.toLong * ma.dstrBmat.blocker.rStride.toLong}
        val cBase =
          if (c >= ma.dstrBmat.blocker.clipN)
            {ma.dstrBmat.blocker.clipN.toLong * ma.dstrBmat.blocker.cStride.toLong}
          else
            {c.toLong * ma.dstrBmat.blocker.cStride.toLong}
        def perrow(mv: GpiAdaptiveVector[T1], rlocal: Long): GpiAdaptiveVector[T2] = {
          GpiOps.gpi_zip_with_index_matrix(f,
                                           mv,
                                           rlocal,
                                           cBase,
                                           sparseValueT3Opt = Option(msSparse))
        }
        val vSparse = if (c == ma.dstrBmat.blocker.clipN) {
          GpiAdaptiveVector.fillWithSparse[T2](
              ma.dstrBmat.blocker.cClipStride)(msSparse)
        } else {
          GpiAdaptiveVector.fillWithSparse[T2](
              ma.dstrBmat.blocker.cStride)(msSparse)
        }
        val vovZip = GpiOps.gpi_zip_with_index_vector(perrow,
                                                      v.a,
                                                      base = rBase,
                                                      sparseValueT3Opt = Option(vSparse))
        List(((r, c), GpiBmat(vovZip))).toIterator
      }
      val matRdd = ma.dstrBmat.matRdd.mapPartitionsWithIndex(perblock, true)

      val dstrBmat =
        GpiDstrBmat(ma.dstrBmat.dstr, ma.dstrBmat.blocker,
            matRdd, ma.dstrBmat.ncol, ma.dstrBmat.nrow, msSparse)
      val rcvRdd = dstr.dstrBmatAdaptiveToRcvRdd(sc, dstrBmat)
      LagDstrMatrix(m.hc, rcvRdd, dstrBmat)
    }
  }
  // ********
  private[lagraph] def mTv[T: ClassTag](sr: LagSemiring[T],
                                        lm: LagMatrix[T],
                                        lv: LagVector[T]): LagVector[T] =
    (lm, lv) match {
      case (ldm: LagDstrMatrix[T], ldu: LagDstrVector[T]) => {
        require(lm.size._2 == lv.size,
            "mTv: size mismatch: M: >(%s,%s), v: >%s<".format(
                lm.size._1, lm.size._2, lv.size))
        require(
          ldm.dstrBmat.sparseValue == ldu.dstrBvec.sparseValue,
          "mTv does not currently support disparate sparsities:" +
          " matrix sparseValue: >%s<, vector sparseValue: >%s<"
            .format(ldm.dstrBmat.sparseValue, ldu.dstrBvec.sparseValue)
        )
        val vsSparse = ldu.dstrBvec.sparseValue
        val A = ldm.dstrBmat
        val u = ldu.dstrBvec
        require(A.blocker.nblock == u.blocker.nblock,
            "current design precludes disparate blocks: " +
            "M.block: >%s< != u.block: >%s<, " +
            "try reducing nblock to: >%s<".
            format(A.blocker.nblock, u.blocker.nblock,
                scala.math.min(A.blocker.nblock, u.blocker.nblock)))
        val nblock = A.blocker.nblock
        // ********
        // distribution functor
        def distributeVector(
            kv: ((Int, Int), GpiBvec[T])): List[((Int, Int), (Int, GpiBvec[T]))] = {
          val empty = List[((Int, Int), (Int, GpiBvec[T]))]()
          val (r, c) = kv._1
          val gxa = kv._2.asInstanceOf[GpiBvec[T]]
          def recurse(rdls: List[((Int, Int), (Int, GpiBvec[T]))],
                      targetBlock: Int): List[((Int, Int), (Int, GpiBvec[T]))] =
            if (targetBlock == u.blocker.clipN + 1) rdls
            else {
              // ****
              recurse(((targetBlock, r), (c, gxa)) :: rdls, targetBlock + 1)
            }
          recurse(empty, 0)
        }
        //      u.vecRdd.collect().foreach { case (k, v) =>
        //        println("u.vecRdd: (%s,%s): %s".format(k._1, k._2, v.u.toVector)) }
        val srb = u.vecRdd.flatMap(distributeVector)
        //      srb.collect().foreach { case (k, v) =>
        //        println("srb: (%s,%s): >%s< %s".format(k._1, k._2, v._1, v._2.u.toVector)) }
        // ********
        // calculation functor
        def calculate(kv: ((Int, Int), (Iterable[GpiBmat[T]], Iterable[(Int, GpiBvec[T])]))) = {
          val r = kv._1._1
          val c = kv._1._2
          val stride = u.blocker.getVectorStride(r, c)
          val (vclipn, vstride, vclipstride) = u.blocker.getVectorStrides(c)
          //        for debug only ... note .next() changes the state of the iterator
          //        val cPartial = kv._2._1.iterator.next()
          //        val cPartial = GpiAdaptiveVector.fillWithSparse[T](stride)(sr.zero)
          //        val cAa = kv._2._1.iterator.next().asInstanceOf[GpiBmatAdaptive[T]].a
          val cAa = kv._2._1.iterator.next().a
          val sortedU = Array.fill[GpiAdaptiveVector[T]](vclipn + 1)(null)
          //        println("Z for (%s, %s)".format(r, c))
          val it = kv._2._2.iterator
          while (it.hasNext) {
            val (k, v) = it.next()
            //          println("ZZZZ", k, v.u.toVector)
            sortedU(k) = v.u
          }
          //        println("ZEN")
          //        println(sortedU.foreach(println))
          val xxx = GpiAdaptiveVector.concatenateToDense(sortedU)
          //        println("SORTEDU: >%s<".format(xxx.toVector))
          val blockResult = GpiOps.gpi_m_times_v(sr.addition,
                                                 sr.multiplication,
                                                 sr.addition,
                                                 sr.zero,
                                                 cAa,
                                                 xxx)

          ((r, c), blockResult)
        }
        //      A.matRdd.collect().foreach { case (k, v) => println(
        //        "A.matRdd: (%s,%s): %s".format(k._1, k._2,
        //          GpiSparseRowMatrix.toString(v.asInstanceOf[GpiBmatAdaptive[T]].a))) }
        //      val xxx = A.matRdd.cogroup(srb)
        //      def debugXxx(
        //        kv: ((Int, Int), (Iterable[GpiBmat[T]], Iterable[(Int, GpiBvec[T])]))) = {
        //        println("XXXXX: %s".format(kv._1))
        //        val it1 = kv._2._1.iterator
        //        while (it1.hasNext) {
        //          val v = it1.next()
        //          println("  BMAT: %s".format(
        //            GpiSparseRowMatrix.toString(v.asInstanceOf[GpiBmatAdaptive[T]].a)))
        //        }
        //        val it2 = kv._2._2.iterator
        //        while (it2.hasNext) {
        //          val (k, v) = it2.next()
        //          println("  BVEC: >%s< %s".format(k, v.u.toVector))
        //        }
        //      }
        //      xxx.map(debugXxx).count()
        //      val partial1 = initBlockResult.cogroup(A.matRdd, srb).map(calculate)
        val partial1 = A.matRdd.cogroup(srb).map(calculate)
        //      partial1.collect().foreach { case (k, v) =>
        //        println("partial1: (%s,%s): %s".format(k._1, k._2, v.toVector)) }
        // ****
        // Blocker for resultant vector
        val vblocker = GpiDstrVectorBlocker(A.blocker.nrow, nblock)
        // ****
        // functor to chop and send
        def chopResults(
            kv: ((Int, Int), GpiAdaptiveVector[T])): List[((Int, Int), GpiAdaptiveVector[T])] = {
          val (r, c) = kv._1
          val vd = GpiAdaptiveVector.toDense(kv._2)

          require(vd.isInstanceOf[com.ibm.lagraph.impl.GpiDenseVector[T]],
                  "must be dense for this approach to work")
          val array =
            vd.asInstanceOf[com.ibm.lagraph.impl.GpiDenseVector[T]].iseq.elems

          //        val curVectorStride = dstr.getVectorStride(r, c)
          //        def chop(n: Int, pos: Int, choplist: List[((Int, Int), GpiAdaptiveVector[T])]):
          //          List[((Int, Int), GpiAdaptiveVector[T])] = if (n > dstr.clipN) choplist else {
          def chop(n: Int, pos: Int, choplist: List[((Int, Int), GpiAdaptiveVector[T])])
            : List[((Int, Int), GpiAdaptiveVector[T])] =
            if (pos >= array.size) choplist
            else {
              val curVectorStride = vblocker.getVectorStride(r, n)
              val nextPos = pos + curVectorStride
              val newArray = array.slice(pos, nextPos)
              //          println("chop: (r,c): (%s,%s) -> (r,n):
              //            (%s,%s) pos: %s array.size: >%s< newArray.size: >%s<".format(
              //            r, c, r,n, pos, array.size, newArray.size))
              chop(n + 1,
                   nextPos,
                   ((r, n),
                    GpiAdaptiveVector
                      .wrapDenseArray(newArray, vsSparse)) :: choplist)
            }
          chop(0, 0, List[((Int, Int), GpiAdaptiveVector[T])]())
        }
        val choppedResults = partial1
          .flatMap(chopResults)
          .partitionBy(
            new com.ibm.lagraph.impl.GpiBlockVectorPartitioner(vblocker.clipN + 1,
                                                               vblocker.vecClipN + 1,
                                                               vblocker.remClipN + 1))
        //      choppedResults.collect().foreach { case (k, v) =>
        //        println("choppedResults: (%s,%s): %s".format(k._1, k._2, v.toVector)) }
        // ****
        // functor to combine results
        def zipChoppedResults(partition: Int, iter: Iterator[((Int, Int), GpiAdaptiveVector[T])])
          : Iterator[((Int, Int), GpiAdaptiveVector[T])] = {
          def f(kv1: ((Int, Int), GpiAdaptiveVector[T]),
                kv2: ((Int, Int), GpiAdaptiveVector[T])) = {
            val result =
              GpiOps.gpi_zip(sr.addition, kv1._2, kv2._2)
            (kv1._1, result)
          }
          //        println("XXXX: PART:",partition)
          //        val xxx = List(iter.reduce(f)) // .toIterator
          //        println("XXXX: PART: >%s<, LEN: >%s<".format(partition, xxx(0)._2.size))
          //        xxx.toIterator
          List(iter.reduce(f)).toIterator
        }
        val result = choppedResults
          .mapPartitionsWithIndex(zipChoppedResults, preservesPartitioning = true)
          .map { case (k, v) => Tuple2(k, GpiBvec(v)) }
        // ****
        LagDstrVector(this, GpiDstrBvec(dstr, vblocker, result, vsSparse))
      }
    }
  private[lagraph] override def mTm[T: ClassTag](sr: LagSemiring[T],
                                                 ma: LagMatrix[T],
                                                 mb: LagMatrix[T]): LagMatrix[T] = (ma, mb) match {
    case (maa: LagDstrMatrix[T], mba: LagDstrMatrix[T]) => {
      require(maa.dstrBmat.sparseValue == mba.dstrBmat.sparseValue,
          "mTm does not currently support disparate sparsities")
      val a_blocker = maa.dstrBmat.blocker
      val b_blocker = mba.dstrBmat.blocker
      require(a_blocker.clipN == b_blocker.clipN, "a.clipN: >%s< != b.clipN: >%s<)".format(
          a_blocker.clipN, b_blocker.clipN))
      val clipN = a_blocker.clipN
      require(a_blocker.ncol == b_blocker.nrow, "(%s,%s) x (%s,%s) not supported".format(
          a_blocker.nrow, a_blocker.ncol,
          b_blocker.nrow, b_blocker.ncol))
      val msSparse = maa.dstrBmat.sparseValue
      val hcd = this
      val dstr = hcd.dstr
      val clipnp1 = clipN + 1

      // ********
      // get dstrBmat
      val A = maa.dstrBmat
      val BT = mba.transpose.asInstanceOf[LagDstrMatrix[T]].dstrBmat
      val bt_blocker = BT.blocker

      // ********
      // initialize partial result
      val partial_blocker = GpiDstrMatrixBlocker((mb.size._2, ma.size._1), clipnp1)
      def initPartial(rc: (Int, Int)) = {
        val (r, c) = rc
        val rChunk = if (r == clipN) partial_blocker.rClipStride else partial_blocker.rStride
        val cChunk = if (c == clipN) partial_blocker.cClipStride else partial_blocker.cStride
        val vSparse = GpiAdaptiveVector.fillWithSparse[T](cChunk)(sr.zero)
        ((r, c), GpiAdaptiveVector.fillWithSparse(rChunk)(vSparse))
      }
      val coordRdd = GpiDstr.getCoordRdd(sc, clipN + 1)
      val Partial = coordRdd.cartesian(coordRdd).map(initPartial)
      // ********
      // top level
      def recurse(contributingIndex: Int,
                  rPartial: RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])],
                  rA: RDD[((Int, Int), GpiBmat[T])],
                  rB: RDD[((Int, Int), GpiBmat[T])])
        : RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])] =
        if (contributingIndex == clipnp1) rPartial
        else {
          def selectA(kv: ((Int, Int), GpiBmat[T])): List[((Int, Int), GpiBmat[T])] = {
            val (r, c) = kv._1
            val gxa = kv._2.asInstanceOf[GpiBmat[T]]
            def selectIt(rd: Int,
                         dl: List[((Int, Int), GpiBmat[T])]): List[((Int, Int), GpiBmat[T])] =
              if (rd == clipnp1) dl else selectIt(rd + 1, ((rd, r), gxa) :: dl)
            val empty = List[((Int, Int), GpiBmat[T])]()
            if (c == contributingIndex) selectIt(0, empty) else empty
          }
          def selectB(kv: ((Int, Int), GpiBmat[T])): List[((Int, Int), GpiBmat[T])] = {
            val (c, r) = kv._1
            val gxa = kv._2.asInstanceOf[GpiBmat[T]]
            val empty = List[((Int, Int), GpiBmat[T])]()
            def selectIt(rd: Int,
                         dl: List[((Int, Int), GpiBmat[T])]): List[((Int, Int), GpiBmat[T])] =
              if (rd == clipnp1) dl else selectIt(rd + 1, ((c, rd), gxa) :: dl)
            if (r == contributingIndex) selectIt(0, empty) else empty
          }
          // ********
          // permutations
          val sra = rA.flatMap(selectA)
          val srb = rB.flatMap(selectB)

          // ********
          // calculation functor
          def calculate(
              kv: ((Int, Int),
                   (Iterable[GpiAdaptiveVector[GpiAdaptiveVector[T]]],
                    Iterable[GpiBmat[T]],
                    Iterable[GpiBmat[T]]))) = {
            val r = kv._1._1
            val c = kv._1._2
            val cPartial = kv._2._1.iterator.next()
            val cAa = kv._2._2.iterator.next().a
            val cBa = kv._2._3.iterator.next().a
            val iPartial = GpiOps.gpi_m_times_m(sr.addition,
                                                sr.multiplication,
                                                sr.addition,
                                                sr.zero,
                                                cAa,
                                                cBa)
//            val updatedPartial = cPartial // DEBUG
            val updatedPartial = GpiOps.gpi_zip(
              GpiOps.gpi_zip(sr.addition, _: GpiAdaptiveVector[T], _: GpiAdaptiveVector[T]),
              iPartial,
              cPartial)
            ((r, c), updatedPartial)
          }
          val nextPartialA = rPartial.cogroup(sra, srb)//.cache()
//          println("nextpartialA.count: >%s<".format(nextPartialA.count))
          val nextPartial = nextPartialA.map(calculate).cache()
          nextPartialA.unpersist(true)
          println("nextpartialB.count: >%s<".format(nextPartial.count))

          // ********
          // recurse
          recurse(contributingIndex + 1, nextPartial, rA, rB)
        }
      val transposedResult = recurse(0, Partial, A.matRdd, BT.matRdd)

      // ********
      def toRcv(kv: ((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])) = {
        val arOff = kv._1._1.toLong * partial_blocker.rStride.toLong
        val acOff = kv._1._2.toLong * partial_blocker.cStride.toLong
        val ac = kv._1._2
        var res = List[((Long, Long), T)]()
        val itr = kv._2.denseIterator
        while (itr.hasNext) {
          val (ir, rv) = itr.next()
          val itc = rv.denseIterator
          while (itc.hasNext) {
            val (ic, v) = itc.next()
            res = ((arOff + ir.toLong, acOff + ic.toLong), v) :: res
          }
        }
        res
      }
      val rcvRdd = transposedResult.flatMap(toRcv)
      hcd.mFromRcvRdd((partial_blocker.ncol, partial_blocker.nrow), rcvRdd.map {
        case (k, v) => ((k._2, k._1), v) }, msSparse)

      //  // for transpose ...
      //  val bmatRdd = transposedResult.map { case (k, v) =>
      //    (k, GpiBmatAdaptive(v).asInstanceOf[GpiBmat[T]]) }
      //  LagDstrMatrix(hcd, rcvRdd,
      //    GpiDstrBmat(dstr, bmatRdd, maa.dstrBmat.nrow, maa.dstrBmat.ncol,
      //                maa.dstrBmat.sparseValue))
    }
  }
  private[lagraph] override def mDm[T: ClassTag](sr: LagSemiring[T],
                                                 ma: LagMatrix[T],
                                                 mb: LagMatrix[T]): LagMatrix[T] = (ma, mb) match {
    case (maa: LagDstrMatrix[T], mba: LagDstrMatrix[T]) => {
      require(maa.dstrBmat.sparseValue == mba.dstrBmat.sparseValue,
          "mDm does not currently support disparate sparsities")
      val a_blocker = maa.dstrBmat.blocker
      val b_blocker = mba.dstrBmat.blocker
      require(a_blocker.clipN == b_blocker.clipN, "a.clipN: >%s< != b.clipN: >%s<)".format(
          a_blocker.clipN, b_blocker.clipN))
      val clipN = a_blocker.clipN
      require(a_blocker.ncol == b_blocker.ncol, "(%s,%s) d (%s,%s) not supported".format(
          a_blocker.nrow, a_blocker.ncol,
          b_blocker.nrow, b_blocker.ncol))
      val msSparse = maa.dstrBmat.sparseValue
      val hcd = this
      val dstr = hcd.dstr
      val clipnp1 = clipN + 1

      // ********
      // get dstrBmat
      val A = maa.dstrBmat
      val BT = mba.dstrBmat
      val bt_blocker = b_blocker

      // ********
      // initialize partial result
      val partial_blocker = GpiDstrMatrixBlocker((mb.size._1, ma.size._1), clipnp1)
      val pbPartitioner = new GpiBlockMatrixPartitioner(partial_blocker.clipN + 1)
      def initPartial(rc: (Int, Int)) = {
        val (r, c) = rc
        val rChunk = if (r == clipN) partial_blocker.rClipStride else partial_blocker.rStride
        val cChunk = if (c == clipN) partial_blocker.cClipStride else partial_blocker.cStride
        val vSparse = GpiAdaptiveVector.fillWithSparse[T](cChunk)(sr.zero)
        ((r, c), GpiAdaptiveVector.fillWithSparse(rChunk)(vSparse))
      }
      val coordRdd = GpiDstr.getCoordRdd(sc, clipN + 1)
      val Partial = coordRdd.cartesian(coordRdd).map(initPartial).partitionBy(pbPartitioner)
      // ********
      // top level
      def recurse(contributingIndex: Int,
                  rPartial: RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])],
                  rA: RDD[((Int, Int), GpiBmat[T])],
                  rB: RDD[((Int, Int), GpiBmat[T])])
        : RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])] = 
        if (contributingIndex == clipnp1) rPartial
        else {
          def selectA(kv: ((Int, Int), GpiBmat[T])): List[((Int, Int), GpiBmat[T])] = {
            val (r, c) = kv._1
            val gxa = kv._2
            def selectIt(rd: Int,
                         dl: List[((Int, Int), GpiBmat[T])]): List[((Int, Int), GpiBmat[T])] =
              if (rd == clipnp1) dl else selectIt(rd + 1, ((rd, r), gxa) :: dl)
            val empty = List[((Int, Int), GpiBmat[T])]()
            if (c == contributingIndex) selectIt(0, empty) else empty
          }
          def selectB(kv: ((Int, Int), GpiBmat[T])): List[((Int, Int), GpiBmat[T])] = {
            val (c, r) = kv._1
            val gxa = kv._2
            def selectIt(rd: Int,
                         dl: List[((Int, Int), GpiBmat[T])]): List[((Int, Int), GpiBmat[T])] =
              if (rd == clipnp1) dl else selectIt(rd + 1, ((c, rd), gxa) :: dl)
            val empty = List[((Int, Int), GpiBmat[T])]()
            if (r == contributingIndex) selectIt(0, empty) else empty
          }
          // ********
          // permutations
          val sra = rA.flatMap(selectA).partitionBy(pbPartitioner)
          val srb = rB.flatMap(selectB).partitionBy(pbPartitioner)

          // ********
          // calculation functor
          val stats = GpiAdaptiveVector.Stat.Stat()
          def calculate(
              kv: ((Int, Int),
                   (Iterable[GpiAdaptiveVector[GpiAdaptiveVector[T]]],
                    Iterable[GpiBmat[T]],
                    Iterable[GpiBmat[T]]))) = {
            val t0 = System.nanoTime()
            val t0Add = stats.getAdd
            val t0Mul = stats.getMul
            val r = kv._1._1
            val c = kv._1._2
            val cPartial = kv._2._1.iterator.next()
            val cAa = kv._2._2.iterator.next().a
            val cBa = kv._2._3.iterator.next().a
            val iPartial = GpiOps.gpi_m_times_m(sr.addition,
                                                sr.multiplication,
                                                sr.addition,
                                                sr.zero,
                                                cAa,
                                                cBa,
                                                Option(stats))
            //            val updatedPartial = cPartial // DEBUG
            val t1 = System.nanoTime()
    val t01 = (t1 - t0) * 1.0e-9
    val t01Add = stats.getAdd - t0Add
    val t01Mul = stats.getMul - t0Mul
    val mflops =  (t01Add + t01Mul).toDouble / t01 * 1.0e-6
    
//    println ("mTm: t: >%.3f<, mflops: >%.3f<, adds: >(%s,%s), muls: >(%s,%s)".format(
//        t01, mflops, t01Add, LagUtils.pow2Bound(t01Add), t01Mul, LagUtils.pow2Bound(t01Mul)))
            val updatedPartial = GpiOps.gpi_zip(
              GpiOps.gpi_zip(sr.addition, _: GpiAdaptiveVector[T], _: GpiAdaptiveVector[T]),
              iPartial,
              cPartial)
            println(("mDm: updatedPartial for: >( %s , %s );" +
                "mTm: t: > %.3f <, mflops: > %.3f <, adds: >( %s , %s )<, muls: >( %s , %s )<;" +
                "zip: t: > %.3f <").format(
                r,c,
                t01, mflops, t01Add, LagUtils.pow2Bound(t01Add), t01Mul, LagUtils.pow2Bound(t01Mul),
                LagUtils.tt(t1, System.nanoTime())))
//            updatedPartial(1) match {
//              case _: com.ibm.lagraph.impl.GpiDenseVector[_] => println("DENSE")
//              case _: com.ibm.lagraph.impl.GpiSparseVector[_] => println("SPARSE")
//            }
            ((r, c), updatedPartial)
          }
          val nextPartialA = rPartial.cogroup(sra, srb)//.cache()
//          println("nextpartialA.count: >%s<".format(nextPartialA.count))
          val nextPartial = nextPartialA.map(calculate).partitionBy(pbPartitioner).cache()
          nextPartial.count

          // ********
          // recurse
          recurse(contributingIndex + 1, nextPartial, rA, rB)
        }
      val transposedResult = recurse(0, Partial, A.matRdd, BT.matRdd)

      // ********
      def toRcv(kv: ((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])) = {
        val arOff = kv._1._1.toLong * partial_blocker.rStride.toLong
        val acOff = kv._1._2.toLong * partial_blocker.cStride.toLong
        val ac = kv._1._2
        var res = List[((Long, Long), T)]()
        val itr = kv._2.denseIterator
        while (itr.hasNext) {
          val (ir, rv) = itr.next()
          val itc = rv.denseIterator
          while (itc.hasNext) {
            val (ic, v) = itc.next()
            res = ((arOff + ir.toLong, acOff + ic.toLong), v) :: res
          }
        }
        res
      }
      val rcvRdd = transposedResult.flatMap(toRcv)
      def perblock(iter: Iterator[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])]): Iterator[((Int, Int), GpiBmat[T])] = {
        require(iter.hasNext)
        val rcb = iter.next()
        require(!iter.hasNext)
        List(Tuple2(Tuple2(rcb._1._1, rcb._1._2), GpiBmat(rcb._2))).toIterator
      }
      LagDstrMatrix(
          hcd,
          rcvRdd,
          GpiDstrBmat(
              A.dstr,
              partial_blocker,
              transposedResult.mapPartitions(perblock, preservesPartitioning = true),
              partial_blocker.nrow,
              partial_blocker.ncol,
              msSparse))

      //Dhcd.mFromRcvRdd((partial_blocker.ncol, partial_blocker.nrow), rcvRdd.map {
      //D  case (k, v) => ((k._2, k._1), v) }, msSparse)

      //  // for transpose ...
      //  val bmatRdd = transposedResult.map { case (k, v) =>
      //    (k, GpiBmatAdaptive(v).asInstanceOf[GpiBmat[T]]) }
      //  LagDstrMatrix(hcd, rcvRdd,
      //    GpiDstrBmat(dstr, bmatRdd, maa.dstrBmat.nrow, maa.dstrBmat.ncol,
      //                maa.dstrBmat.sparseValue))
    }
  }
  // Hadagard ".multiplication"
  private[lagraph] override def mHm[T: ClassTag](
                                                 // // TODO maybe sr OR maybe f: (T1, T2) => T3?
                                                 //    f: (T, T) => T,
                                                 sr: LagSemiring[T],
                                                 m: LagMatrix[T],
                                                 n: LagMatrix[T]): LagMatrix[T] = {
    mZip(sr.multiplication, m, n)
    //    mZip(f, m, n)
  }
  // OuterProduct
  private[lagraph] def mOp[T: ClassTag](f: LagSemiring[T],
                                        m: LagMatrix[T],
                                        mCol: Long,
                                        n: LagMatrix[T],
                                        nRow: Long): LagMatrix[T] =
    (m, n) match {
      case (ma: LagDstrMatrix[T], na: LagDstrMatrix[T]) => {
        assert(ma.dstrBmat.sparseValue == na.dstrBmat.sparseValue)
        assert(f.multiplication.annihilator.isDefined)
        assert(f.multiplication.annihilator.get == ma.dstrBmat.sparseValue)
        val sparseValue = ma.dstrBmat.sparseValue
        val mac = ma.rcvRdd.filter { case (k, v) => k._2 == mCol }.map {
          case (k, v) => Tuple2(k._1, v)
        }
        val nar = na.rcvRdd.filter { case (k, v) => k._1 == nRow }.map {
          case (k, v) => Tuple2(k._2, v)
        }
        val cp = mac.cartesian(nar)
        val rcvRdd = cp.map {
          case (p1, p2) =>
            Tuple2(Tuple2(p1._1, p2._1), f.multiplication.op(p1._2, p2._2))
        }
        mFromRcvRdd(m.size, rcvRdd, sparseValue)
      }
    }

  // *******
  // matrix mechanics
  @deprecated("Not in scope", "0.1.0")
  private[lagraph] override def mTranspose[T: ClassTag](m: LagMatrix[T]): LagMatrix[T] = m match {
    case ma: LagDstrMatrix[T] => {
      def perblock(iter: Iterator[((Int, Int), GpiBmat[T])]): Iterator[((Int, Int), GpiBmat[T])] = {
        require(iter.hasNext)
        val rcb = iter.next()
        require(!iter.hasNext)
        val bmata = rcb._2 // bmat for this block
        //        val bmata = bmat
        //        bmat match {
        //          case bmata: GpiBmatAdaptive[T] => {
        val at = GpiSparseRowMatrix.transpose(bmata.a)
        if (rcb._1._1 == rcb._1._2) {
          List(Tuple2(Tuple2(rcb._1._1, rcb._1._2), GpiBmat(at))).toIterator
        }
        else {
          List(Tuple2(Tuple2(rcb._1._2, rcb._1._1), GpiBmat(at))).toIterator
        }
        //          }
        //        }
      }
      val newMmap = ma.rcvRdd.map { case (k, v) => ((k._2, k._1), v) }
      val matRdd = ma.dstrBmat.matRdd
        .mapPartitions(perblock, preservesPartitioning = false)
      val blocker = GpiDstrMatrixBlocker((ma.size._2, ma.size._1), ma.dstrBmat.dstr.nblockRequested)
      LagDstrMatrix(this,
                    newMmap,
                    GpiDstrBmat(ma.dstrBmat.dstr,
                                blocker,
                                matRdd,
                                ma.dstrBmat.ncol,
                                ma.dstrBmat.nrow,
                                ma.dstrBmat.sparseValue))
    }
  }
  private[lagraph] def vFromMrow[T: ClassTag](m: LagMatrix[T], mRow: Long): LagVector[T] =
    m match {
      case ma: LagDstrMatrix[T] =>
        LagDstrVector(this,
                      dstr.dstrBvecFromRowOfRcvRdd(sc, ma.size._2,
                                                   ma.rcvRdd, mRow, ma.dstrBmat.sparseValue))
    }
  private[lagraph] def vFromMcol[T: ClassTag](m: LagMatrix[T], mCol: Long): LagVector[T] =
    m match {
      case ma: LagDstrMatrix[T] =>
        LagDstrVector(this,
                      dstr.dstrBvecFromColOfRcvRdd(sc, ma.size._1,
                                                   ma.rcvRdd, mCol, ma.dstrBmat.sparseValue))
    }
  // *******
  // equivalence
  private[lagraph] override def vEquiv[T: ClassTag](u: LagVector[T], v: LagVector[T]): Boolean =
    (u, v) match {
      case (ua: LagDstrVector[T], va: LagDstrVector[T]) => {
        dstr.dstr_equiv(ua.dstrBvec, va.dstrBvec)
      }
    }
  // *******
  // diagnostics
  def mDiagnose[T: ClassTag](m: LagMatrix[T]): Unit =
    m match {
      case ma: LagDstrMatrix[T] =>
        {}
    }

}
// object LagDstrContext {
//  private def computeNblock(hc: LagContext,
//                            numv: Long,
//                            nblockRequestedInBlocker: Option[Int] = None): Int = {
//    val nblockRequestedInContext = hc match {
//      case hca: LagDstrContext => {
//        hca.nblockRequested
//      }
//    }
//    val nblockRequested = nblockRequestedInBlocker.getOrElse(nblockRequestedInContext)
//    if (nblockRequested > numv) numv.toInt else nblockRequested
//  }
// }

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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Builder
import scala.collection.mutable.{Map => MMap}
import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

final case class GpiDstrMatrixBlocker(val size: (Long, Long),
    val nblockRequested: Int) {

  require(nblockRequested > 0,
          "requested number_of_blocks: >%s< must be > 0".format(nblockRequested))
  val (nrow, ncol) = size
  val nblock = if (nblockRequested > nrow) nrow.toInt else nblockRequested
  require(ncol >= nblock, "current design precludes ncol: >%s< < nblock: >%s<" +
      "try reducing nblock to: >%s<".
      format(ncol, nblock, ncol))
  val partitions = nblock * nblock
  // clipping
  private def computeClip(n: Long, nblock: Int): Tuple2[Int, Int] =
    if (n >= nblock.toLong) { // at least one element per block
      val nblockl = nblock.toLong
      val strideA = (n / nblockl).toInt
      val clipA = (n - nblockl * strideA.toLong).toInt
      if (clipA == 0) {
        (strideA, strideA)
      } else {
        (strideA, (n - (nblockl - 1) * strideA.toLong).toInt)
      }
    } else { // more blocks than elements
      throw new RuntimeException("illegal condition: n: >%s< > nblock: >%s<".
          format(n, nblock))
    }
  val clipN = nblock - 1
  val (rStride, rClipStride) = computeClip(nrow, nblock)
  val (cStride, cClipStride) = computeClip(ncol, nblock)

  def matrixPartitionIndexToKey(index: Int): ((Int, Int)) = {
    (index / (clipN + 1), index % (clipN + 1))
  }
  def diagnose: Unit = {
    // scalastyle:off println
    println("GpiDstrMatrixBlocker: nrow: >%s<, ncol: >%s<, nblockRequested: >%s<".format(
        nrow, ncol, nblockRequested))
    println("GpiDstrMatrixBlocker: nblock: >%s<, clipN: >%s<, partitions: >%s<".format(
        nblock, clipN, partitions))
    println("GpiDstrMatrixBlocker: rStride: >%s<, rClipStride: >%s<".format(
        rStride, rClipStride))
    println("GpiDstrMatrixBlocker: cStride: >%s<, cClipStride: >%s<".format(
        cStride, cClipStride))
    // scalastyle:on println
  }
}


final case class GpiDstrVectorBlocker(val nrow: Long,
    val nblockRequested: Int) {

  require(nblockRequested > 0,
          "requested number_of_blocks: >%s< must be > 0".format(nblockRequested))
  val nblock = if (nblockRequested > nrow) nrow.toInt else nblockRequested
  val partitions = nblock * nblock
  // clipping
  private def computeClip(n: Long, nblock: Int): Tuple3[Int, Int, Int] =
    if (n >= nblock.toLong) { // at least one element per block
      val nblockl = nblock.toLong
      val strideA = (n / nblockl).toInt
      val clipA = (n - nblockl * strideA.toLong).toInt
      if (clipA == 0) {
        (nblock - 1, strideA, strideA)
      } else {
        (nblock - 1, strideA, (n - (nblockl - 1) * strideA.toLong).toInt)
      }
    } else { // more blocks than elements
      (n.toInt - 1, 1, 1)
    }
  val (clipN, stride, clipStride) = computeClip(nrow, nblock)
  val (vecClipN, vecStride, vecClipStride) = computeClip(stride, clipN + 1)
  val (remClipN, remStride, remClipStride) = computeClip(clipStride, clipN + 1)

  def vectorPartitionIndexToKey(index: Int): ((Int, Int)) = {
    val rblockin = index / (vecClipN + 1)
    require(index < clipN * (vecClipN + 1) + remClipN + 1, "bad index: >%s<".format(index))
    if (rblockin >= clipN) {
      (clipN, (index - clipN * (vecClipN + 1)) % (remClipN + 1))
    } else { (rblockin, index % (vecClipN + 1)) }
  }
  // ****
  def createVectorBaseRdd(sc: SparkContext): RDD[((Int, Int), Int)] = {
    val cp = for (r <- 0 until clipN + 1; c <- 0 until clipN + 1) yield (r, c)
    val cpFiltered = cp.flatMap {
      case (r, c) if r == clipN && c <= remClipN => Some((r, c))
      case (r, c) if r == clipN && c > remClipN => None
      case (r, c) if c <= vecClipN => Some((r, c))
      case (r, c) => None
    }
    sc.parallelize(cpFiltered.map((_, 0)), numSlices = (clipN + 1) * (vecClipN + 1))
      .partitionBy(new GpiBlockVectorPartitioner(clipN + 1, vecClipN + 1, remClipN + 1))
  }
  def getVectorStrides(rblock: Int): (Int, Int, Int) = {
    rblock match {
      case `clipN` => (remClipN, remStride, remClipStride)
      case _ => (vecClipN, vecStride, vecClipStride)
    }
  }
  def getVectorStride(rblock: Int, cblock: Int): Int = {
    (rblock, cblock) match {
      case (`clipN`, `remClipN`) => remClipStride
      case (`clipN`, _) => remStride
      case (_, `vecClipN`) => vecClipStride
      case (_, _) => vecStride
    }
  }
  def diagnose: Unit = {
    // scalastyle:off println
    println("GpiDstrVectorBlocker: nrow: >%s<, nblockRequested: >%s<".format(
        nrow, nblockRequested))
    println("GpiDstrVectorBlocker: nblock: >%s<, partitions: >%s<".format(
        nblock, partitions))
    println("GpiDstrVectorBlocker: clipN: >%s<, stride: >%s<, clipStride: >%s<".format(
        clipN, stride, clipStride))
    println("GpiDstrVectorBlocker: vecClipN: >%s<, vecStride: >%s<, vecClipStride: >%s<".format(
      vecClipN, vecStride, vecClipStride))
    println("GpiDstrVectorBlocker: remClipN: >%s<, remStride: >%s<, remClipStride: >%s<".format(
      remClipN, remStride, remClipStride))
    // scalastyle:on println
  }
}

// ********
case class GpiBvec[VS: ClassTag](val u: GpiAdaptiveVector[VS]) {
  def add(f: (VS, VS) => VS, a: GpiBvec[VS]): GpiBvec[VS] = {
    GpiBvec(GpiOps.gpi_zip(f, u, a.u))
  }
  def reduce[T2: ClassTag](f: (VS, T2) => T2, c: (T2, T2) => T2, zero: T2): T2 = {
    GpiOps.gpi_reduce(f, c, zero, u)
  }
  override def toString(): String = u.toString()
}

// ********
case class GpiBmat[MS: ClassTag](val a: GpiAdaptiveVector[GpiAdaptiveVector[MS]]) {
  val n = a.size
  def loadMapBuilder(cl: Builder[((Long, Long), MS), Seq[((Long, Long), MS)]],
                     rowOffset: Long,
                     colOffset: Long): Unit = {
    val itr = a.denseIterator
    while (itr.hasNext) {
      val (ir, rv) = itr.next()
      val itc = rv.denseIterator
      while (itc.hasNext) {
        val (ic, v) = itc.next()
        cl += Tuple2((rowOffset + ir.toLong, colOffset + ic.toLong), v) // LONG ISSUE
      }
    }
  }
  override def toString(): String = a.toString() // added by GABI
}

case class GpiDstrBvec[VS](val dstr: GpiDstr,
                           val blocker: GpiDstrVectorBlocker,
                           val vecRdd: RDD[((Int, Int), GpiBvec[VS])],
                           val sparseValue: VS) {
  val nrow = blocker.nrow
  override def toString(): String = {
    val ab = ArrayBuffer.fill(1, nrow.toInt)(sparseValue)
    val mr = vecRdd.collect
    vecRdd.collect.foreach {
      case (k, v) => {
        val vecstride = blocker.getVectorStrides(k._1)._2
        for (r <- 0 until v.u.size) {
          ab(0)(k._1 * blocker.stride + k._2 * vecstride + r) = v.u(r)
        }
      }
    }
    val aab = ab.toArray
    aab.deep.mkString("\n").replaceAll("ArrayBuffer", "")
  }
}

case class GpiDstrBmat[MS](val dstr: GpiDstr,
                           val blocker: GpiDstrMatrixBlocker,
                           val matRdd: RDD[((Int, Int), GpiBmat[MS])],
                           val nrow: Long,
                           val ncol: Long,
                           val sparseValue: MS) {
  override def toString(): String = {
    val ab = ArrayBuffer.fill(nrow.toInt, ncol.toInt)(sparseValue)
    val mr = matRdd.collect
    var a = 0
    for (a <- 0 until mr.size) {
      val ai = mr(a)
      val ar = ai._1._1
      val ac = ai._1._2
      val avga = ai._2
      var r = 0
      for (r <- 0 until avga.a.size) {
        val rv = avga.a(r)
        var c = 0
        for (c <- 0 until rv.size) {
          ab(ar * blocker.rStride + r)(ac * blocker.cStride + c) = rv(c)
        }
      }
    }
    val aab = ab.toArray
    aab.deep.mkString("\n").replaceAll("ArrayBuffer", "")
  }
}

// ********
/**
  * Distributed GPI abstraction
  */
class GpiDstr(val nblockRequested: Int, DEBUG: Boolean = false)
    extends Serializable {
  require(nblockRequested > 0,
          "requested number_of_blocks: >%s< must be > 0".format(nblockRequested))
  // ****
  def dstrBvecFromRowOfRcvRdd[VS: ClassTag](
      sc: SparkContext,
      nrow: Long,
      rcvRdd: RDD[((Long, Long), VS)], // ((VertexId, VertexId), VS)
      mRow: Long,
      vsSparse: VS): GpiDstrBvec[VS] = {
    def f(input: ((Long, Long), VS)): Option[(Long, VS)] = {
      val r = input._1._1
      val c = input._1._2
      val v = input._2
      if (r == mRow) Option(c, v) else None
    }
    val aRdd_rcv_init = rcvRdd.flatMap(f)
    aRdd_rcv_init.setName("aRdd_rcv_init")
    dstrBvecFromRvRdd(sc, nrow, aRdd_rcv_init, vsSparse)
  }
  // ****
  def dstrBvecFromColOfRcvRdd[VS: ClassTag](
      sc: SparkContext,
      nrow: Long,
      rcvRdd: RDD[((Long, Long), VS)], // ((VertexId, VertexId), VS)
      mCol: Long,
      vsSparse: VS): GpiDstrBvec[VS] = {
    def f(input: ((Long, Long), VS)): Option[(Long, VS)] = {
      val r = input._1._1
      val c = input._1._2
      val v = input._2
      if (c == mCol) Option(r, v) else None
    }
    val aRdd_rcv_init = rcvRdd.flatMap(f)
    aRdd_rcv_init.setName("aRdd_rcv_init")
    dstrBvecFromRvRdd(sc, nrow, aRdd_rcv_init, vsSparse)
  }
  // ****
  def dstrBmatToRcvRdd[MS](dstrBmat: GpiDstrBmat[MS]): RDD[((Long, Long), MS)] = {
    def f(input: ((Int, Int), GpiBmat[MS])): List[((Long, Long), MS)] = {
      val rblockl = input._1._1.toLong
      val cblockl = input._1._2.toLong
      val rstridel = dstrBmat.blocker.rStride.toLong
      val cstridel = dstrBmat.blocker.cStride.toLong
      val bmata = input._2
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
    }
    dstrBmat.matRdd.flatMap(f)
  }
  // ****
  def dstrBvecToRvRdd[VS](dstrBvec: GpiDstrBvec[VS]): RDD[(Long, VS)] = {
    val blocker = dstrBvec.blocker
    val vecRdd = dstrBvec.vecRdd
    val stridel = dstrBvec.blocker.stride.toLong
    def f(input: ((Int, Int), GpiBvec[VS])): List[(Long, VS)] = {
      val (rblock, cblock) = input._1
      val av = input._2.u
      if (av.denseCount == 0) {
        List()
      } else {
        val vecstridel = blocker.getVectorStrides(rblock)._2.toLong
        av.denseIterator.toList.map {
          case (r: Int, v) => (rblock * stridel + cblock * vecstridel + r, v)
        }
      }
    }
    vecRdd.flatMap(f)
  }

  // ****
  def dstrBmatAdaptiveToRcvRdd[MS: ClassTag](sc: SparkContext,
                                             dstrBmat: GpiDstrBmat[MS]): RDD[((Long, Long), MS)] = {
    val rstridel = dstrBmat.blocker.rStride.toLong
    val cstridel = dstrBmat.blocker.cStride.toLong
    def perblock(kv: ((Int, Int), GpiBmat[MS])): List[((Long, Long), MS)] = {
      val rblockl = kv._1._1.toLong
      val cblockl = kv._1._2.toLong
      def rowIterate(riter: Iterator[(Int, GpiAdaptiveVector[MS])],
                     rcv: List[((Long, Long), MS)]): List[((Long, Long), MS)] =
        if (!riter.hasNext) rcv
        else {
          def colIterate(cr: Int,
                         citer: Iterator[(Int, MS)],
                         rcv: List[((Long, Long), MS)]): List[((Long, Long), MS)] =
            if (!citer.hasNext) rcv
            else {
              val (cc, cv) = citer.next()
              colIterate(
                cr,
                citer,
                ((rblockl * rstridel + cr.toLong, cblockl * cstridel + cc.toLong), cv) :: rcv)
            }
          val (rr, rv) = riter.next()
          rowIterate(riter, colIterate(rr, rv.denseIterator, rcv))
        }
      rowIterate(kv._2.a.denseIterator, List[((Long, Long), MS)]())
    }
    dstrBmat.matRdd.flatMap(perblock)
  }
  // ****
  def dstrBmatAdaptiveFromRcvRdd[MS: ClassTag](
      sc: SparkContext,
      size: (Long, Long),
      rr: RDD[((Long, Long), MS)],
      msSparse: MS): (GpiDstrBmat[MS], Long, Map[String, Double]) = {
    val (nrow, ncol) = size
    val ttMS = classTag[MS]
    val blocker = GpiDstrMatrixBlocker(size, this.nblockRequested)
    val r_chunk = blocker.rStride
    val r_csrChunkL = r_chunk.toLong
    val r_csrChunkClipL = blocker.rClipStride.toLong
    val c_chunk = blocker.cStride
    val c_csrChunkL = c_chunk.toLong
    val c_csrChunkClipL = blocker.cClipStride.toLong

    // **
    // block coordinates
    val blockedRdd = rr.map {
      case ((i, j), elem) => {
        val (rgin, cgin, rlin, clin) =
          ((i / r_chunk).toInt, (j / c_chunk).toInt, (i % r_chunk).toInt, (j % c_chunk).toInt)
        val (rg, rl) =
          if (rgin > blocker.clipN) (blocker.clipN, rlin + (rgin - blocker.clipN) * r_chunk)
          else (rgin, rlin)
        val (cg, cl) =
          if (cgin > blocker.clipN) (blocker.clipN, clin + (cgin - blocker.clipN) * c_chunk)
          else (cgin, clin)
        ((rg, cg, rl, cl), elem)
      }
    }
    blockedRdd.setName("blockedRdd")
    // blockedRdd.cache()
    // blockedRdd.count()

    // **
    implicit val csrOrdering = new Ordering[(Int, Int, Int, Int)] {
      override def compare(l: (Int, Int, Int, Int), r: (Int, Int, Int, Int)): Int = {
        val lr = l._3.toLong
        val lc = l._4.toLong
        val rr = r._3.toLong
        val rc = r._4.toLong
        val lthis =
          if (l._2 == blocker.clipN) lr * c_csrChunkClipL + lc else lr * c_csrChunkL + lc
        val rthat =
          if (r._2 == blocker.clipN) rr * c_csrChunkClipL + rc else rr * c_csrChunkL + rc
        if (lthis < rthat) -1
        else {
          if (lthis == rthat) 0 else 1
        }
      }
    }
    // the global sort
    val blockedRdd_partitioned = blockedRdd.repartitionAndSortWithinPartitions(
      new GpiBlockMatrixPartitioner(blocker.clipN + 1))
    //    blockedRdd_partitioned.collect().foreach { case (k, v) => println(
    //      "blockedRdd_partitioned: k: (%s, %s, %s, %s) v: %s".format(k._1,k._2,k._3,k._4,v)) }
    blockedRdd_partitioned.setName("blockedRdd_partitioned")
    // blockedRdd_partitioned.cache()
    // blockedRdd_partitioned.count()


    // **
    def addArrays(
        index: Int,
        iter: Iterator[((Int, Int, Int, Int), MS)]): Iterator[((Int, Int), GpiBmat[MS])] = {
      val (r, c) = (index / blocker.nblock, index % blocker.nblock)
      val rChunk = if (r == blocker.clipN) blocker.rClipStride else r_chunk
      val cChunk = if (c == blocker.clipN) blocker.cClipStride else c_chunk
      val vSparse = GpiAdaptiveVector.fillWithSparse[MS](cChunk)(msSparse)
      val mSparse = GpiAdaptiveVector.fillWithSparse(rChunk)(vSparse)
      val rBuffer = ArrayBuffer.fill(rChunk)(vSparse)
      val crBuffer = ArrayBuffer.empty[Int]
      val cvBuffer = ArrayBuffer.empty[MS]
      if (iter.hasNext) {
        var first = true
        var lrs = -1
        var lr = -1
        while (iter.hasNext) {
          val ircv = iter.next()
          lr = ircv._1._3
          if (first) {
            lrs = lr
            first = false
          }
          val lc = ircv._1._4
          val v = ircv._2
          if (lr != lrs) {
            val rseq =
              GpiAdaptiveVector.fromRvSeq(crBuffer, cvBuffer, msSparse, cChunk)
            rBuffer(lrs) = rseq
            crBuffer.clear()
            cvBuffer.clear()
            lrs = lr
          }
          crBuffer += lc
          cvBuffer += v
        }
        val rseq =
          GpiAdaptiveVector.fromRvSeq(crBuffer, cvBuffer, msSparse, cChunk)
        rBuffer(lr) = rseq
        val mav = GpiAdaptiveVector.fromSeq(rBuffer, vSparse)

        val l = Tuple2(Tuple2(r, c), GpiBmat(mav))

        List(l).toIterator
      } else {
        // SPARSE
        val l = Tuple2(Tuple2(r, c), GpiBmat(mSparse))
        List(l).toIterator
      }
    }
    // **
    val matrixRdd =
      blockedRdd_partitioned.mapPartitionsWithIndex(addArrays, true)
    matrixRdd.setName("matrixRdd")
    // matrixRdd.cache()
    // matrixRdd.count()
    //    matrixRdd.collect().foreach { case (k, v) => println(
    //      "matrixRdd: (%s,%s): %s".format(
    //        k._1, k._2, GpiSparseRowMatrix.toString(v.asInstanceOf[GpiBmatAdaptive[MS]].a))) }

    // create DstrBmat
    val dstrBmatLoaded = GpiDstrBmat(this, blocker, matrixRdd, blocker.nrow, blocker.ncol, msSparse)

    // obtain count and force checkpoint
    val t0 = System.nanoTime()
    val nBlock = dstrBmatLoaded.matRdd.cache().count()
    val t1 = System.nanoTime()

    // ****************
    def fixtime(nano: Long): Double = {
      nano * 1.0e-9
    }
    val times = Map("pt1" -> fixtime(t1 - t0))
    (dstrBmatLoaded, nBlock, times)
  }

  // ****
  // Map[GlobalIndex, VS] -> RDD[(LocalIndex, Option[GpiBvec[VS]])]
  def dstrBvecFromMap[VS: ClassTag](sc: SparkContext,
                                    nrow: Long,
                                    v: Map[Long, VS],
                                    vsSparse: VS,
                                    cache: Boolean = true): GpiDstrBvec[VS] = {
    val l = v.map { case (k: Long, v) => (k, v) }(scala.collection.breakOut): List[(Long, VS)]
    // List((rabs,value))
    val aRdd_map_init = sc.parallelize(l)
    aRdd_map_init.setName("aRdd_map_init")
    dstrBvecFromRvRdd(sc, nrow, aRdd_map_init, vsSparse, cache)
  }
  // ****
  private def dstrBvecFromCons[VS: ClassTag](sc: SparkContext,
                                             nrow: Long,
                                             vsCons: VS,
                                             vsSparse: VS): GpiDstrBvec[VS] = {
    val blocker = GpiDstrVectorBlocker(nrow, nblockRequested)
    def consBlockerV(kv: ((Int, Int), Int)): ((Int, Int), GpiBvec[VS]) = {
      val (rblock, cblock) = kv._1
      val curVectorStride = blocker.getVectorStride(rblock, cblock)
      val vec = Vector.fill[VS](blocker.getVectorStride(rblock, cblock))(vsCons)
      val av = GpiAdaptiveVector.fromSeq[VS](vec, vsSparse)
      //      if (av.denseCount == 0) (p, bvSparse) else (p, GpiBvec(av))
      ((rblock, cblock), GpiBvec(av))
    }
    val aRdd_cons = blocker.createVectorBaseRdd(sc).map(consBlockerV)
    // .partitionBy(new GpiBlockVectorPartitioner(blocker.clipN + 1, vecClipN + 1, remClipN + 1))
    aRdd_cons.setName("aRdd_cons")
    GpiDstrBvec(this, blocker, aRdd_cons, vsSparse)

  }
  // ****
  // RDD[(VertexId, VS)] -> RDD[(LocalIndex, Option[GpiBvec[VS]])]
  private def dstrBvecFromRvRdd[VS: ClassTag](sc: SparkContext,
                                              nrow: Long,
                                              aRdd_init: RDD[(Long, VS)],
                                              vsSparse: VS,
                                              cache: Boolean = true): GpiDstrBvec[VS] = {
    // blocker
    val blocker = GpiDstrVectorBlocker(nrow, nblockRequested)
    // sparse
    val bvecSparseVecStride =
        GpiBvec(GpiAdaptiveVector.fillWithSparse[VS](blocker.vecStride)(vsSparse))
    val bvecSparseVecClipStride = GpiBvec(
        GpiAdaptiveVector.fillWithSparse[VS](blocker.vecClipStride)(vsSparse))
    val bvecSparseRemStride =
        GpiBvec(GpiAdaptiveVector.fillWithSparse[VS](blocker.remStride)(vsSparse))
    val bvecSparseRemClipStride = GpiBvec(
        GpiAdaptiveVector.fillWithSparse[VS](blocker.remClipStride)(vsSparse))

    // blocking
    val stridel = blocker.stride.toLong
    val vecStridel = blocker.vecStride.toLong
    val vecClipStridel = blocker.vecClipStride.toLong
    val remStridel = blocker.remStride.toLong
    val remClipStridel = blocker.remClipStride.toLong

    // **
    def partitionVectorIntoBlocks(rv: (Long, VS)): ((Int, Int), (Int, VS)) = {

      val rabs = rv._1
      val rblockin = (rabs / stridel)
      val (rblock, cabs) =
        if (rblockin > blocker.clipN) {
          (blocker.clipN.toLong, rabs % stridel + (rblockin - blocker.clipN) * stridel)
        } else { (rblockin, rabs % stridel) }
      if (rblock == blocker.clipN) {
        val cblockin = cabs / remStridel
        val (cblock, rlocal) =
          if (cblockin > blocker.clipN) {
            (blocker.clipN.toLong, cabs % remStridel + (cblockin - blocker.clipN) * remStridel)
          } else { (cblockin, cabs % remStridel) }
        //   println(
        //  "rblock: >%s<, cabs: >%s<, cblockin: >%s<, cblock: >%s<, rlocal: >%s<, value: >%s<"
        //    .format(
        //      rblock,cabs,cblockin,cblock,rlocal,rv._2))
        ((rblock.toInt, cblock.toInt), (rlocal.toInt, rv._2))
      } else {
        val cblockin = cabs / vecStridel
        val (cblock, rlocal) =
          if (cblockin > blocker.clipN) {
            (blocker.clipN.toLong, cabs % vecStridel + (cblockin - blocker.clipN) * vecStridel)
          } else { (cblockin, cabs % vecStridel) }
        ((rblock.toInt, cblock.toInt), (rlocal.toInt, rv._2))
      }
    }
    //    println(aRdd_init.collect.size)
    //    aRdd_init.collect().foreach { case (k, v) =>
    //      println("aRdd_init: k: %s, v: %s".format(k,v)) }
    val vRdd_tagged = aRdd_init.map(partitionVectorIntoBlocks)

    //    vRdd_tagged.collect().foreach { case (k, v) =>
    //      println("vRdd_tagged: k: (%s, %s) v: (%s, %s)".format(k._1,k._2,v._1,v._2)) }
    val vRdd_partitioned = vRdd_tagged.combineByKey(
      List[(Int, VS)](_),
      (x: List[(Int, VS)], y: (Int, VS)) => x :+ y,
      (x: List[(Int, VS)], y: List[(Int, VS)]) => x ::: y)

    //    vRdd_partitioned.collect().foreach { case (k, v) =>
    //      println("vRdd_partitioned: k: (%s, %s) v: %s".format(k._1,k._2,v)) }
    // TODO replace MMap w/ repartitionAndSortWithinPartitions?
    def toBvec(kv: ((Int, Int), List[(Int, VS)])): ((Int, Int), GpiBvec[VS]) = {
      val (rblock, cblock) = kv._1
      val listOfV = kv._2
      val mm: MMap[Int, VS] = MMap()
      listOfV.foreach { case (k, v) => mm(k) = v }
      val curStride = blocker.getVectorStride(rblock, cblock)
      //      println("rblock: >%s<, cblock: >%s<, curStride: >%s<".format(
      //        rblock, cblock, curStride))
      val avec = GpiAdaptiveVector.fromMap[VS](mm.toMap, vsSparse, curStride)
      ((rblock, cblock), GpiBvec(avec))
    }
    val aRdd_bvec = vRdd_partitioned.map(toBvec)
    //    aRdd_bvec.collect().foreach { case (k, v) =>
    //      println("aRdd_bvec: k: (%s, %s) v: %s".format(k._1,k._2,v.u.toVector)) }
    def addSparseBvec(
        index: Int,
        iter: Iterator[((Int, Int), GpiBvec[VS])]): Iterator[((Int, Int), GpiBvec[VS])] = {
      if (iter.hasNext) iter
      else {
        val (rblock, cblock) = blocker.vectorPartitionIndexToKey(index)
        val clipN = blocker.clipN
        val remClipN = blocker.remClipN
        val vecClipN = blocker.vecClipN
        val sparseBvec = (rblock, cblock) match {
          case (`clipN`, `remClipN`) => bvecSparseRemClipStride
          case (`clipN`, _) => bvecSparseRemStride
          case (_, `vecClipN`) => bvecSparseVecClipStride
          case (_, _) => bvecSparseVecStride
        }
        List(((rblock, cblock), sparseBvec)).toIterator
      }
    }
    val aRdd_complete = aRdd_bvec
      .partitionBy(new GpiBlockVectorPartitioner(blocker.clipN + 1,
                                                 blocker.vecClipN + 1,
                                                 blocker.remClipN + 1))
      .mapPartitionsWithIndex(addSparseBvec, true)

    val aRdd = if (cache) aRdd_complete else aRdd_complete.cache()
    aRdd.setName("aRdd")
    //    aRdd.collect().foreach { case (k, v) =>
    //      println("aRdd: k: (%s, %s) v: %s".format(k._1,k._2,v.u.toVector)) }
    GpiDstrBvec(this, blocker, aRdd, vsSparse)

  }

  // ****
  private def dstrBvecFromIndices(sc: SparkContext,
                                  nrow: Long,
                                  offset: Long,
                                  vsSparseOpt: Option[Long]): GpiDstrBvec[Long] = {
    val vsSparse = vsSparseOpt.getOrElse(0L)

    val blocker = GpiDstrVectorBlocker(nrow, nblockRequested)

    // ****
    // partition vector into blocks:
    //  List((r_abs, value))
    //    => (partition, (r_local, value))
    def indexBlockerV(kv: ((Int, Int), Int)): ((Int, Int), GpiBvec[Long]) = {
      val (rblock, cblock) = kv._1
      val (vclipn, vstride, vclipstride) = blocker.getVectorStrides(rblock)
      val curVectorStride = blocker.getVectorStride(rblock, cblock)
      val begin = rblock.toLong * blocker.stride.toLong + cblock.toLong * vstride.toLong + offset
      val end = begin + curVectorStride
      //      println("clipN >%s<, stride >%s<, clipStride >%s<".format(clipN, stride, clipStride))
      //      println("vstride >%s<, vclipstride >%s<, rblock: >%s<, cblock: >%s<, stride: >%s<" +
      //        ", clipstride: >%s<, curVectorStride: >%s<, begin: >%s<, end: >%s<".format(
      //        vstride, vclipstride, rblock,cblock,stride,vclipstride,curVectorStride,begin,end))
      // never sparse
      ((rblock, cblock), GpiBvec(GpiAdaptiveVector.fromSeq[Long]((begin until end), vsSparse)))
    }
    val aRdd_indices = blocker.createVectorBaseRdd(sc).map(indexBlockerV)
    // .partitionBy(new GpiBlockVectorPartitioner(clipN + 1, vecClipN + 1, remClipN + 1))
    aRdd_indices.setName("aRdd_indices")
    GpiDstrBvec(this, blocker, aRdd_indices, vsSparse)
  }

  // GPI base

  /**
    * Creates an vector where each element is set equal to a specified value
    *
    *  @param T type of the new vector.
    *  @param size length of the new vector.
    *  @param x specified value.
    *  @param sparseValue determines the sparsity of the new vector.
    */
  def dstr_replicate[T: ClassTag](sc: SparkContext,
                                  nrow: Long,
                                  x: T,
                                  sparseValueOpt: Option[T] = None): GpiDstrBvec[T] = {
    val sparseValue = sparseValueOpt.getOrElse(x)
    dstrBvecFromCons(sc, nrow, x, sparseValue)
  }

  /**
    * Creates an vector of type Int with range [start;end) and a step value of 1
    *
    *  @param start the start of the range
    *  @param end the end of the range
    */
  def dstr_indices(sc: SparkContext,
                   nrow: Long,
                   start: Long,
                   sparseValueOpt: Option[Long] = None): GpiDstrBvec[Long] = {
    dstrBvecFromIndices(sc, nrow, start, sparseValueOpt)
  }

  /**
    * Applies a binary operator to a start value and all elements of this
    *  vector, going left to right.
    *
    *  @tparam T1 the input vector type.
    *  @tparam T2 the result type of the binary operator.
    *  @param z the start value.
    *  @param f the binary operator
    *  @param c binary operator for merging values between partitions
    */
  def dstr_reduce[T1, T2: ClassTag](f: (T1, T2) => T2,
                                    c: (T2, T2) => T2,
                                    z: T2,
                                    u: GpiDstrBvec[T1]): T2 = {
    u.vecRdd.map { case (i, v) => v.reduce(f, c, z) }.fold(z)(c)
  }

  /**
    * Creates a new vector by applying a unary operator to all elements of the input vector.
    *
    *  @tparam T1 the input vector type.
    *  @tparam T2 the output vector type.
    *  @param f the unary operator.
    *  @param u the input AdaptiveVector
    *
    */
  def dstr_map[T1: ClassTag, T2: ClassTag](f: (T1) => T2,
                                           u: GpiDstrBvec[T1],
                                           sparseValueT2Opt: Option[T2] = None): GpiDstrBvec[T2] = {
    def fv(bvec: GpiBvec[T1]): GpiBvec[T2] = {

      val sparseValueT2 = sparseValueT2Opt.getOrElse(f(u.sparseValue))
      // x      val mr = LagUtils.timeblock(..., "gpi_map: ")
      val mr = GpiOps.gpi_map[T1, T2](f, bvec.u)
      GpiBvec(mr)
    }
    val sparseValueT2 = sparseValueT2Opt.getOrElse(f(u.sparseValue))
    val mappedDVV = u.vecRdd
      .mapValues(fv)
      .partitionBy(new GpiBlockVectorPartitioner(u.blocker.clipN + 1,
                                                 u.blocker.vecClipN + 1,
                                                 u.blocker.remClipN + 1))
    GpiDstrBvec(this, u.blocker, mappedDVV, sparseValueT2)
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
  def dstr_zip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      u: GpiDstrBvec[T1],
      v: GpiDstrBvec[T2],
      sparseValueT3Opt: Option[T3] = None): GpiDstrBvec[T3] = {
    val uVecRdd = u.vecRdd
    val unrow = u.nrow
    val vVecRdd = v.vecRdd
    val vnrow = v.nrow
    require(unrow == vnrow)
    val nrow = unrow
    val a_dstr_zip = uVecRdd.cogroup(vVecRdd)
    a_dstr_zip.setName("a_dstr_zip")
    val sparseValueT3 =
      sparseValueT3Opt.getOrElse(f(u.sparseValue, v.sparseValue))
    def fco(input: (Iterable[GpiBvec[T1]], Iterable[GpiBvec[T2]])): GpiBvec[T3] = {
      val iterableUoV = input._1
      require(iterableUoV.size < 2)
      require(iterableUoV.size > 0)
      val iterableVoV = input._2
      require(iterableVoV.size < 2)
      require(iterableVoV.size > 0)
      val ua = iterableUoV.head.u
      val va = iterableVoV.head.u
      // x val wa = LagUtils.timeblock(GpiOps.gpi_zip(
      //   f, ua, va, sparseValueT3Opt, Option(ua.threshold)), "gpi_zip")
      val wa = GpiOps.gpi_zip(f, ua, va)
      //      // SPARSE
      //      if (wa.denseCount == 0) (indx, GpiBvec(
      //        GpiAdaptiveVector.fillWithSparse[T3](stride)
      //        (sparseValueT3))) else (indx, GpiBvec(wa))
      GpiBvec(wa)
    }
    val a_dstr_zip_map = a_dstr_zip.mapValues(fco)
    a_dstr_zip_map.setName("a_dstr_zip_map")
    //    println("SSSSSSSS: dstr_zip: sparseValueT3: >%s<".format(sparseValueT3))
    GpiDstrBvec(this, u.blocker, a_dstr_zip_map, sparseValueT3)
  }

  // GPI derived

  /**
    * Compute the inner product between two vector
    *
    *  @tparam T1 first input vector type
    *  @tparam T2 second input vector type
    *  @tparam T3 output type of semiring multiplication
    *  @tparam T4 output type of semiring addition
    *  @param f semiring addition (commutative monoid with identity element)
    *           used for merging values within a partition
    *  @param c semiring addition used for merging values between partitions
    *  @param g semiring multiplication (a monoid)
    *  @param zero identity element for semiring addition
    *  @param u first input vector.
    *  @param v second input vector.
    *  @param sparseValue determines sparsity for output of semiring multiplication
    *
    */
  def dstr_innerp[T1: ClassTag, T2: ClassTag, T3: ClassTag, T4: ClassTag](
      f: (T3, T4) => T4,
      c: (T4, T4) => T4,
      g: (T1, T2) => T3,
      zero: T4,
      u: GpiDstrBvec[T1],
      v: GpiDstrBvec[T2],
      sparseValueT3Opt: Option[T3] = None): T4 = {
    dstr_reduce(f, c, zero, dstr_zip(g, u, v, sparseValueT3Opt))
  }
  // ******
  def dstr_equiv[T: ClassTag](u: GpiDstrBvec[T], v: GpiDstrBvec[T]): Boolean = {
    val uVecRdd = u.vecRdd
    val unrow = u.nrow
    val vVecRdd = v.vecRdd
    val vnrow = v.nrow
    require(unrow == vnrow)
    val nrow = unrow
    val a_dstr_equiv = uVecRdd.cogroup(vVecRdd)
    a_dstr_equiv.setName("a_dstr_equiv")
    def fco(kv: ((Int, Int), (Iterable[GpiBvec[T]], Iterable[GpiBvec[T]]))): Boolean = {
      val iterableUoV = kv._2._1
      require(iterableUoV.size < 2)
      require(iterableUoV.size > 0)
      val iterableVoV = kv._2._2
      require(iterableVoV.size < 2)
      require(iterableVoV.size > 0)
      val ua = iterableUoV.head.u
      val va = iterableVoV.head.u
      // x      val wa = LagUtils.timeblock(GpiOps.gpi_equiv(ua, va), "gpi_equiv")
      val wa = GpiOps.gpi_equiv(ua, va)
      wa
    }
    val a_dstr_equiv_map = a_dstr_equiv.map(fco)
    a_dstr_equiv_map.setName("a_dstr_equiv_map")
    a_dstr_equiv_map.fold(true)((l, r) => l && r)
  }

}

// ********
object GpiDstr {

  // constructor
  def apply(sc: SparkContext, nblockRequested: Int, DEBUG: Boolean = false): GpiDstr = {
    // val mtv = GpiMtv(sc, if (numv >= nblock) nblock else numv.toInt, DEBUG = DEBUG)
    // new GpiDstr(nblock, numv, mtv, DEBUG)
    new GpiDstr(nblockRequested, DEBUG)
  }

  // ****
  def mrddToString[MS](mRdd: RDD[((Long, Long), MS)], ncol: Int, sparseValue: MS): String = {
    val ma = mRdd.collect()
    val (nrowx: Int, ncolx: Int) = ma.fold((0, 0)) {
      case (c, n) =>
        val crc: (Int, Int) = c match {
          case (z: (Int @unchecked, Int @unchecked)) =>
            z
        }
        val nrc: (Int, Int) = n match {
          case (z: (Long @unchecked, Long @unchecked), _) =>
            (z._1.toInt, z._2.toInt)
        }
        val nmr = if (nrc._1 > crc._1) nrc._1 else crc._1
        val nmc = if (nrc._2 > crc._2) nrc._2 else crc._2
        (nmr, nmc)
    }
    val ab = ArrayBuffer.fill(nrowx + 1, ncol)(sparseValue)
    ma.map { case (x, y) => ab(x._1.toInt)(x._2.toInt) = y }
    val aab = ab.toArray
    aab.deep.mkString("\n").replaceAll("ArrayBuffer", "")
  }

  // ****
  def vrddToString[VS](vRdd: RDD[(Long, VS)], nrow: Int, sparseValue: VS): String = {
    val va = vRdd.collect()
    val ab = ArrayBuffer.fill(1, nrow)(sparseValue)
    va.map { case (x, y) => ab(0)(x.toInt) = y }
    val aab = ab.toArray
    aab.deep.mkString("\n").replaceAll("ArrayBuffer", "")
  }

  // ****
  def debugSummaryOfDstrBmatSparsity[MS](db: GpiDstrBmat[MS]): String = {
    def debit(a: ((Int, Int), GpiBmat[MS])): String = {
      val (r, c) = a._1
      val bga = a._2
      val x = bga.a
      var ys = 0
      var ydc = 0L
      val itr = bga.a.denseIterator
      while (itr.hasNext) {
        val (ir, r) = itr.next()
        if (r.size > ys) ys = r.size
        ydc = ydc + r.denseCount
      }
      val pt2 = "(%d, %d), (%d, %d)".format(x.size, ys, x.denseCount, ydc)
      "(%d,%d):%s".format(r, c, pt2)
    }
    db.matRdd.collect().map(debit).mkString("[", ",", "]")
    //      db.matRdd.collect().map{ case (k, v)
    //         => "(%d,%d)".format(k._1,k._2)}.mkString("[", ",", "]")
  }

  // ****
  def countDstrBvecNonzeros(dstrBvec: GpiDstrBvec[_]): Long = {
    // ****
    def countTotalNonzeros(bveco: GpiBvec[_]): Long = {
      bveco.u.denseCount
    }
    def reduceCountTotalNonzeros(a: Long, b: Long) = a + b
    val checkTotalNonzeros = dstrBvec.vecRdd
      .mapValues(countTotalNonzeros)
      .values
      .reduce(reduceCountTotalNonzeros)
    checkTotalNonzeros
  }
  // utility for constructing blocked vectors and matrices
  def getCoordRdd(sc: SparkContext, N: Int): RDD[Int] = {
    val coordRdd =
      sc.parallelize(List.range(0, N), numSlices = N)
    coordRdd.setName("GpiDstrCoordRdd")
    coordRdd
  }
}

// ********
class GpiBlockVectorPartitioner(rblocks: Int, cblocks: Int, remCblocks: Int) extends Partitioner {
  val rblocksm1 = rblocks - 1
  override def numPartitions: Int = rblocksm1 * cblocks + remCblocks
  override def getPartition(key: Any): Int = key match {
    case (i: Int, j: Int) => {
      if (i == rblocksm1) (i * cblocks) + j % remCblocks
      else (i * cblocks) + j % cblocks
    }
    case _ =>
      throw new IllegalArgumentException("illegal key")
  }
  override def equals(other: Any): Boolean = other match {
    case h: GpiBlockVectorPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}

// ********
class GpiBlockMatrixPartitioner(nblx: Int) extends Partitioner {
  override def numPartitions: Int = nblx * nblx
  override def getPartition(key: Any): Int = key match {
    case (i: Int, j: Int) =>
      (i * nblx) + j % nblx
    case (i: Int, j: Int, rc: Long) =>
      (i * nblx) + j % nblx
    case (i: Int, j: Int, r: Int, c: Int) =>
      (i * nblx) + j % nblx
    case _ =>
      throw new IllegalArgumentException("illegal key")
  }

  override def equals(other: Any): Boolean = other match {
    case h: GpiBlockMatrixPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}

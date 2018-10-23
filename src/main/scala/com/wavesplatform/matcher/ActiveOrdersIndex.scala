package com.wavesplatform.matcher

import java.nio.ByteBuffer

import cats.syntax.functor._
import com.google.common.primitives.Ints
import com.wavesplatform.account.Address
import com.wavesplatform.crypto
import com.wavesplatform.database.{Key, RW, ReadOnlyDB}
import com.wavesplatform.db.prefixIterator
import com.wavesplatform.matcher.ActiveOrdersIndex._
import com.wavesplatform.state.ByteStr
import com.wavesplatform.transaction.AssetId
import com.wavesplatform.transaction.assets.exchange.AssetPair
import com.wavesplatform.transaction.assets.exchange.Order.Id

class ActiveOrdersIndex(address: Address, maxElements: Int) {
  def add(rw: RW, pair: AssetPair, id: Id): Unit = {
    val newestIdx        = rw.get(newestIdxKey)
    val updatedNewestIdx = newestIdx.getOrElse(Int.MinValue) - 1

    // A new order
    rw.put(nodeKey(updatedNewestIdx), Node((pair, id), None))
    rw.put(orderIdxKey(id), Some(updatedNewestIdx))

    // A previous order in the index
    newestIdx.foreach { idx =>
      val n = rw.get(nodeKey(idx))
      rw.put(nodeKey(idx), n.copy(newerIdx = Some(updatedNewestIdx)))
    }

    rw.put(newestIdxKey, Some(updatedNewestIdx))
    rw.update(sizeKey) { orig =>
      val size = orig.getOrElse(0)
      Some(size + 1)
    }
  }

  def delete(rw: RW, id: Id): Unit = rw.get(orderIdxKey(id)).foreach { idx =>
    val nk   = nodeKey(idx)
    val node = rw.get(nk)

    val olderNode = findOlder(rw, idx)
    olderNode.foreach {
      case (olderIdx, n) => rw.put(nodeKey(olderIdx), n.copy(newerIdx = node.newerIdx))
    }

    if (node.newerIdx.isEmpty) {
      findOlder(rw, idx) match {
        case None => rw.delete(newestIdxKey)
        case x    => rw.put(newestIdxKey, x.map(_._1))
      }
    }

    rw.delete(nk)
    rw.delete(orderIdxKey(id))
    rw.get(sizeKey).getOrElse(0) - 1 match {
      case x if x <= 0 => rw.delete(sizeKey)
      case x           => rw.put(sizeKey, Some(x))
    }
  }

  def size(ro: ReadOnlyDB): Int = ro.get(sizeKey).getOrElse(0)

  def iterator(ro: ReadOnlyDB): ClosableIterable[NodeContent] =
    ro.get(newestIdxKey).fold(ClosableIterable.empty: ClosableIterable[NodeContent])(mkIterator(ro, _).map(_._2.elem))

  private def findOlder(ro: ReadOnlyDB, thanIdx: Int): Option[(Index, Node)] = {
    val iter = mkIterator(ro, latestIdx = thanIdx)
    try iter.iterator.drop(1).find(_ => true)
    finally iter.close()
  }

  private def mkIterator(ro: ReadOnlyDB, latestIdx: Int): ClosableIterable[(Index, Node)] = new ClosableIterable[(Index, Node)] {
    private val prefix   = MatcherKeys.ActiveOrdersPrefixBytes ++ address.bytes.arr
    private val internal = ro.iterator
    internal.seek(nodeKey(latestIdx).keyBytes)

    override val iterator: Iterator[(Index, Node)] = prefixIterator(internal, prefix) { e =>
      val idx = ByteBuffer.wrap(e.getKey).position(prefix.length).asInstanceOf[ByteBuffer].getInt()
      (idx, Node.read(e.getValue))
    }
    override def close(): Unit = internal.close()
  }

  private val sizeKey: Key[Option[Index]]             = MatcherKeys.activeOrdersSize(address)
  private def nodeKey(idx: Index): Key[Node]          = MatcherKeys.activeOrders(address, idx)
  private val newestIdxKey: Key[Option[Index]]        = MatcherKeys.activeOrdersSeqNr(address)
  private def orderIdxKey(id: Id): Key[Option[Index]] = MatcherKeys.activeOrderSeqNr(address, id)
}

object ActiveOrdersIndex {
  type NodeContent = (AssetPair, Id)
  type Index       = Int

  case class Node(elem: (AssetPair, Id), newerIdx: Option[Index])
  object Node {
    def read(xs: Array[Byte]): Node = {
      val bb = ByteBuffer.wrap(xs)
      Node((assetPairFromBytes(bb), orderIdFromBytes(bb)), indexFromBytes(bb))
    }

    private def indexFromBytes(bb: ByteBuffer): Option[Index] = bb.get match {
      case 0 => None
      case 1 =>
        val bytes = new Array[Byte](Ints.BYTES)
        bb.get(bytes)
        Some(Ints.fromByteArray(bytes))
    }

    private def assetPairFromBytes(bb: ByteBuffer): AssetPair = AssetPair(assetIdFromBytes(bb), assetIdFromBytes(bb))
    private def assetIdFromBytes(bb: ByteBuffer): Option[AssetId] = bb.get match {
      case 0 => None
      case 1 =>
        val bytes = new Array[Byte](crypto.DigestSize)
        bb.get(bytes)
        Some(ByteStr(bytes))
    }

    private def orderIdFromBytes(bb: ByteBuffer): Id = {
      val bytes = new Array[Byte](crypto.DigestSize)
      bb.get(bytes)
      ByteStr(bytes)
    }

    def write(x: Node): Array[Byte] = toBytes(x.elem) ++ indexToBytes(x.newerIdx)

    private def indexToBytes(x: Option[Index]): Array[Byte] = x.fold(Array[Byte](0))(x => (1: Byte) +: Ints.toByteArray(x))
    private def toBytes(x: (AssetPair, Id)): Array[Byte]    = x._1.bytes ++ x._2.arr
  }
}

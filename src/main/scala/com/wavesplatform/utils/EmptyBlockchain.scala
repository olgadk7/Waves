package com.wavesplatform.utils

import com.wavesplatform.account.{Address, Alias}
import com.wavesplatform.block.{Block, BlockHeader}
import com.wavesplatform.state.reader.LeaseDetails
import com.wavesplatform.state.{AccountDataInfo, AssetDescription, BalanceSnapshot, Blockchain, ByteStr, DataEntry, Diff, Portfolio, VolumeAndFee}
import com.wavesplatform.transaction.Transaction.Type
import com.wavesplatform.transaction.ValidationError.GenericError
import com.wavesplatform.transaction.{AssetId, Transaction, ValidationError}
import com.wavesplatform.transaction.lease.LeaseTransaction
import com.wavesplatform.transaction.smart.script.Script

object EmptyBlockchain extends Blockchain {
  override def height: Int = 0

  override def score: BigInt = 0

  override def scoreOf(blockId: ByteStr): Option[BigInt] = None

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = None

  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = None

  override def lastBlock: Option[Block] = None

  override def carryFee: Long = 0

  override def blockBytes(height: Int): Option[Array[Byte]] = None

  override def blockBytes(blockId: ByteStr): Option[Array[Byte]] = None

  override def heightOf(blockId: ByteStr): Option[Int] = None

  /** Returns the most recent block IDs, starting from the most recent  one */
  override def lastBlockIds(howMany: Int): Seq[ByteStr] = Seq.empty

  /** Returns a chain of blocks starting with the block with the given ID (from oldest to newest) */
  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = None

  override def parent(block: Block, back: Int): Option[Block] = None

  /** Features related */
  override def approvedFeatures: Map[Short, Int] = Map.empty

  override def activatedFeatures: Map[Short, Int] = Map.empty

  override def featureVotes(height: Int): Map[Short, Int] = Map.empty

  override def portfolio(a: Address): Portfolio = Portfolio.empty

  override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] = None

  override def transactionHeight(id: ByteStr): Option[Int] = None

  override def addressTransactions(address: Address, types: Set[Type], count: Int, fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] =
    Right(Seq.empty)

  override def containsTransaction(id: ByteStr): Boolean = false

  override def forgetTransactions(pred: (ByteStr, Long) => Boolean): Map[ByteStr, Long] = Map.empty

  override def learnTransactions(values: Map[ByteStr, Long]): Unit = ()

  override def assetDescription(id: ByteStr): Option[AssetDescription] = None

  override def resolveAlias(a: Alias): Either[ValidationError, Address] = Left(GenericError("Empty blockchain"))

  override def leaseDetails(leaseId: ByteStr): Option[LeaseDetails] = None

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee = VolumeAndFee(0, 0)

  /** Retrieves Waves balance snapshot in the [from, to] range (inclusive) */
  override def balanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = Seq.empty

  override def accountScript(address: Address): Option[Script] = None

  override def hasScript(address: Address): Boolean = false

  override def assetScript(asset: AssetId): Option[Script] = None

  override def hasAssetScript(asset: AssetId): Boolean = false

  override def accountData(acc: Address): AccountDataInfo = AccountDataInfo(Map.empty)

  override def accountData(acc: Address, key: String): Option[DataEntry[_]] = None

  override def balance(address: Address, mayBeAssetId: Option[AssetId]): Long = 0

  override def assetDistribution(assetId: ByteStr): Map[Address, Long] = Map.empty

  override def wavesDistribution(height: Int): Map[Address, Long] = Map.empty

  override def allActiveLeases: Set[LeaseTransaction] = Set.empty

  override def assetDistributionAtHeight(assetId: AssetId, height: Int): Map[Address, Long] = Map.empty

  /** Builds a new portfolio map by applying a partial function to all portfolios on which the function is defined.
    *
    * @note Portfolios passed to `pf` only contain Waves and Leasing balances to improve performance */
  override def collectLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A] = Map.empty
  override def append(diff: Diff, carryFee: Long, block: Block): Unit                                  = ()
  override def rollbackTo(targetBlockId: ByteStr): Seq[Block]                                          = Seq.empty
}

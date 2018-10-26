package com.wavesplatform.it.sync.matcher

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory.parseString
import com.wavesplatform.it.ReportingTestName
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.api.SyncMatcherHttpApi._
import com.wavesplatform.it.sync._
import com.wavesplatform.it.sync.matcher.config.MatcherDefaultConfig._
import com.wavesplatform.it.transactions.NodesFromDocker
import com.wavesplatform.it.util._
import com.wavesplatform.state.ByteStr.decodeBase58
import com.wavesplatform.transaction.assets.exchange.OrderType._
import com.wavesplatform.transaction.assets.exchange.AssetPair
import org.scalatest.{BeforeAndAfterAll, CancelAfterFailure, FreeSpec, Matchers}

class BlacklistedTradingTestSuite
    extends FreeSpec
    with Matchers
    with BeforeAndAfterAll
    with CancelAfterFailure
    with NodesFromDocker
    with ReportingTestName {

  import BlacklistedTradingTestSuite._
  override protected def nodeConfigs: Seq[Config] = Configs.map(configWithForbidden().withFallback(_))

  private def matcher = dockerNodes().head
  private def alice   = dockerNodes()(1)
  private def bob     = dockerNodes()(2)

  "Trading with blacklisted assets" in {
    val golovaAsset = alice
      .issue(alice.address, "GolovaCoin", "Test", someAssetAmount, 0, reissuable = false, 100000000L)
      .id
    nodes.waitForHeightAriseAndTxPresent(golovaAsset)

    val zhopaAsset = bob
      .issue(bob.address, "ZhopaCoin", "Test", someAssetAmount, 0, reissuable = false, 100000000L)
      .id
    nodes.waitForHeightAriseAndTxPresent(zhopaAsset)

    val golovaPair = AssetPair(decodeBase58(golovaAsset).toOption, None)
    val zhopaPair  = AssetPair(decodeBase58(zhopaAsset).toOption, None)

    val correctOrder   = matcher.placeOrder(alice, golovaPair, SELL, 10, 15.waves).message.id
    val incorrectOrder = matcher.placeOrder(bob, zhopaPair, SELL, 10, 15.waves).message.id

    matcher.waitOrderStatus(zhopaPair, incorrectOrder, "Accepted")

    docker.restartNodeWithNewConfig(matcher, configWithForbidden(assets = Array("ZhopaCoin")))
//    docker.restartContainer(matcher)

    matcher.orderStatus(correctOrder, golovaPair).status shouldBe "Accepted"
    val incorrectOrder2         = matcher.prepareOrder(bob, zhopaPair, SELL, 21, 15.waves)
    matcher.expectIncorrectOrderPlacement(incorrectOrder2, 400, "OrderRejected")
    matcher.orderStatus(incorrectOrder, zhopaPair).status shouldBe "Cancelled"
  }

}

object BlacklistedTradingTestSuite {

  def configWithForbidden(assets: Array[String] = Array(), names: Array[String] = Array(), addresses: Array[String] = Array()): Config = {
    def toStr(array: Array[String]): String = if (array.length == 0) "" else array.mkString("\"", "\", \"", "\"")
    parseString(s"""
                |waves.matcher {
                |  blacklisted-assets = [${toStr(assets)}]
                |  blacklisted-names = [${toStr(names)}]
                |  blacklisted-addresses = [${toStr(addresses)}]
                |}
    """.stripMargin)
  }

}

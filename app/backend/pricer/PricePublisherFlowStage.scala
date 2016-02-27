package backend.pricer

import java.util.Random
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend._
import backend.PricerMsg._
import backend.shared.Currencies
import backend.utils.{Metrics, SimpleThroughputTracker}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.language.postfixOps


private object PriceGenerator {
  private val basePrice: Map[String, Int] = Map(
    "AUD/CAD" -> 98780, "AUD/CHF" -> 70590, "AUD/NZD" -> 106840, "AUD/USD" -> 71820, "CAD/CHF" -> 71400,
    "EUR/GBP" -> 72214, "EUR/CHF" -> 108080, "EUR/USD" -> 109920, "GBP/AUD" -> 211760, "GBP/CAD" -> 209390,
    "GBP/CHF" -> 149080, "GBP/USD" -> 152160, "USD/CAD" -> 137540, "USD/CHF" -> 98300, "NZD/USD" -> 67090
  )

  private val generator = new Random()

  def generateFor(id: Int) = {
    val rnd = generator.nextGaussian() * 15 match {
      case i if Math.abs(i) > 20 => i / 3
      case i => i
    }
    (basePrice.get(Currencies.all(id)).get + rnd).toInt
  }
}


private object PricePublisherFlowStage {
  def apply(serverId: Int)(implicit sys: ActorSystem) = Flow.fromGraph(new PricePublisherFlowStage(serverId))
}

private class PricePublisherFlowStage(serverId: Int)(implicit sys: ActorSystem) extends GraphStage[FlowShape[PricerMsg, PricerMsg]] with StrictLogging {
  val in: Inlet[PricerMsg] = Inlet("Incoming")
  val out: Outlet[PricerMsg] = Outlet("Outgoing")

  override val shape: FlowShape[PricerMsg, PricerMsg] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with Metrics with SimpleThroughputTracker {

    case object TokensTimer

    case object PingTimer

    private[this] val latency = metrics.timer("latency")
    metrics.gauge(s"pricer.$serverId.msgOut")(calculateThroughputAndReset())

    val TokenReplenishInterval = 200 millis
    val PingInterval = 1 second
    val UpdatesPerCcyPerSecond = sys.settings.config.getInt("pricer.updates-per-ccy-per-sec")
    val TokensForEachCcy = UpdatesPerCcyPerSecond / (1000 / TokenReplenishInterval.toMillis)

    var tokens = 0
    var lastTokensIssuedAt = 0L

    var subscribedCurrencies: Array[Short] = Array()
    var subscriptionIdx = 0
    var sendPing = false

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        grab(in) match {
          case StreamRequest(id) => startPublishingPricesFor(id)
          case StreamCancel(id) => stopPublishingPricesFor(id)
          case KillServerRequest() => sys.shutdown()
          case Pong(id) =>
            val diff = ((System.nanoTime() % Int.MaxValue) - id) / 2
            if (diff > 0 && diff < 60L * 1000 * 1000000) latency.update(diff, TimeUnit.NANOSECONDS)

        }
        pull(in)
      }
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = publishNext()
    })

    override def postStop(): Unit = {
      logger.info(s"Server $serverId stopped")
    }

    override def preStart(): Unit = {
      schedulePeriodically(TokensTimer, TokenReplenishInterval)
      schedulePeriodicallyWithInitialDelay(PingTimer, 5 seconds, PingInterval)
      pull(in)
    }

    override protected def onTimer(timerKey: Any): Unit = timerKey match {
      case TokensTimer =>
        val now = System.currentTimeMillis()
        val elapsedInterval = if (lastTokensIssuedAt == 0) TokenReplenishInterval.toMillis else now - lastTokensIssuedAt
        tokens = (TokensForEachCcy * subscribedCurrencies.length * elapsedInterval / TokenReplenishInterval.toMillis).toInt
        lastTokensIssuedAt = now
        publishNext()
      case PingTimer => sendPing = true
    }

    def publishNext(): Unit = if (isAvailable(out) && tokens > 0) {
      if (sendPing) {
        pushAndConsumeToken(Ping((System.nanoTime() % Int.MaxValue).toInt))
        sendPing = false
      } else if (haveSubscriptions) {
        val ccyId = nextSubscribedCcyId()
        pushAndConsumeToken(PriceUpdate(ccyId, PriceGenerator.generateFor(ccyId.toInt), serverId.toByte))
      }
    }

    def pushAndConsumeToken(m: PricerMsg) = {
      push(out, m)
      tokens -= 1
      updateThroughput(1)
    }

    def startPublishingPricesFor(id: Short) = if (!subscribedCurrencies.contains(id)) subscribedCurrencies = subscribedCurrencies :+ id

    def stopPublishingPricesFor(id: Short) = subscribedCurrencies = subscribedCurrencies filterNot (_ == id)

    def nextSubscribedCcyId() = {
      subscriptionIdx += 1
      if (subscriptionIdx >= subscribedCurrencies.length) subscriptionIdx = 0
      subscribedCurrencies(subscriptionIdx)
    }

    def haveSubscriptions = subscribedCurrencies.length > 0

  }

}



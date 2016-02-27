package backend.distributor

import akka.stream._
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.stream.stage._
import backend.PricerMsg

import scala.concurrent.duration._
import scala.language.postfixOps

object ShapingStage {

  def apply(msgSec: Int) = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val in = b.add(Flow[PricerMsg])
    val out = b.add(Flow.fromGraph(new SimpleThrottledFlow(msgSec)))
    BidiShape.fromFlows(in, out)
  })

}

/**
  * Basic traffic shaping stage
  */
private class SimpleThrottledFlow(msgSec: Int) extends GraphStage[FlowShape[PricerMsg, PricerMsg]] {
  val in: Inlet[PricerMsg] = Inlet("Inbound")
  val out: Outlet[PricerMsg] = Outlet("ShapedOutbound")

  override val shape: FlowShape[PricerMsg, PricerMsg] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    case object TokensTimer

    val TokenReplenishInterval = 200 millis
    val TokensCount = msgSec / (1000 / TokenReplenishInterval.toMillis)

    var tokens = 0
    var lastTokensIssuedAt = 0L

    var maybeNext: Option[PricerMsg] = None

    override def preStart(): Unit = {
      schedulePeriodically(TokensTimer, TokenReplenishInterval)
      pull(in)
    }

    override protected def onTimer(timerKey: Any): Unit = {
      val now = System.currentTimeMillis()
      val elapsedTime = if (lastTokensIssuedAt == 0) TokenReplenishInterval.toMillis else now - lastTokensIssuedAt
      tokens = (elapsedTime * TokenReplenishInterval.toMillis / TokensCount).toInt
      lastTokensIssuedAt = now
      forwardThrottled()
    }

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        maybeNext = Some(grab(in))
        forwardThrottled()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = forwardThrottled()
    })

    def forwardThrottled() = if (isAvailable(out)) maybeNext foreach { next =>
      if (tokens > 0) {
        pull(in)
        push(out, next)
        maybeNext = None
        if (tokens > 0) tokens -= 1
      }
    }

  }

}

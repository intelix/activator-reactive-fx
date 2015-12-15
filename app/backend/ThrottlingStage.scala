package backend

import akka.stream._
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.stream.stage._

import scala.concurrent.duration._
import scala.language.postfixOps

object ThrottlingStage {

  def apply(msgSec: Int) = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val in = b.add(Flow[ApplicationMessage])
    val out = b.add(Flow.fromGraph(new SimpleThrottledFlow(msgSec)))
    BidiShape.fromFlows(in, out)
  })

}

private class SimpleThrottledFlow(msgSec: Int) extends GraphStage[FlowShape[ApplicationMessage, ApplicationMessage]] {
  val in: Inlet[ApplicationMessage] = Inlet("Inbound")
  val out: Outlet[ApplicationMessage] = Outlet("ThrottledOut")

  override val shape: FlowShape[ApplicationMessage, ApplicationMessage] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    case object TokensTimer

    val TokenReplenishInterval = 200 millis
    val TokensCount = msgSec / (1000 / TokenReplenishInterval.toMillis)

    var tokens = 0
    var lastTokensIssuedAt = 0L

    var maybeNext: Option[ApplicationMessage] = None

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
      if (tokens > 0 || next.isInstanceOf[PriorityMessage]) {
        pull(in)
        push(out, next)
        maybeNext = None
        if (tokens > 0) tokens -= 1
      }
    }

  }

}

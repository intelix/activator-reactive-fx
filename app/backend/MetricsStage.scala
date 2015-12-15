package backend

import akka.stream.BidiShape
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.stream.stage.{Context, PushStage, SyncDirective}
import backend.utils.{Metrics, SimpleThroughputTracker}

object MetricsStage {

  def apply(connectionId: Int) = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val metricsStage = new PushStage[ApplicationMessage, ApplicationMessage] with Metrics with SimpleThroughputTracker {
      private[this] val gauge = metrics.gauge(s"websocket.$connectionId.msgOut")(calculateThroughputAndReset())

      override def onPush(elem: ApplicationMessage, ctx: Context[ApplicationMessage]): SyncDirective = {
        updateThroughput(1)
        ctx.push(elem)
      }
    }
    val out = b.add(Flow[ApplicationMessage].transform(() => metricsStage))
    val in = b.add(Flow[ApplicationMessage])
    BidiShape.fromFlows(in, out)
  })

}

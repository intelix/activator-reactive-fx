package backend.distributor

import akka.stream.BidiShape
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.stream.stage.{Context, PushStage, SyncDirective}
import backend.PricerMsg
import backend.utils.{Metrics, SimpleThroughputTracker}

/**
  * Calculates throughput stats
  */
object MetricsStage {

  def apply(connectionId: Int) = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val metricsStage = new PushStage[PricerMsg, PricerMsg] with Metrics with SimpleThroughputTracker {
      private[this] val gauge = metrics.gauge(s"websocket.$connectionId.msgOut")(calculateThroughputAndReset())

      override def onPush(elem: PricerMsg, ctx: Context[PricerMsg]): SyncDirective = {
        updateThroughput(1)
        ctx.push(elem)
      }
    }
    val out = b.add(Flow[PricerMsg].transform(() => metricsStage))
    val in = b.add(Flow[PricerMsg])
    BidiShape.fromFlows(in, out)
  })

}

package backend.distributor

import akka.stream.BidiShape
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.stream.stage.{Context, PushStage, SyncDirective}
import backend.PricerApi
import backend.utils.{Metrics, SimpleThroughputTracker}

object MetricsStage {

  def apply(connectionId: Int) = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val metricsStage = new PushStage[PricerApi, PricerApi] with Metrics with SimpleThroughputTracker {
      private[this] val gauge = metrics.gauge(s"websocket.$connectionId.msgOut")(calculateThroughputAndReset())

      override def onPush(elem: PricerApi, ctx: Context[PricerApi]): SyncDirective = {
        updateThroughput(1)
        ctx.push(elem)
      }
    }
    val out = b.add(Flow[PricerApi].transform(() => metricsStage))
    val in = b.add(Flow[PricerApi])
    BidiShape.fromFlows(in, out)
  })

}

package backend.shared

import akka.stream.BidiShape
import akka.stream.io.Framing
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.util.ByteString

object FramingStage {
  def apply() = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val delimiter = ByteString("\n")
    val in = b.add(Framing.delimiter(delimiter, 256, allowTruncation = false))
    val out = b.add(Flow[ByteString].map(_ ++ delimiter))
    BidiShape.fromFlows(in, out)
  })
}

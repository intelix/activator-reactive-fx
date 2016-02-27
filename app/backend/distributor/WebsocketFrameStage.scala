package backend.distributor

import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.BidiShape
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.util.ByteString

/**
  * Basic websocket framing stage
  */
object WebsocketFrameStage {
  def apply() = BidiFlow.fromGraph[Message, ByteString, ByteString, Message, Unit](FlowGraph.create() { b =>
    val in = b.add(Flow[Message].map {
      case t: TextMessage.Strict =>
        ByteString(t.text)
      case t: TextMessage =>
        throw new UnsupportedOperationException("Streamed payloads are not supported")
      case _ => ByteString.empty
    })
    val out = b.add(Flow[ByteString].map { b => TextMessage.Strict(b.utf8String) })
    BidiShape.fromFlows(in, out)
  })
}

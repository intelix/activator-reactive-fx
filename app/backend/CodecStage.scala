package backend

import akka.stream.BidiShape
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph}
import akka.util.ByteString

object CodecStage {
  def apply() = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val in = b.add(Flow[ByteString].map(fromDSBytes))
    val out = b.add(Flow[ApplicationMessage].map(toDSBytes))
    BidiShape.fromFlows(in, out)
  })

  private def toDSBytes(msg: ApplicationMessage): ByteString = msg match {
    case StreamRequest(id) => ByteString("r:" + id)
    case StreamCancel(id) => ByteString("c:" + id)
    case PriceUpdate(id, a, sId) => ByteString("u:" + id + ":" + a + ":" + sId)
    case Pong(id) => ByteString("o:" + id)
    case Ping(id) => ByteString("p:" + id)
    case KillServerRequest() => ByteString("k")
  }

  private def fromDSBytes(bytes: ByteString): ApplicationMessage = {
    val s = bytes.utf8String.trim
    s.charAt(0) match {
      case 'k' =>
        KillServerRequest()
      case 'r' =>
        StreamRequest(s.substring(2).toShort)
      case 'p' =>
        Ping(s.substring(2).toInt)
      case 'o' =>
        Pong(s.substring(2).toInt)
      case 'c' => StreamCancel(s.substring(2).toShort)
      case 'u' => s.split(":") match {
        case Array(_, id, a, sId) => PriceUpdate(id.toShort, a.toInt, sId.toByte)
        case el => println(s"!>>>> Oops: $el "); throw new RuntimeException(s)
      }
      case _ => println(s"!>>>> Unrecognised: $s "); throw new RuntimeException(s)
    }
  }


}

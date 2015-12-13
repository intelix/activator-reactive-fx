package server

import java.nio.ByteOrder

import akka.actor.ActorRef
import akka.stream.BidiShape
import akka.stream.io.Framing
import akka.stream.scaladsl.{Flow, FlowGraph, BidiFlow}
import akka.util.ByteString

object StreamsShared {



  case class ClientLink(ref: ActorRef)

  case class DataSourceLink(ref: ActorRef)

  case class Demand(ref: ActorRef)

  case class RequestWithRef(ref: ActorRef, req: DSDialect)

  trait DSDialect

  trait DatasourceOut extends DSDialect

  trait DatasourceIn extends DSDialect

  case class SharedStreamRequest(cId: Short) extends DatasourceOut

  case class PongResponse(id: Int) extends DatasourceOut

  case class SharedStreamCancel(cId: Short) extends DatasourceOut
  case class ControlKillServer() extends DatasourceOut




  case class SharedStreamUpdate(ccyPairId: Short, price: Int) extends DatasourceIn
  case class PingRequest(id: Int) extends DatasourceIn


  def toDSBytes(msg: DSDialect): ByteString = msg match {
    case SharedStreamRequest(id) => ByteString("r:" + id)
    case SharedStreamCancel(id) => ByteString("c:" + id)
    case SharedStreamUpdate(id, a) => ByteString("u:" + id + ":" + a )
    case PongResponse(id) => ByteString("o:" + id)
    case PingRequest(id) => ByteString("p:" + id)
    case ControlKillServer() => ByteString("k")
  }

  def fromDSBytes(bytes: ByteString): DSDialect = {
    val s = bytes.utf8String.trim
    s.charAt(0) match {
      case 'k' =>
        ControlKillServer()
      case 'r' =>
        SharedStreamRequest(s.substring(2).toShort)
      case 'p' =>
        PingRequest(s.substring(2).toInt)
      case 'o' =>
        PongResponse(s.substring(2).toInt)
      case 'c' => SharedStreamCancel(s.substring(2).toShort)
      case 'u' => s.split(":") match {
        case Array(_, id, a) => SharedStreamUpdate(id.toShort, a.toInt)
        case el => println(s"!>>>> Oops: $el "); throw new RuntimeException(s)
      }
      case _ => println(s"!>>>> Unrecognised: $s "); throw new RuntimeException(s)
    }
  }

  def dsCodec = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val out = b.add(Flow[DSDialect].map(toDSBytes))
    val in = b.add(Flow[ByteString].map(fromDSBytes))
    BidiShape.fromFlows(in, out)
  })

  def dsFraming = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val delimeter = ByteString("\n")
    val out = b.add(Flow[ByteString].map(_ ++ delimeter))
    val in = b.add(Framing.delimiter(delimeter, 256, allowTruncation = false))
    BidiShape.fromFlows(in, out)
  })

  def dsLogging = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val out = b.add(Flow[ByteString].map { x => println(s"!>>> out: ${x.utf8String} [$x]"); x })
    val in = b.add(Flow[ByteString].map { x => println(s"!>>> in: ${x.utf8String} [$x]"); x })
    BidiShape.fromFlows(in, out)
  })


}

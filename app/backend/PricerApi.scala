package backend

trait PricerApi

object PricerApi {

  trait ServerToClient extends PricerApi

  trait ClientToServer extends PricerApi

  case class StreamRequest(cId: Short) extends ServerToClient

  case class Pong(id: Int) extends ServerToClient

  case class StreamCancel(cId: Short) extends ServerToClient

  case class KillServerRequest() extends ServerToClient


  case class PriceUpdate(ccyPairId: Short, price: Int, sourceId: Byte) extends ClientToServer

  case class Ping(id: Int) extends ClientToServer

}
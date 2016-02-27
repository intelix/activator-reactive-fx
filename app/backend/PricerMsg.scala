package backend

/**
  * Pricer dialect.
  */
trait PricerMsg

object PricerMsg {

  trait ServerToClient extends PricerMsg

  trait ClientToServer extends PricerMsg

  case class StreamRequest(cId: Short) extends ServerToClient

  case class Pong(id: Int) extends ServerToClient

  case class StreamCancel(cId: Short) extends ServerToClient

  case class KillServerRequest() extends ServerToClient


  case class PriceUpdate(ccyPairId: Short, price: Int, sourceId: Byte) extends ClientToServer

  case class Ping(id: Int) extends ClientToServer

}
package backend

trait ApplicationMessage

trait ServerToClient extends ApplicationMessage

trait ClientToServer extends ApplicationMessage

case class StreamRequest(cId: Short) extends ServerToClient

case class Pong(id: Int) extends ServerToClient

case class StreamCancel(cId: Short) extends ServerToClient

case class KillServerRequest() extends ServerToClient


case class PriceUpdate(ccyPairId: Short, price: Int, sourceId: Byte) extends ClientToServer

case class Ping(id: Int) extends ClientToServer

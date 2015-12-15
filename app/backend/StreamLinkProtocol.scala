package backend

import akka.actor.ActorRef

object StreamLinkProtocol {

  case class WebsocketStreamRef(ref: ActorRef)

  case class DatasourceStreamRef(ref: ActorRef)

  case class Demand(sender: ActorRef)

  case class Payload(sender: ActorRef, msg: ApplicationMessage)

}

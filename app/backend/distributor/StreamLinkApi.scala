package backend.distributor

import akka.actor.ActorRef
import backend.PricerMsg

/**
  * Internal API for stream linking
  */
object StreamLinkApi {

  case class DistributorStreamRef(ref: ActorRef)

  case class PricerStreamRef(ref: ActorRef)

  case class Demand(sender: ActorRef)

  case class Payload(sender: ActorRef, msg: PricerMsg)

}

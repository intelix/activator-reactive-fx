package backend.distributor

import akka.actor._
import StreamLinkApi.{PricerStreamRef, DistributorStreamRef}

object StreamRegistry {
  private val id = "registry"
  private val selectionPath = s"/user/$id"

  def selection(implicit ctx: ActorRefFactory) = ctx.actorSelection(selectionPath)

  def start()(implicit sys: ActorSystem) = sys.actorOf(Props[StreamRegistry], id)
}

class StreamRegistry extends Actor {

  private var clients: List[DistributorStreamRef] = List()
  private var pricer: Option[ActorRef] = None

  override def receive: Actor.Receive = {
    case m: DistributorStreamRef =>
      context.watch(m.ref)
      clients :+= m
      pricer foreach (_ ! m)
    case PricerStreamRef(ref) =>
      pricer = Some(context.watch(ref))
      clients foreach (ref ! _)
    case Terminated(ref) if pricer.contains(ref) =>
      pricer = None
    case Terminated(ref) =>
      clients = clients filterNot (_.ref == ref)
  }
}

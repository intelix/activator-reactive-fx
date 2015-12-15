package backend

import akka.actor._
import backend.StreamLinkProtocol.{DatasourceStreamRef, WebsocketStreamRef}

object StreamRegistry {
  private val id = "registry"
  private val selectionPath = s"/user/$id"

  def selection(implicit ctx: ActorRefFactory) = ctx.actorSelection(selectionPath)

  def start()(implicit sys: ActorSystem) = sys.actorOf(Props[StreamRegistry], id)
}

class StreamRegistry extends Actor {

  private var clients: List[WebsocketStreamRef] = List()
  private var dataSource: Option[ActorRef] = None

  override def receive: Actor.Receive = {
    case m: WebsocketStreamRef =>
      context.watch(m.ref)
      clients :+= m
      dataSource foreach (_ ! m)
    case DatasourceStreamRef(ref) =>
      dataSource = Some(context.watch(ref))
      clients foreach (ref ! _)
    case Terminated(ref) if dataSource.contains(ref) =>
      dataSource = None
    case Terminated(ref) =>
      clients = clients filterNot (_.ref == ref)
  }
}

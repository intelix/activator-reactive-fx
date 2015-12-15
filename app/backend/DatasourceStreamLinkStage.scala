package backend

import akka.actor.{ActorRef, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend.StreamLinkProtocol.{DatasourceStreamRef, Demand, Payload, WebsocketStreamRef}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps

object DatasourceStreamLinkStage {
  def apply(parentRef: ActorRef) = Flow.fromGraph(new DatasourceStreamLinkStage(parentRef))
}

private class DatasourceStreamLinkStage(monitorRef: ActorRef) extends GraphStage[FlowShape[ApplicationMessage, ApplicationMessage]] {

  val in: Inlet[ApplicationMessage] = Inlet("ClientBound")
  val out: Outlet[ApplicationMessage] = Outlet("DatasourceBound")

  override val shape: FlowShape[ApplicationMessage, ApplicationMessage] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with StrictLogging {

    case object TimerKey

    lazy val self = getStageActorRef(onMessage)
    var pendingToDatasource: Queue[StreamHead] = Queue()
    var activeWebsocketStreams: Set[ActorRef] = Set()
    var openSubscriptions: Map[Short, List[ActorRef]] = Map()

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        grab(in) match {
          case m@PriceUpdate(cId, _, _) => openSubscriptions get cId foreach (_.foreach(_ ! m))
          case m: Ping => activeWebsocketStreams foreach (_ ! m)
        }
        pull(in)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pushToDatasource()
    })

    override def preStart(): Unit = {
      monitorRef ! DatasourceStreamRef(self)
      pull(in)
    }

    override protected def onTimer(timerKey: Any): Unit = {
      openSubscriptions = openSubscriptions filter {
        case (cId, list) if list.isEmpty =>
          pendingToDatasource = StreamHead(None, StreamCancel(cId)) +: pendingToDatasource
          false
        case _ => true
      }
      pushToDatasource()
      if (openSubscriptions.isEmpty) cancelTimer(TimerKey)
    }

    private def onMessage(x: (ActorRef, Any)): Unit = x match {
      case (_, WebsocketStreamRef(ref)) =>
        logger.info(s"Linked with websocket stream at $ref")
        activeWebsocketStreams += ref
        self.watch(ref)
        ref ! Demand(self)
      case (_, Terminated(ref)) =>
        logger.info(s"Broken link with $ref")
        pendingToDatasource = pendingToDatasource.filterNot(_.maybeRef.contains(ref))
        activeWebsocketStreams -= ref
        openSubscriptions = openSubscriptions map {
          case (cId, subscribers) if subscribers.contains(ref) => cId -> subscribers.filterNot(_ == ref)
          case other => other
        }
      case (_, Payload(ref, m@StreamRequest(cId))) =>
        val ccy = Currencies.all(cId.toInt)
        logger.info(s"Subscription request for $ccy")
        if (!openSubscriptions.contains(cId)) {
          logger.info(s"Opening datasource subscription for $ccy")
          pendingToDatasource = pendingToDatasource :+ StreamHead(Some(ref), m)
          pushToDatasource()
          if (openSubscriptions.isEmpty) schedulePeriodicallyWithInitialDelay(TimerKey, 5 seconds, 5 seconds)
        } else {
          ref ! Demand(self)
          logger.info(s"Sharing $ccy stream, total ${openSubscriptions.get(cId).get.size + 1} subscribers")
        }
        if (!openSubscriptions.get(cId).exists(_.contains(ref)))
          openSubscriptions += cId -> (openSubscriptions.getOrElse(cId, List()) :+ ref)
      case (_, Payload(ref, m: ServerToClient)) =>
        pendingToDatasource = StreamHead(Some(ref), m) +: pendingToDatasource
        pushToDatasource()
      case (_, el) => logger.warn(s"Unexpected: $el")
    }

    private def pushToDatasource() = if (isAvailable(out) && pendingToDatasource.nonEmpty)
      pendingToDatasource.dequeue match {
        case (StreamHead(ref, el), queue) =>
          push(out, el)
          pendingToDatasource = queue
          ref foreach (_ ! Demand(self))
      }

  }

  case class StreamHead(maybeRef: Option[ActorRef], element: ApplicationMessage)

}



package backend.distributor

import akka.actor.{ActorRef, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend._
import backend.PricerMsg._
import backend.distributor.StreamLinkApi.{Payload, Demand, DistributorStreamRef, PricerStreamRef}
import backend.shared.Currencies
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps

object PricerStreamEndpointStage {
  def apply(parentRef: ActorRef) = Flow.fromGraph(new PricerStreamEndpointStage(parentRef))
}

private class PricerStreamEndpointStage(monitorRef: ActorRef) extends GraphStage[FlowShape[PricerMsg, PricerMsg]] {

  val in: Inlet[PricerMsg] = Inlet("ClientBound")
  val out: Outlet[PricerMsg] = Outlet("PricerBound")

  override val shape: FlowShape[PricerMsg, PricerMsg] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with StrictLogging {

    case object TimerKey

    lazy val self = getStageActorRef(onMessage)
    var pendingToPricer: Queue[StreamHead] = Queue()
    var activeDistributorStreams: Set[ActorRef] = Set()
    var openSubscriptions: Map[Short, List[ActorRef]] = Map()

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        grab(in) match {
          case m@PriceUpdate(cId, _, _) => openSubscriptions get cId foreach (_.foreach(_ ! m))
          case m: Ping => activeDistributorStreams foreach (_ ! m)
        }
        pull(in)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pushToPricer()
    })

    override def preStart(): Unit = {
      monitorRef ! PricerStreamRef(self)
      pull(in)
    }

    override protected def onTimer(timerKey: Any): Unit = {
      openSubscriptions = openSubscriptions filter {
        case (cId, list) if list.isEmpty =>
          pendingToPricer = pendingToPricer :+ StreamHead(None, StreamCancel(cId))
          false
        case _ => true
      }
      pushToPricer()
      if (openSubscriptions.isEmpty) cancelTimer(TimerKey)
    }

    private def onMessage(x: (ActorRef, Any)): Unit = x match {
      case (_, DistributorStreamRef(ref)) =>
        logger.info(s"Linked with distributor stream at $ref")
        activeDistributorStreams += ref
        self.watch(ref)
        ref ! Demand(self)
      case (_, Terminated(ref)) =>
        logger.info(s"Broken link with $ref")
        pendingToPricer = pendingToPricer.filterNot(_.maybeRef.contains(ref))
        activeDistributorStreams -= ref
        openSubscriptions = openSubscriptions map {
          case (cId, subscribers) if subscribers.contains(ref) => cId -> subscribers.filterNot(_ == ref)
          case other => other
        }
      case (_, Payload(ref, m@StreamRequest(cId))) =>
        val ccy = Currencies.all(cId.toInt)
        logger.info(s"Subscription request for $ccy")
        if (!openSubscriptions.contains(cId)) {
          logger.info(s"Opening pricer subscription for $ccy")
          pendingToPricer = pendingToPricer :+ StreamHead(Some(ref), m)
          pushToPricer()
          if (openSubscriptions.isEmpty) schedulePeriodicallyWithInitialDelay(TimerKey, 5 seconds, 5 seconds)
        } else {
          ref ! Demand(self)
          logger.info(s"Sharing $ccy stream, total ${openSubscriptions.get(cId).get.size + 1} subscribers")
        }
        if (!openSubscriptions.get(cId).exists(_.contains(ref)))
          openSubscriptions += cId -> (openSubscriptions.getOrElse(cId, List()) :+ ref)
      case (_, Payload(ref, m: ServerToClient)) =>
        pendingToPricer = StreamHead(Some(ref), m) +: pendingToPricer
        pushToPricer()
      case (_, el) => logger.warn(s"Unexpected: $el")
    }

    private def pushToPricer() = if (isAvailable(out) && pendingToPricer.nonEmpty)
      pendingToPricer.dequeue match {
        case (StreamHead(ref, el), queue) =>
          push(out, el)
          pendingToPricer = queue
          ref foreach (_ ! Demand(self))
      }

  }

  case class StreamHead(maybeRef: Option[ActorRef], element: PricerMsg)

}



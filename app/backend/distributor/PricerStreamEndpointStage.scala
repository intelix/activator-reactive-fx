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

/**
  * This is the last stage of the pricer stream. This stage links with the distributor stream to form end-to-end flow.
  */
private class PricerStreamEndpointStage(monitorRef: ActorRef) extends GraphStage[FlowShape[PricerMsg, PricerMsg]] {

  val in: Inlet[PricerMsg] = Inlet("ClientBound")
  val out: Outlet[PricerMsg] = Outlet("PricerBound")

  override val shape: FlowShape[PricerMsg, PricerMsg] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with StrictLogging {

    case object TimerKey

    lazy val self = getStageActorRef(onMessage)                       // available only when stage is started
    var pendingToPricer: Queue[StreamHead] = Queue()                  // an element pending to Pricer
    var activeDistributorStreams: Set[ActorRef] = Set()               // set of active distributor streams (one for each connected client)
    var openSubscriptions: Map[Short, List[ActorRef]] = Map()         // list of all open unique subscriptions with Pricer

    setHandler(in, new InHandler {
      // this method is called every time next element is available for consumption
      override def onPush(): Unit = {
        grab(in) match {
          case m@PriceUpdate(cId, _, _) =>
            openSubscriptions get cId foreach (_.foreach(_ ! m))      // forward price update to all streams subscribed to that id
          case m: Ping => activeDistributorStreams foreach (_ ! m)    // forward to all live streams
        }
        pull(in)                                                      // pull next element
      }
    })

    setHandler(out, new OutHandler {
      // this method is called every time downstream is ready for the next element
      override def onPull(): Unit = pushToPricer()
    })

    override def preStart(): Unit = {
      monitorRef ! PricerStreamRef(self)                              // publish location of the endpoint
      pull(in)                                                        // pull the first element
    }

    override protected def onTimer(timerKey: Any): Unit = {
      openSubscriptions = openSubscriptions filter {                  // subscription maintenance routine...
        case (cId, list) if list.isEmpty =>
          pendingToPricer = pendingToPricer :+ StreamHead(None, StreamCancel(cId))  // unsubscribe if no interested parties
          false
        case _ => true
      }
      pushToPricer()                                                  // push cancels if possible
      if (openSubscriptions.isEmpty) cancelTimer(TimerKey)            // cancel timer if no active subscriptions
    }

    private def onMessage(x: (ActorRef, Any)): Unit = x match {
      case (_, DistributorStreamRef(ref)) =>                          // location of the distibutor stream ..
        logger.info(s"Linked with distributor stream at $ref")
        activeDistributorStreams += ref                               // add to the list ..
        self.watch(ref)                                               // and watch it
        ref ! Demand(self)                                            // kickoff the flow with a demand request
      case (_, Terminated(ref)) =>                                    // distributor stream terminated ..
        logger.info(s"Broken link with $ref")
        pendingToPricer = pendingToPricer.filterNot(_.maybeRef.contains(ref)) // remove all related requests
        activeDistributorStreams -= ref
        openSubscriptions = openSubscriptions map {                   // remove from the open subscriptions lists
          case (cId, subscribers) if subscribers.contains(ref) => cId -> subscribers.filterNot(_ == ref)
          case other => other
        }
      case (_, Payload(ref, m@StreamRequest(cId))) =>                 // subscription request
        val ccy = Currencies.all(cId.toInt)
        logger.info(s"Subscription request for $ccy")
        if (!openSubscriptions.contains(cId)) {                       // no subscription with pricer yet?
          logger.info(s"Opening pricer subscription for $ccy")
          pendingToPricer = pendingToPricer :+ StreamHead(Some(ref), m)
          pushToPricer()
          if (openSubscriptions.isEmpty) schedulePeriodicallyWithInitialDelay(TimerKey, 5 seconds, 5 seconds)
        } else {
          ref ! Demand(self)                                          // request next element
          logger.info(s"Sharing $ccy stream, total ${openSubscriptions.get(cId).get.size + 1} subscribers")
        }
        if (!openSubscriptions.get(cId).exists(_.contains(ref)))      // keep track of stream interest
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



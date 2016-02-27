package backend.distributor

import akka.actor.{ActorRef, ActorSelection, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.stage.GraphStageLogic.StageActorRef
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend.PricerMsg
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.language.postfixOps
import backend.PricerMsg._

object DistributorEndpointStage {
  def apply(connectionId: Int, registryRef: ActorSelection) = Flow.fromGraph(new DistributorEndpointStage(connectionId, registryRef))
}

/**
  * This is the last stage of the stream attached to the websocket connection. This stage links with the Price stream endpoint to form end-to-end flow.
  */
private class DistributorEndpointStage(connectionId: Int, registryRef: ActorSelection) extends GraphStage[FlowShape[PricerMsg, PricerMsg]] {
  spec =>
  val in: Inlet[PricerMsg] = Inlet("RequestsIn")
  val out: Outlet[PricerMsg] = Outlet("UpdatesOut")
  override val shape: FlowShape[PricerMsg, PricerMsg] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with ManuallyControlledStreamProducer with FastStreamConsumer {
      override def selfRef = getStageActorRef(onMessage)
      override def in: Inlet[PricerMsg] = spec.in
      override def out: Outlet[PricerMsg] = spec.out
      override def preStart(): Unit = {
        // publish this endpoint' location
        registryRef ! StreamLinkApi.DistributorStreamRef(selfRef)
        super.preStart()
      }

      def onMessage(x: (ActorRef, Any)): Unit = handlePricerBoundMessage orElse clientBoundMessage apply x._2
    }

}

/**
  * Producing side of the stage - one that consumes from the fast producer (Pricer stream) and pushes messages into the websocket stream
  */
private trait FastStreamConsumer {
  _: GraphStageLogic =>

  def out: Outlet[PricerMsg]

  private var pendingPing: Option[Ping] = None                        // indicates a pending ping request
  private var clientUpdatesQueue: Queue[Short] = Queue()              // pending price updates (currency ids)
  private var pendingClientUpdates: Map[Short, PriceUpdate] = Map()   // latest price ticks for all pending updates

  setHandler(out, new OutHandler {
    // this method is called every time downstream is ready for the next element
    override def onPull(): Unit = {
      forwardPingToClient()
      forwardUpdateToClient()
    }
  })

  protected val clientBoundMessage: PartialFunction[Any, Unit] = {
    case u: PriceUpdate =>
      // if we receive a price update, update the snapshot, schedule the update and attempt to push an element downstream
      if (!pendingClientUpdates.contains(u.ccyPairId)) clientUpdatesQueue = clientUpdatesQueue enqueue u.ccyPairId
      pendingClientUpdates += u.ccyPairId -> u
      forwardUpdateToClient()
    case u: Ping if pendingPing.isEmpty =>
      // schedule a ping request if not scheduled already
      pendingPing = Some(u)
      forwardPingToClient()
    case u: Ping => // otherwise ignore
  }

  private def forwardPingToClient() = if (isAvailable(out)) pendingPing foreach { ping =>
    push(out, ping)
    pendingPing = None
  }

  private def forwardUpdateToClient() = if (isAvailable(out)) clientUpdatesQueue.dequeueOption foreach {
    case (key, queue) =>
      clientUpdatesQueue = queue
      pendingClientUpdates.get(key) foreach (push(out, _))
      pendingClientUpdates -= key
  }
}

/**
  * Consuming side of the stage - one that takes requests coming from the websocket connection and pushes to the Pricer
  */
private trait ManuallyControlledStreamProducer extends StrictLogging {
  _: GraphStageLogic =>

  def in: Inlet[PricerMsg]
  def selfRef: StageActorRef


  private var pricerStreamRef: Option[ActorRef] = None        // current location of the pricer stream
  private var demand  = 0                                     // current demand
  private var pricerMessageQueue: Queue[PricerMsg] = Queue()  // messages scheduled for Pricer

  private var openSubscriptions: Set[Short] = Set()           // all currently open subscriptions by the client

  override def preStart(): Unit = pull(in)                    // pull first element

  setHandler(in, new InHandler {
    // this method is called every time next element is available for consumption
    override def onPush(): Unit = forwardRequestToPriceEngine()
  })

  @tailrec private def forwardRequestToPriceEngine(): Unit =
    if (hasDemand) pricerStreamRef match {                    // if there is demand ..
      case Some(target) =>                                    // and pricer location is known ..
        takeQueueHead() orElse takeStreamHead() match {       // drain queue first, then take stream head ..
          case Some(msg) =>                                   // and if there is an element available ..
            target ! StreamLinkApi.Payload(selfRef, msg)      // send to pricer
            demand -= 1                                       // and consume a demand point
            forwardRequestToPriceEngine()                     // and repeat
          case _ =>
        }
      case _ =>
    }

  def hasDemand = demand > 0
  def takeQueueHead(): Option[PricerMsg] =
    pricerMessageQueue.dequeueOption.map {
      case (msg, remainder) =>
        pricerMessageQueue = remainder
        msg
    }
  def takeStreamHead(): Option[PricerMsg] =
    if (isAvailable(in)) {                                    // If there is an element available in the stream ..
      val next = grab(in)                                     // take it ..
      next match {
        case StreamRequest(id) => openSubscriptions += id     // if it's a subscription request - remember it
        case _ =>
      }
      pull(in)                                                // pull next element
      Some(next)
    } else None


  protected val handlePricerBoundMessage: PartialFunction[Any, Unit] = {
    case StreamLinkApi.Demand(sender) =>                      // if we receive a demand signal
      if (pricerStreamRef.isEmpty) {                          // and pricer location is currently unknown
        logger.info(s"Linked with pricer stream at $sender, re-opening ${openSubscriptions.size} subscription(s)")
        selfRef.watch(sender)                                 // watch it - so we know when it terminates
        pricerStreamRef = Some(sender)                        // and remember it
        pricerMessageQueue = Queue(openSubscriptions.map(x => StreamRequest(x)).toSeq: _*) // re-open all subscriptions
      }
      demand = 1                                              // account the demand
      forwardRequestToPriceEngine()                           // attempt to push message to Pricer
    case Terminated(ref) =>                                   // when pricer terminates ...
      logger.info(s"Broken link with $ref")
      pricerMessageQueue = Queue()                            // clean the pending queue
      pricerStreamRef = None                                  // clean the location
      demand = 0                                              // reset the demand
  }

}



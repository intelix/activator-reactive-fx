package backend.distributor

import akka.actor.{ActorRef, ActorSelection, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.stage.GraphStageLogic.StageActorRef
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend.PricerApi
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.language.postfixOps
import backend.PricerApi._

object WebsocketStreamLinkStage {
  def apply(connectionId: Int, registryRef: ActorSelection) = Flow.fromGraph(new WebsocketStreamLinkStage(connectionId, registryRef))
}

private trait FastStreamConsumer {
  _: GraphStageLogic =>

  def out: Outlet[PricerApi]

  private var pendingPing: Option[Ping] = None
  private var pendingClientUpdates: Map[Short, PriceUpdate] = Map()
  private var clientUpdatesQueue: Queue[Short] = Queue()

  setHandler(out, new OutHandler {
    override def onPull(): Unit = {
      forwardPingToClient()
      forwardUpdateToClient()
    }
  })

  protected val clientBoundMessage: PartialFunction[Any, Unit] = {
    case u: PriceUpdate =>
      if (!pendingClientUpdates.contains(u.ccyPairId)) clientUpdatesQueue = clientUpdatesQueue enqueue u.ccyPairId
      pendingClientUpdates += u.ccyPairId -> u
      forwardUpdateToClient()
    case u: Ping if pendingPing.isEmpty =>
      pendingPing = Some(u)
      forwardPingToClient()
    case u: Ping =>
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

private trait ManuallyControlledStreamProducer extends StrictLogging {
  _: GraphStageLogic =>

  def in: Inlet[PricerApi]

  def selfRef: StageActorRef

  private var pricerStreamRef: Option[ActorRef] = None
  private var demand  = 0
  private var pricerMessageQueue: Queue[PricerApi] = Queue()

  private var openSubscriptions: Set[Short] = Set()

  override def preStart(): Unit = pull(in)

  setHandler(in, new InHandler {
    override def onPush(): Unit = forwardRequestToPriceEngine()
  })

  @tailrec private def forwardRequestToPriceEngine(): Unit =
    if (hasDemand) pricerStreamRef match {
      case Some(target) =>
        takeQueueHead() orElse takeStreamHead() match {
          case Some(msg) =>
            target ! StreamLinkApi.Payload(selfRef, msg)
            demand -= 1
            forwardRequestToPriceEngine()
          case _ =>
        }
      case _ =>
    }

  def hasDemand = demand > 0
  def takeQueueHead(): Option[PricerApi] =
    pricerMessageQueue.dequeueOption.map {
      case (msg, remainder) =>
        pricerMessageQueue = remainder
        msg
    }
  def takeStreamHead(): Option[PricerApi] =
    if (isAvailable(in)) {
      val next = grab(in)
      next match {
        case StreamRequest(id) => openSubscriptions += id
        case _ =>
      }
      pull(in)
      Some(next)
    } else None


  protected val handlePricerBoundMessage: PartialFunction[Any, Unit] = {
    case StreamLinkApi.Demand(sender) =>
      if (pricerStreamRef.isEmpty) {
        logger.info(s"Linked with pricer stream at $sender, re-opening ${openSubscriptions.size} subscription(s)")
        selfRef.watch(sender)
        pricerStreamRef = Some(sender)
        pricerMessageQueue = Queue(openSubscriptions.map(x => StreamRequest(x)).toSeq: _*)
      }
      demand = 1
      forwardRequestToPriceEngine()
    case Terminated(ref) =>
      logger.info(s"Broken link with $ref")
      pricerMessageQueue = Queue()
      pricerStreamRef = None
      demand = 0
  }

}


private class WebsocketStreamLinkStage(connectionId: Int, registryRef: ActorSelection) extends GraphStage[FlowShape[PricerApi, PricerApi]] {
  spec =>
  val in: Inlet[PricerApi] = Inlet("RequestsIn")
  val out: Outlet[PricerApi] = Outlet("UpdatesOut")
  override val shape: FlowShape[PricerApi, PricerApi] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with ManuallyControlledStreamProducer with FastStreamConsumer {
      override def selfRef = getStageActorRef(onMessage)
      override def in: Inlet[PricerApi] = spec.in
      override def out: Outlet[PricerApi] = spec.out
      override def preStart(): Unit = {
        registryRef ! StreamLinkApi.DistributorStreamRef(selfRef)
        super.preStart()
      }

      def onMessage(x: (ActorRef, Any)): Unit = handlePricerBoundMessage orElse clientBoundMessage apply x._2
    }

}
package backend

import akka.actor.{ActorRef, ActorSelection, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.stage.GraphStageLogic.StageActorRef
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend.StreamLinkProtocol.{Demand, Payload, WebsocketStreamRef}
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.language.postfixOps

object WebsocketStreamLinkStage {
  def apply(connectionId: Int, registryRef: ActorSelection) = Flow.fromGraph(new WebsocketStreamLinkStage(connectionId, registryRef))
}

private trait FastStreamConsumer {
  _: GraphStageLogic =>

  def out: Outlet[ApplicationMessage]

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

  def in: Inlet[ApplicationMessage]

  def selfRef: StageActorRef

  private var datasourceStreamRef: Option[ActorRef] = None
  private var demand  = 0
  private var datasourceMessageQueue: Queue[ApplicationMessage] = Queue()

  private var openSubscriptions: Set[Short] = Set()

  override def preStart(): Unit = pull(in)

  setHandler(in, new InHandler {
    override def onPush(): Unit = forwardRequestToPriceEngine()
  })

  @tailrec private def forwardRequestToPriceEngine(): Unit =
    if (hasDemand) datasourceStreamRef match {
      case Some(target) =>
        takeQueueHead() orElse takeStreamHead() match {
          case Some(msg) =>
            target ! Payload(selfRef, msg)
            demand -= 1
            forwardRequestToPriceEngine()
          case _ =>
        }
      case _ =>
    }

  def hasDemand = demand > 0
  def takeQueueHead(): Option[ApplicationMessage] =
    datasourceMessageQueue.dequeueOption.map {
      case (msg, remainder) =>
        datasourceMessageQueue = remainder
        msg
    }
  def takeStreamHead(): Option[ApplicationMessage] =
    if (isAvailable(in)) {
      val next = grab(in)
      next match {
        case StreamRequest(id) => openSubscriptions += id
        case _ =>
      }
      pull(in)
      Some(next)
    } else None


  protected val datasourceBoundMessage: PartialFunction[Any, Unit] = {
    case Demand(sender) =>
      if (datasourceStreamRef.isEmpty) {
        logger.info(s"Linked with datasource stream at $sender, re-opening ${openSubscriptions.size} subscription(s)")
        selfRef.watch(sender)
        datasourceStreamRef = Some(sender)
        datasourceMessageQueue = Queue(openSubscriptions.map(x => StreamRequest(x)).toSeq: _*)
      }
      demand = 1
      forwardRequestToPriceEngine()
    case Terminated(ref) =>
      logger.info(s"Broken link with $ref")
      datasourceMessageQueue = Queue()
      datasourceStreamRef = None
      demand = 0
  }

}


private class WebsocketStreamLinkStage(connectionId: Int, registryRef: ActorSelection) extends GraphStage[FlowShape[ApplicationMessage, ApplicationMessage]] {
  spec =>
  val in: Inlet[ApplicationMessage] = Inlet("RequestsIn")
  val out: Outlet[ApplicationMessage] = Outlet("UpdatesOut")
  override val shape: FlowShape[ApplicationMessage, ApplicationMessage] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with ManuallyControlledStreamProducer with FastStreamConsumer {
      override lazy val selfRef = getStageActorRef(onMessage)
      override val in: Inlet[ApplicationMessage] = spec.in
      override val out: Outlet[ApplicationMessage] = spec.out

      override def preStart(): Unit = {
        registryRef ! WebsocketStreamRef(selfRef)
        super.preStart()
      }

      def onMessage(x: (ActorRef, Any)): Unit = datasourceBoundMessage orElse clientBoundMessage apply x._2
    }
}
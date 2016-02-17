package backend

import akka.actor.{ActorRef, ActorSelection, Terminated}
import akka.stream.scaladsl.Flow
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

private class WebsocketStreamLinkStage(connectionId: Int, registryRef: ActorSelection) extends GraphStage[FlowShape[ApplicationMessage, ApplicationMessage]] {
  val in: Inlet[ApplicationMessage] = Inlet("RequestsIn")
  val out: Outlet[ApplicationMessage] = Outlet("UpdatesOut")
  override val shape: FlowShape[ApplicationMessage, ApplicationMessage] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with StrictLogging {

    lazy val self = getStageActorRef(onMessage)

    private var datasourceStreamRef: Option[ActorRef] = None
    private var demand = 0
    private var datasourceMessageQueue: Queue[ApplicationMessage] = Queue()

    private var pendingClientUpdates: Map[Short, PriceUpdate] = Map()
    private var clientUpdatesQueue: Queue[Short] = Queue()

    private var openSubscriptions: Set[Short] = Set()

    setHandler(in, new InHandler {
      override def onPush(): Unit = forwardRequestToPriceEngine()
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = forwardUpdateToClient()
    })

    override def preStart(): Unit = {
      registryRef ! WebsocketStreamRef(self)
      pull(in)
    }

    def onMessage(x: (ActorRef, Any)): Unit = x match {
      case (_, Demand(sender)) =>
        if (datasourceStreamRef.isEmpty) {
          logger.info(s"Linked with datasource stream at $sender, re-opening ${openSubscriptions.size} subscription(s)")
          self.watch(sender)
          datasourceStreamRef = Some(sender)
          datasourceMessageQueue = Queue(openSubscriptions.map(x => StreamRequest(x)).toSeq: _*)
        }
        demand = 1
        forwardRequestToPriceEngine()
      case (_, Terminated(ref)) =>
        logger.info(s"Broken link with $ref")
        self.unwatch(ref)
        datasourceMessageQueue = Queue()
        datasourceStreamRef = None
        demand = 0
        if (isAvailable(in)) pull(in)
      case (_, u: PriceUpdate) =>
        if (!pendingClientUpdates.contains(u.ccyPairId)) clientUpdatesQueue = clientUpdatesQueue enqueue u.ccyPairId
        pendingClientUpdates += u.ccyPairId -> u
        forwardUpdateToClient()
      case (_, u: Ping) => if (isAvailable(out)) push(out, u)
    }

    @tailrec def forwardRequestToPriceEngine(): Unit =
      if (hasDemand) datasourceStreamRef match {
        case Some(target) =>
          takeQueueHead() orElse takeStreamHead() match {
            case Some(msg) =>
              target ! Payload(self, msg)
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

    def forwardUpdateToClient() = if (isAvailable(out)) clientUpdatesQueue.dequeueOption foreach {
      case (key, queue) =>
        clientUpdatesQueue = queue
        pendingClientUpdates.get(key) foreach (push(out, _))
        pendingClientUpdates -= key
    }

  }
}
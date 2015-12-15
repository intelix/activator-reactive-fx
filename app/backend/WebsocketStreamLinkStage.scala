package backend

import akka.actor.{ActorRef, ActorSelection, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import backend.StreamLinkProtocol.{Demand, Payload, WebsocketStreamRef}
import com.typesafe.scalalogging.StrictLogging

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

    case class QueuedMessage(payload: ApplicationMessage, pullAfterSending: Boolean)

    lazy val self = getStageActorRef(onMessage)

    private var datasourceStreamRef: Option[ActorRef] = None
    private var hasDemand = false
    private var datasourceMessageQueue: Queue[QueuedMessage] = Queue()

    private var pendingClientUpdates: Map[Short, PriceUpdate] = Map()
    private var clientUpdatesQueue: Queue[Short] = Queue()

    private var openSubscriptions: Set[Short] = Set()

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val next = grab(in)
        next match {
          case StreamRequest(id) => openSubscriptions += id
          case _ =>
        }
        datasourceMessageQueue = datasourceMessageQueue enqueue QueuedMessage(next, pullAfterSending = true)
        forwardToDatasource()
      }
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = forwardToClient()
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
          datasourceMessageQueue = Queue(openSubscriptions.map(x => QueuedMessage(StreamRequest(x), pullAfterSending = false)).toSeq: _*)
        }
        hasDemand = true
        forwardToDatasource()
      case (_, Terminated(ref)) =>
        logger.info(s"Broken link with $ref")
        self.unwatch(ref)
        datasourceMessageQueue = Queue()
        datasourceStreamRef = None
        hasDemand = false
        if (isAvailable(in)) pull(in)
      case (_, u: PriceUpdate) =>
        if (!pendingClientUpdates.contains(u.ccyPairId)) clientUpdatesQueue = clientUpdatesQueue enqueue u.ccyPairId
        pendingClientUpdates += u.ccyPairId -> u
        forwardToClient()
      case (_, u: Ping) => if (isAvailable(out)) push(out, u)
    }

    def forwardToDatasource() =
      for (
        target <- datasourceStreamRef if hasDemand;
        (QueuedMessage(msg, pullAfterSending), remainder) <- datasourceMessageQueue dequeueOption
      ) {
        target ! Payload(self, msg)
        hasDemand = false
        datasourceMessageQueue = remainder
        if (pullAfterSending) pull(in)
      }

    def forwardToClient() = if (isAvailable(out)) clientUpdatesQueue.dequeueOption foreach {
      case (key, queue) =>
        clientUpdatesQueue = queue
        pendingClientUpdates.get(key) foreach (push(out, _))
        pendingClientUpdates -= key
    }

  }
}
package server

import java.nio.ByteOrder

import akka.actor.{Actor, ActorRef, ActorSelection, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, UpgradeToWebsocket}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream._
import akka.stream.scaladsl.{BidiFlow, Flow, FlowGraph, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import server.StreamsShared._

import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success}


class SubscriptionLinkFlow(registryRef: ActorSelection) extends GraphStage[FlowShape[DSDialect, DSDialect]] {
  val in: Inlet[DSDialect] = Inlet("RequestsIn")
  val out: Outlet[DSDialect] = Outlet("UpdatesOut")
  override val shape: FlowShape[DSDialect, DSDialect] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    case class QueuedMessage(payload: DSDialect, fromClient: Boolean)

    lazy val self = getStageActorRef(onMessage)
    private var requestor: Option[ActorRef] = None
    private var hasDemand = false
    private var msgQueue: Queue[QueuedMessage] = Queue()
    private var pendingUpdates: Map[Short, SharedStreamUpdate] = Map()
    private var keysOrder: Queue[Short] = Queue()
    private var subscriptions: Set[Short] = Set()

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val next = grab(in)
        next match {
          case SharedStreamRequest(id) => subscriptions += id
          case _ =>
        }
        msgQueue = msgQueue enqueue QueuedMessage(next, fromClient = true)
        forwardToDatasource()
      }
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = forwardToClient()
    })

    override def preStart(): Unit = {
      registryRef ! ClientLink(self)
      pull(in)
    }

    private def onMessage(x: (ActorRef, Any)): Unit = x match {
      case (_, Terminated(ref)) =>
        self.unwatch(ref)
        msgQueue = Queue()
        requestor = None
        hasDemand = false
        if (isAvailable(in)) pull(in)
      case (_, Demand(sender)) =>
        if (requestor.isEmpty) {
          println(s"!>>>> Demand from: $sender ")
          self.watch(sender)
          requestor = Some(sender)
          msgQueue = Queue(subscriptions.map(x => QueuedMessage(SharedStreamRequest(x), fromClient = false)).toSeq: _*)
        }
        hasDemand = true
        forwardToDatasource()
      case (_, u: SharedStreamUpdate) =>
        if (!pendingUpdates.contains(u.ccyPairId)) keysOrder = keysOrder enqueue u.ccyPairId
        pendingUpdates += u.ccyPairId -> u
        forwardToClient()
      case (_, u: PingRequest) =>
        if (isAvailable(out)) push(out, u)
    }

    private def forwardToDatasource() =
      for (
        target <- requestor if hasDemand;
        (QueuedMessage(msg, fromClient), remainder) <- msgQueue dequeueOption
      ) {
        target ! RequestWithRef(self, msg)
        hasDemand = false
        msgQueue = remainder
        if (fromClient) pull(in)
      }

    private def forwardToClient() = if (isAvailable(out)) keysOrder.dequeueOption foreach {
      case (key, queue) =>
        keysOrder = queue
        pendingUpdates.get(key) foreach (push(out, _))
        pendingUpdates -= key
    }

  }
}


class WebsocketServer extends Actor {

  implicit val system = context.system
  implicit val executor = context.dispatcher
  val port = 8080
  val host = "localhost"

  implicit val bo = ByteOrder.BIG_ENDIAN

  val decider: Supervision.Decider = {
    case x =>
      x.printStackTrace()
      Supervision.Stop
  }

  implicit val mat = ActorMaterializer(
    ActorMaterializerSettings(context.system)
      .withDebugLogging(true)
      .withSupervisionStrategy(decider))


  def msgLogging = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val out = b.add(Flow[Message].map { x => println(s"!>>> out: ${x} [$x]"); x })
    val in = b.add(Flow[Message].map { x => println(s"!>>> in: ${x} [$x]"); x })
    BidiShape.fromFlows(in, out)
  })


  val wsCodec = BidiFlow.fromGraph(FlowGraph.create() { b =>
    val in = b.add(Flow[ByteString].map(StreamsShared.fromDSBytes))
    val out = b.add(Flow[DSDialect].map(StreamsShared.toDSBytes))
    BidiShape.fromFlows(in, out)
  })

  def buildFlow() = {
    msgLogging atop frameFolding atop dsLogging atop wsCodec join Flow.fromGraph(new SubscriptionLinkFlow(context.actorSelection("/user/registry")))
  }


  def frameFolding = BidiFlow.fromGraph[Message, ByteString, ByteString, Message, Unit](FlowGraph.create() { b =>
    val in = b.add(Flow[Message].mapAsync(1) {
      case t: TextMessage.Strict =>
        Future.successful(ByteString(t.text))
      case t: TextMessage =>
        val sink = Sink.fold[String, String]("")(_ + _)
        t.textStream.runWith(sink).map(ByteString(_))
      case _ => Future.successful(ByteString.empty)
    })
    val out = b.add(Flow[ByteString].map { b => TextMessage.Strict(b.utf8String) })
    BidiShape.fromFlows(in, out)
  })


  val requestHandler: HttpRequest ⇒ HttpResponse = {
    case req@HttpRequest(HttpMethods.GET, Uri.Path("/"), _, _, _) ⇒
      req.header[UpgradeToWebsocket] match {
        case Some(upgrade) ⇒ upgrade.handleMessages(buildFlow())
        case None ⇒ HttpResponse(400, entity = "Not a valid websocket request!")
      }
    case _: HttpRequest ⇒ HttpResponse(404, entity = "Unknown resource!")
  }

  Http().bindAndHandleSync(handler = requestHandler, interface = host, port = port) onComplete {
    case Success(binding) => self ! SuccessfulBinding(binding)
    case Failure(x) => self ! BindingFailed(x)
  }

  private case class SuccessfulBinding(binding: Http.ServerBinding)

  private case class BindingFailed(x: Throwable)


  override def receive: Receive = {
    case SuccessfulBinding(binding) => println(s"!>>>> Successfully bound : $binding ")
    case BindingFailed(x) =>
      println(s"!>>>> Failed:")
      x.printStackTrace()
  }
}

package server

import akka.actor._
import akka.stream._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{Flow, Tcp}
import akka.stream.stage._
import server.StreamsShared._

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


class RegistryActor extends Actor {

  private var clients: List[ClientLink] = List()
  private var dataSource: Option[ActorRef] = None

  override def receive: Actor.Receive = {
    case m: ClientLink =>
      println(s"!>>>> @Registry: ClientLink = $m ")
      context.watch(m.ref)
      clients :+= m
      dataSource foreach (_ ! m)
    case DataSourceLink(ref) =>
      println(s"!>>>> @Registry: DataSourceLink = $ref ")
      dataSource = Some(context.watch(ref))
      clients foreach (ref ! _)
    case Terminated(ref) if dataSource.contains(ref) =>
      println(s"!>>>> @Registry: DataSourceLink = Gone ")
      dataSource = None
    case Terminated(ref) =>
      println(s"!>>>> @Registry: ClientLink = Gone ($ref) ")
      clients = clients filterNot (_.ref == ref)
  }
}


object ConnectionManagerActor {

  case class StateData(datasourceLinkRef: Option[ActorRef] = None)

  trait State

  case object Disconnected extends State

  case object ConnectionPending extends State

  case object Connected extends State

  object Messages {

    case class SuccessfullyConnected(connection: OutgoingConnection)

    case class ConnectionAttempFailed()

    case object Start

    case object ExposeFlow

  }

}

import server.ConnectionManagerActor._

class ConnectionManagerActor(ref: ActorSelection, endpoints: Array[(String, Int)]) extends FSM[State, StateData] {

  import Messages._

  implicit val sys = context.system
  implicit val ec = context.dispatcher

  val decider: Supervision.Decider = {
    case _ =>
      println(s"!>>> STOPPING")
      Supervision.Stop
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider).withDebugLogging(true))

  startWith(Disconnected, StateData())
  self ! Start

  when(Disconnected, stateTimeout = 1 second) {
    case Event(Terminated(r), _) => stay()
    case Event(Start | StateTimeout, _) => goto(ConnectionPending)
  }

  when(ConnectionPending) {
    case Event(SuccessfullyConnected(c), _) =>
      println(s"!>>> Connected")
      goto(Connected)
    case Event(ConnectionAttempFailed(), _) =>
      println(s"!>>> Failed to connect")
      goto(Disconnected)
  }

  when(Connected)(PartialFunction.empty)


  whenUnhandled {
    case Event(Terminated(r), st) => goto(Disconnected) using StateData()
    case Event(m@DataSourceLink(r), _) =>
      context.watch(r)
      println(s"!>>>> Received $m ")
      stay() using StateData(Some(r))
  }

  onTransition {
    case _ -> ConnectionPending =>
      println(s"!>>> Establishing connection")
      val (host, port) = endpoints((Math.random() * endpoints.length).toInt)
      println(s"!>>> Connecting to $host:$port")
      lazy val connection = Tcp().outgoingConnection(host, port)

      val dsFlow = dsFraming.atop(dsCodec).join(Flow.fromGraph(new DatasourceLinkFlow(self)))

      val joined = connection.join(dsFlow).run()
      joined onComplete {
        case Success(c) =>
          self ! SuccessfullyConnected(c)
        case Failure(f) => self ! ConnectionAttempFailed()
      }
    case _ -> Connected =>
      println(s"!>>>> Connected !! ")
      stateData.datasourceLinkRef foreach (ref ! DataSourceLink(_))
  }

}


class DatasourceLinkFlow(parentRef: ActorRef) extends GraphStage[FlowShape[DSDialect, DSDialect]] {

  case class StreamHead(maybeRef: Option[ActorRef], element: DSDialect)

  val in: Inlet[DSDialect] = Inlet("RequestsOut")
  val out: Outlet[DSDialect] = Outlet("UpdatesIn")
  override val shape: FlowShape[DSDialect, DSDialect] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    lazy val self = getStageActorRef(onMessage)
    var pending: Queue[StreamHead] = Queue()
    var liveLinks: Set[ActorRef] = Set()
    var liveSubscriptions: Map[Short, List[ActorRef]] = Map()

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        grab(in) match {
          case m@SharedStreamUpdate(cId, a) =>
            liveSubscriptions get cId foreach (_.foreach(_ ! m))
          case m@PingRequest(id) =>
            liveLinks foreach (_ ! m)
          case x => println(s"!>>>> UNRECOGNISED $x ")
        }
        pull(in)
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pushToDatasource()
    })

    override def preStart(): Unit = {
      parentRef ! DataSourceLink(self)
      pull(in)
    }


    override def postStop(): Unit = {
      println(s"!>>>> HMMMMM ")
    }

    override protected def onTimer(timerKey: Any): Unit = {
      liveSubscriptions = liveSubscriptions filter {
        case (cId, list) if list.isEmpty =>
          pending = StreamHead(None, SharedStreamCancel(cId)) +: pending
          false
        case _ => true
      }
      pushToDatasource()
      if (liveSubscriptions.isEmpty) cancelTimer("timer")
    }

    private def onMessage(x: (ActorRef, Any)): Unit = x match {
      case (_, ClientLink(ref)) => if (!liveLinks.contains(ref)) {
        liveLinks += ref
        self.watch(ref)
        ref ! Demand(self)
      }
      case (_, Terminated(ref)) =>
        pending = pending.filterNot(_.maybeRef.contains(ref))
        liveLinks -= ref
        liveSubscriptions = liveSubscriptions map {
          case (cId, subscribers) if subscribers.contains(ref) => cId -> subscribers.filterNot(_ == ref)
          case other => other
        }
      case (_, RequestWithRef(ref, m@SharedStreamRequest(cId))) =>
        println(s"!>>>>  StreamRequest from $ref")
        if (!liveSubscriptions.contains(cId)) {
          pending = pending :+ StreamHead(Some(ref), m)
          pushToDatasource()
          if (liveSubscriptions.isEmpty) schedulePeriodicallyWithInitialDelay("timer", 5 seconds, 5 seconds)
        } else ref ! Demand(self)
        if (!liveSubscriptions.get(cId).exists(_.contains(ref)))
          liveSubscriptions += cId -> (liveSubscriptions.getOrElse(cId, List()) :+ ref)
      case (_, RequestWithRef(ref, m: DatasourceOut)) =>
        pending = StreamHead(Some(ref), m) +: pending
        pushToDatasource()
      case(_, el) => println(s"!>>>> UNEXPECTED $el!  ")
    }

    private def pushToDatasource() = if (isAvailable(out) && pending.nonEmpty)
      pending.dequeue match {
        case (StreamHead(ref, el), queue) =>
          push(out, el)
          pending = queue
          ref foreach (_ ! Demand(self))
      }

  }

}


class DatasourceLink()(implicit sys: ActorSystem) {

  val conMgr = sys.actorOf(Props(classOf[ConnectionManagerActor], sys.actorSelection("/user/registry"), Array(("localhost", 8881), ("localhost", 8882), ("localhost", 8883))))


}

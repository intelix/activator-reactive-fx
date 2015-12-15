package backend

import akka.actor._
import akka.stream._
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Tcp.OutgoingConnection
import backend.ConnectionManagerActor.Messages.{Connect, ConnectionAttemptFailed, SuccessfullyConnected}
import backend.ConnectionManagerActor._
import backend.StreamLinkProtocol._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


object DatasourceConnection {
  def start()(implicit sys: ActorSystem) = {
    val cfg = sys.settings.config
    sys.actorOf(Props(classOf[ConnectionManagerActor],
      StreamRegistry.selection,
      List() ++ cfg.getStringList("datasource.servers.enabled") map { id =>
        Endpoint(cfg.getString(s"datasource.servers.$id.host"), cfg.getInt(s"datasource.servers.$id.port"))
      }))
  }
}

private object ConnectionManagerActor {

  case class Endpoint(host: String, port: Int)

  case class StateData(endpoints: Vector[Endpoint], datasourceLinkRef: Option[ActorRef] = None)

  trait State

  case object Disconnected extends State

  case object ConnectionPending extends State

  case object Connected extends State

  object Messages {

    case class SuccessfullyConnected(connection: OutgoingConnection)

    case class ConnectionAttemptFailed()

    case object Connect

    case object ExposeFlow

  }

}

private class ConnectionManagerActor(ref: ActorSelection, endpoints: List[Endpoint]) extends FSM[State, StateData] with StrictLogging {

  implicit val sys = context.system
  implicit val ec = context.dispatcher

  val decider: Supervision.Decider = {
    case x =>
      x.printStackTrace()
      Supervision.Stop
  }
  implicit val materializer =
    ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider).withDebugLogging(enable = false))

  startWith(Disconnected, StateData(Vector() ++ endpoints))
  self ! Connect

  when(Disconnected) {
    case Event(Terminated(r), _) => stay()
    case Event(Connect, data) =>
      goto(ConnectionPending) using data.copy(endpoints = data.endpoints.tail :+ data.endpoints.head)
  }

  when(ConnectionPending) {
    case Event(SuccessfullyConnected(c), data) => goto(Connected)
    case Event(ConnectionAttemptFailed(), data) => goto(Disconnected)
  }

  when(Connected) {
    case Event(m@DatasourceStreamRef(r), data) =>
      context.watch(r)
      ref ! DatasourceStreamRef(r)
      stay() using data.copy(datasourceLinkRef = Some(r))
  }

  whenUnhandled {
    case Event(Terminated(r), data) =>
      goto(Disconnected) using data.copy(datasourceLinkRef = None)
    case Event(m@DatasourceStreamRef(r), data) =>
      stay() using data.copy(datasourceLinkRef = Some(context.watch(r)))
    case Event(_, _) => stay()
  }

  onTransition {
    case ConnectionPending -> Disconnected =>
      log.info("Unable to connect to the datasource")
      setTimer("reconnectDelay", Connect, 1 second, repeat = false)
    case Connected -> Disconnected =>
      log.info("Lost connection to the datasource")
      self ! Connect
    case _ -> ConnectionPending =>
      val connectTo = stateData.endpoints.head
      log.info(s"Connecting to ${connectTo.host}:${connectTo.port}")

      val flow = FramingStage() atop CodecStage() join DatasourceStreamLinkStage(self)

      Tcp().outgoingConnection(connectTo.host, connectTo.port) join flow run() onComplete {
        case Success(c) =>
          self ! SuccessfullyConnected(c)
        case Failure(f) => self ! ConnectionAttemptFailed()
      }
    case _ -> Connected =>
      log.info("Successfully connected")
      stateData.datasourceLinkRef foreach (ref ! DatasourceStreamRef(_))
  }

}



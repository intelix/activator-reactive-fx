package backend.distributor

import akka.actor._
import akka.stream._
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Tcp.OutgoingConnection
import backend.distributor.PricerConnectionManager._
import backend.distributor.PricerConnectionManager.Messages._
import backend.distributor.StreamLinkApi.PricerStreamRef
import backend.shared.{CodecStage, FramingStage}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


object PricerConnectionManager {
  def start()(implicit sys: ActorSystem) = {
    val cfg = sys.settings.config
    sys.actorOf(Props(classOf[PricerConnectionManager],
      StreamRegistry.selection,
      List() ++ cfg.getStringList("pricer.servers.enabled") map { id =>
        Endpoint(cfg.getString(s"pricer.servers.$id.host"), cfg.getInt(s"pricer.servers.$id.port"))
      }))
  }

  case class Endpoint(host: String, port: Int)

  case class StateData(endpoints: Vector[Endpoint], pricerStreamRef: Option[ActorRef] = None)

  sealed trait State

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

/**
  * Manages connection with Pricer endpoint
  */
private class PricerConnectionManager(ref: ActorSelection, endpoints: List[Endpoint]) extends FSM[State, StateData] with StrictLogging {

  implicit val sys = context.system
  implicit val ec = context.dispatcher

  val decider: Supervision.Decider = {
    case x =>
      logger.warn("TCP Stream terminated", x)
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
    case Event(m@PricerStreamRef(r), data) =>
      context.watch(r)
      ref ! PricerStreamRef(r)
      stay() using data.copy(pricerStreamRef = Some(r))
  }

  whenUnhandled {
    case Event(Terminated(r), data) =>
      goto(Disconnected) using data.copy(pricerStreamRef = None)
    case Event(m@PricerStreamRef(r), data) =>
      stay() using data.copy(pricerStreamRef = Some(context.watch(r)))
    case Event(_, _) => stay()
  }

  onTransition {
    case ConnectionPending -> Disconnected =>
      log.info("Unable to connect to the pricer")
      setTimer("reconnectDelay", Connect, 1 second, repeat = false)
    case Connected -> Disconnected =>
      log.info("Lost connection to the pricer")
      self ! Connect
    case _ -> ConnectionPending =>
      val connectTo = stateData.endpoints.head
      log.info(s"Connecting to ${connectTo.host}:${connectTo.port}")

      val processingPipeline = FramingStage() atop CodecStage() join PricerStreamEndpointStage(self)
      Tcp().outgoingConnection(connectTo.host, connectTo.port) join processingPipeline run() onComplete {
        case Success(c) => self ! SuccessfullyConnected(c)
        case Failure(f) => self ! ConnectionAttemptFailed()
      }
    case _ -> Connected =>
      log.info("Successfully connected")
      stateData.pricerStreamRef foreach (ref ! PricerStreamRef(_))
  }

}



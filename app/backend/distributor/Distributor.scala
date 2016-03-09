package backend.distributor

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.StatusCodes.{NotFound, BadRequest}
import akka.http.scaladsl.model.ws.UpgradeToWebsocket
import akka.http.scaladsl.model._
import akka.stream._
import backend.shared.CodecStage
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps
import scala.util.{Failure, Success}

object Distributor {
  def start()(implicit sys: ActorSystem) = sys.actorOf(Props[Distributor], "distributor")
}

/**
  * Distributor component accepts websocket connections and integrates with Pricer component.
  */
private class Distributor extends Actor with StrictLogging {

  implicit val system = context.system
  implicit val executor = context.dispatcher

  var connectionCounter = 0

  // supervision strategy for all websocket handler streams
  val decider: Supervision.Decider = {
    case x => // If anything goes wrong, we terminate the stream and the connection
      logger.error("Stream terminated", x)
      Supervision.Stop
  }

  // Materializer turns stream spec into a runnable entity
  implicit val mat = ActorMaterializer(
    ActorMaterializerSettings(context.system)
      .withDebugLogging(enable = false)
      .withSupervisionStrategy(decider))


  val requestHandler: HttpRequest => HttpResponse = {

    // Handler for /
    case req@HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      req.header[UpgradeToWebsocket] match {
        case Some(upgrade) =>
          connectionCounter += 1
          // initialise a stream...
          upgrade.handleMessages(buildFlow(connectionCounter))
        case None =>
          // not a websocket request
          HttpResponse(BadRequest, entity = "Not a valid websocket request!")
      }
    case _: HttpRequest => HttpResponse(NotFound, entity = "Unknown resource!")
  }

  // Initalise the listener and setup the handler
  val port = context.system.settings.config.getInt("distributor.port")
  val host = context.system.settings.config.getString("distributor.host")
  Http().bindAndHandleSync(handler = requestHandler, interface = host, port = port) onComplete {
    case Success(binding) => self ! SuccessfulBinding(binding)
    case Failure(x) => self ! BindingFailed(x)
  }

  // Flow spec
  def buildFlow(connectionId: Int) =
    WebsocketFrameStage() atop
      CodecStage() atop
      MetricsStage(connectionId) atop
      ShapingStage(1000) join // messages per second
      DistributorEndpointStage(connectionId, StreamRegistry.selection)

  private case class SuccessfulBinding(binding: Http.ServerBinding)

  private case class BindingFailed(x: Throwable)


  override def receive: Receive = {
    case SuccessfulBinding(binding) =>
      logger.info(s"Websocket server listening at $host:$port")
    case BindingFailed(x) =>
      // Here we just logging the result of the binding. Normally you may want to retry or terminate the process
      logger.error(s"Websocket server failed to bind to $host:$port", x)
  }


}



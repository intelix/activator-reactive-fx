package backend

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.UpgradeToWebsocket
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream._
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps
import scala.util.{Failure, Success}

object WebsocketServer {
  def start()(implicit sys: ActorSystem) = sys.actorOf(Props[WebsocketServer], "websocket-server")
}

private class WebsocketServer extends Actor with StrictLogging {

  implicit val system = context.system
  implicit val executor = context.dispatcher

  val port = context.system.settings.config.getInt("websocket.port")
  val host = context.system.settings.config.getString("websocket.host")

  var connectionCounter = 0

  val decider: Supervision.Decider = {
    case x => Supervision.Stop
  }

  implicit val mat = ActorMaterializer(
    ActorMaterializerSettings(context.system)
      .withDebugLogging(enable = false)
      .withSupervisionStrategy(decider))


  def buildFlow(connId: Int) =
    WebsocketFrameStage() atop CodecStage() atop MetricsStage(connId) atop ThrottlingStage(1000) join WebsocketStreamLinkStage(connId, StreamRegistry.selection)


  val requestHandler: HttpRequest => HttpResponse = {
    case req@HttpRequest(HttpMethods.GET, Uri.Path("/"), _, _, _) =>
      req.header[UpgradeToWebsocket] match {
        case Some(upgrade) =>
          connectionCounter += 1
          upgrade.handleMessages(buildFlow(connectionCounter))
        case None => HttpResponse(400, entity = "Not a valid websocket request!")
      }
    case _: HttpRequest => HttpResponse(404, entity = "Unknown resource!")
  }

  Http().bindAndHandleSync(handler = requestHandler, interface = host, port = port) onComplete {
    case Success(binding) => self ! SuccessfulBinding(binding)
    case Failure(x) => self ! BindingFailed(x)
  }

  private case class SuccessfulBinding(binding: Http.ServerBinding)

  private case class BindingFailed(x: Throwable)

  override def receive: Receive = {
    case SuccessfulBinding(binding) =>
      logger.info(s"Websocket server listening at $host:$port")
    case BindingFailed(x) =>
      logger.error(s"Websocket server failed to bind to $host:$port", x)
  }


}



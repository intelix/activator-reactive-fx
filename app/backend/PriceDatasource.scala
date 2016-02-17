package backend

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Tcp
import com.typesafe.config._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.language.postfixOps

object PriceDatasource {
  def start()(implicit sys: ActorSystem) = {
    implicit val cfg = sys.settings.config
    cfg.getStringList("datasource.servers.enabled").zipWithIndex.foreach { case (id, i) =>
      startIsolated(cfg.getString(s"datasource.servers.$id.host"), cfg.getInt(s"datasource.servers.$id.port"), i + 1)
    }
  }

  private def startIsolated(host: String, port: Int, serverId: Int)(implicit cfg: Config) = new StrictLogging {
    implicit val system = ActorSystem("datasource", cfg)

    val decider: Supervision.Decider = {
      case x =>
        x.printStackTrace()
        Supervision.Stop
    }

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider).withDebugLogging(enable = false))

    Tcp().bind(host, port) runForeach { connection =>
      logger.info(s"New connection from ${connection.remoteAddress}")
      connection handleWith (PricePublisherFlowStage(serverId) join (CodecStage().reversed atop FramingStage().reversed))
    }

  }
}


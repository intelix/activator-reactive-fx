package backend

import akka.actor.ActorSystem
import backend.utils.SystemMonitor
import com.typesafe.config.ConfigFactory
import play.api.{Application, GlobalSettings}

object BackendBootstrap extends GlobalSettings {

  override def onStart(app: Application): Unit = {

    implicit val sys = ActorSystem("backend", ConfigFactory.load("backend.conf"))

    PriceDatasource.start()

    StreamRegistry.start()

    DatasourceConnection.start()

    WebsocketServer.start()

    SystemMonitor.start()

  }

}

package server

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import play.api.{Application, GlobalSettings}

object Global extends GlobalSettings {

  override def onStart(app: Application): Unit = {

    implicit val sys = ActorSystem("server", ConfigFactory.load("akka.conf"))

    val registryActor = sys.actorOf(Props[RegistryActor], "registry")


    val x = new Server()
    val y = new DatasourceLink()
    val zz = sys.actorOf(Props[WebsocketServer])
  }

  override def onStop(app: Application): Unit = {

  }
}

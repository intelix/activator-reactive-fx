package backend

import akka.actor.ActorSystem
import backend.pricer.Pricer
import backend.utils.CPUMonitor
import backend.distributor.{Distributor, PricerConnectionManager, StreamRegistry}
import com.typesafe.config.ConfigFactory
import play.api.{Application, GlobalSettings}

/**
  * Bootstrap for all components.
  */
object BackendBootstrap extends GlobalSettings {

  override def onStart(app: Application): Unit = {

    implicit val sys = ActorSystem("backend", ConfigFactory.load("backend.conf"))

    StreamRegistry.start()

    Pricer.start()

    PricerConnectionManager.start()

    Distributor.start()

    CPUMonitor.start()

  }

}

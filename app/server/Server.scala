package server

import java.util.Random

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream._
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Source, Tcp}
import akka.stream.stage._
import com.typesafe.config.ConfigFactory
import server.StreamsShared._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


object BasePrices {
  val ccyMap: Array[String] = Array("AUD/CAD", "AUD/CHF", "AUD/NZD", "AUD/USD", "CAD/CHF", "EUR/GBP", "EUR/CHF", "EUR/USD", "GBP/AUD", "GBP/CAD", "GBP/CHF", "GBP/USD", "USD/CAD", "USD/CHF", "NZD/USD")
  val basePrice: Map[String, Int] = Map(
    "AUD/CAD" -> 98780,
    "AUD/CHF" -> 70590,
    "AUD/NZD" -> 106840,
    "AUD/USD" -> 71820,
    "CAD/CHF" -> 71400,
    "EUR/GBP" -> 72214,
    "EUR/CHF" -> 108080,
    "EUR/USD" -> 109920,
    "GBP/AUD" -> 211760,
    "GBP/CAD" -> 209390,
    "GBP/CHF" -> 149080,
    "GBP/USD" -> 152160,
    "USD/CAD" -> 137540,
    "USD/CHF" -> 98300,
    "NZD/USD" -> 67090
  )

  val generator = new Random()

  def generateFor(id: Int) = {
    val rnd = generator.nextGaussian() * 15 match {
      case i if Math.abs(i) > 20 => i / 2
      case i => i
    }
    (basePrice.get(ccyMap(id)).get + rnd).toInt
  }
}

class RestartingActor(host: String, port: Int) extends Actor {

  var subsystem: Option[ActorSystem] = None

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    implicit val ds = context.dispatcher
//    val cancellable = context.system.scheduler.schedule(10 seconds, 10 seconds, self, "restart")
    start()
  }

  def start() = {
    subsystem foreach { current =>
      current.shutdown()
      current.awaitTermination()
    }
    var s = ActorSystem("listener", ConfigFactory.load("akka.conf"))
    s.actorOf(Props(classOf[ListenerActor], host, port))
    subsystem = Some(s)
  }

  override def receive: Receive = {
    case "restart" => start()
  }
}

class ListenerActor(host: String, port: Int) extends Actor {

  implicit val sys = context.system

  val decider: Supervision.Decider = {
    case x =>
      println(s"!>>> STOPPING")
      x.printStackTrace()
      Supervision.Stop
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider).withDebugLogging(true))(context)

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind(host, port)
  connections runForeach { connection =>
    println(s"New connection from: ${connection.remoteAddress}")




    class PricePublisherFlow() extends GraphStage[FlowShape[DSDialect, DSDialect]] {
      val in: Inlet[DSDialect] = Inlet("RequestsIn")
      val out: Outlet[DSDialect] = Outlet("UpdatesOut")
      override val shape: FlowShape[DSDialect, DSDialect] = FlowShape(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

        var tokens = 0

        var enabled: Array[Short] = Array()
        var idx = 0

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            grab(in) match {
              case SharedStreamRequest(id) => enable(id)
              case SharedStreamCancel(id) => disable(id)
              case ControlKillServer() => context.system.shutdown()
              case PongResponse(id) =>
                val diff = ((System.nanoTime() % Int.MaxValue) - id) / 2
                println(s"!>>>> diff: " + diff)
            }
            pull(in)
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = publishNext()
        })

        override def preStart(): Unit = {
          schedulePeriodically("tokens", 1000 millis)
          schedulePeriodically("ping", 1000 millis)
          pull(in)
        }

        override protected def onTimer(timerKey: Any): Unit = timerKey match {
          case "tokens" =>
            tokens = 1
            publishNext()
          case "ping" =>
            publishPing()
        }

        private def publishPing(): Unit = if (isAvailable(out) && tokens > 0) {
          push(out, PingRequest((System.nanoTime() % Int.MaxValue).toInt))
          tokens -= 1
        }

        private def publishNext(): Unit = if (enabled.length > 0 && isAvailable(out) && tokens > 0) {
          idx += 1
          if (idx >= enabled.length) idx = 0
          val quote = BasePrices.generateFor(idx)
          push(out, SharedStreamUpdate(enabled(idx), quote))
          tokens -= 1
        }

        private def enable(id: Short) = if (!enabled.contains(id)) enabled = enabled :+ id

        private def disable(id: Short) = enabled = enabled filterNot (_ == id)
      }

    }



    val reversedFraming = dsFraming.reversed
    val reversedCodec = dsCodec.reversed
    val reversedLogging = dsLogging.reversed

    val flow = Flow.fromGraph(new PricePublisherFlow())



    val publishingFlow = flow.join(reversedCodec.atop(reversedFraming).atop(reversedLogging))

    connection.handleWith(publishingFlow)
  }


  override def receive: Actor.Receive = {
    case _ =>
  }
}


class Server()(implicit sys: ActorSystem) {


  sys.actorOf(Props(classOf[RestartingActor], "localhost", 8881))
  sys.actorOf(Props(classOf[RestartingActor], "localhost", 8882))
  sys.actorOf(Props(classOf[RestartingActor], "localhost", 8883))

}

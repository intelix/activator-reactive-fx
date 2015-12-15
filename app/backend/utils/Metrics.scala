package backend.utils

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter
import nl.grons.metrics.scala.MetricName
import org.slf4j.LoggerFactory

object Metrics {
  private val metricRegistry = new com.codahale.metrics.MetricRegistry()
  Slf4jReporter.forRegistry(metricRegistry)
    .outputTo(LoggerFactory.getLogger("metrics"))
    .build()
    .start(5, TimeUnit.SECONDS)
}

trait Metrics extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = Metrics.metricRegistry
  override lazy val metricBaseName: MetricName = MetricName("")
}

trait SimpleThroughputTracker {

  private var since = 0L
  private var value = 0L

  def updateThroughput(v: Long) = value += v

  def calculateThroughputAndReset() = {
    val now = System.currentTimeMillis()
    val result = if (since > 0) {
      val diffMillis = now - since
      value * 1000 / diffMillis
    } else 0
    since = now
    value = 0
    result
  }

}
package backend.utils

import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean

import scala.concurrent.duration._
import scala.language.postfixOps

object SystemMonitor {
  def start() = new Metrics {
    val bean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val gauge = metrics.cachedGauge("cpu.process", 2 seconds)((bean.getProcessCpuLoad * 100).toInt)
  }
}


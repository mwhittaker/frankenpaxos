package frankenpaxos

import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports

object PrometheusUtil {
  def server(host: String, port: Int): Option[HTTPServer] = {
    if (port != -1) {
      DefaultExports.initialize()
      Some(new HTTPServer(host, port))
    } else {
      None
    }
  }
}

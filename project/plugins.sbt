addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.5.1")
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "0.5.0")
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "0.6.23")

addSbtPlugin(
  "com.thesamet" % "sbt-protoc" % "0.99.18" exclude ("com.thesamet.scalapb", "protoc-bridge_2.10")
)
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin-shaded" % "0.7.4"

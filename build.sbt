import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

// name := "zeno"
// libraryDependencies ++= Seq(
//   "io.netty" % "netty-all" % "4.1.25.Final"
// )
// enablePlugins(ScalaJSPlugin)
// scalaJSUseMainModuleInitializer := true
//

name := "zeno"
// libraryDependencies ++= Seq(
//   "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
//   "io.netty" % "netty-all" % "4.1.25.Final",
// )
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "io.netty" % "netty-all" % "4.1.25.Final",
  "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
)

lazy val zeno = crossProject(JSPlatform, JVMPlatform)
  .in(file("."))
  .settings(
    name := "zeno",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "io.netty" % "netty-all" % "4.1.25.Final",
      "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
    )
  )
  .jvmSettings()
  .jsSettings()

lazy val zenoJVM = zeno.jvm
lazy val zenoJS = zeno.js

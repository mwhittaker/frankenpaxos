import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

name := "zeno"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "io.netty" % "netty-all" % "4.1.25.Final",
  "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
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
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    ),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    PB.protoSources in Compile := Seq(file("shared/src/main"))
  )
  .jvmSettings()
  .jsSettings()

lazy val zenoJVM = zeno.jvm
lazy val zenoJS = zeno.js

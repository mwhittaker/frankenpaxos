import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

name := "zeno"

// libraryDependencies ++= Seq(
//   "ch.qos.logback" % "logback-classic" % "1.2.3",
//   "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
//   "io.netty" % "netty-all" % "4.1.25.Final",
//   "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
//   "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
// )

lazy val zeno = crossProject(JSPlatform, JVMPlatform)
  .in(file("."))
  .settings(
    name := "zeno",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.thesamet.scalapb" %%% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %%% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "io.netty" % "netty-all" % "4.1.25.Final",
      "org.scala-js" %% "scalajs-library" % scalaJSVersion % "provided",
      "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
    ),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    PB.protoSources in Compile := Seq(file("shared/src/main"))
  )
  .jvmSettings()
  .jsSettings(
    libraryDependencies += "org.scala-js" %%% "scalajs-java-time" % "0.2.5"
  )

lazy val zenoJVM = zeno.jvm
lazy val zenoJS = zeno.js

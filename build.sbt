import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

name := "frankenpaxos"

lazy val frankenpaxos = crossProject(JSPlatform, JVMPlatform)
  .in(file("."))
  .settings(
    name := "frankenpaxos",
    scalacOptions ++= Seq("-J-XX:+PreserveFramePointer"),
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % "3.7.0",
      "com.thesamet.scalapb" %%% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %%% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "io.netty" % "netty-all" % "4.1.25.Final",
      "org.scala-js" %% "scalajs-library" % scalaJSVersion % "provided",
      "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
      "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "com.github.tototoshi" %% "scala-csv" % "1.3.5"
    ),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    PB.protoSources in Compile := Seq(
      file("shared/src/main"),
      file("jvm/src/main")
    ),
  )
  .jsSettings(
    libraryDependencies += "org.scala-js" %%% "scalajs-java-time" % "0.2.5"
  )

lazy val frankenpaxosJVM = frankenpaxos.jvm
lazy val frankenpaxosJS = frankenpaxos.js

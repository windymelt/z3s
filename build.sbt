val scala3Version = "3.10.0"

lazy val root = project
  .in(file("."))
  .settings(
    organization := "dev.capslock",
    name := "z3s",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all"),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.7.1",
      "co.fs2" %% "fs2-core" % "3.14.0",
      "co.fs2" %% "fs2-io" % "3.14.0",
      "co.fs2" %% "fs2-scodec" % "3.14.0",
      "org.scodec" %% "scodec-core" % "2.3.3",
      "io.bullet" %% "borer-core" % "1.18.0",
      "com.monovore" %% "decline-effect" % "2.6.2",
      "software.amazon.awssdk" % "s3" % "2.57.2",
      "com.github.luben" % "zstd-jni" % "1.5.7-24",
      "org.tukaani" % "xz" % "1.12",
      "org.apache.commons" % "commons-compress" % "1.28.0",
      "org.scalatest" %% "scalatest" % "3.2.20" % Test,
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.8.0" % Test,
      "org.typelevel" %% "cats-effect-testkit" % "3.7.1" % Test
    ),
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "dev" / "capslock" / "z3s" / "BuildInfo.scala"
      IO.write(
        file,
        s"""package dev.capslock.z3s
           |
           |object BuildInfo:
           |  val version: String = "${version.value}"
           |""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Test / fork := true,
    // zstd-jniがネイティブライブラリを読み込むため
    Test / javaOptions += "--enable-native-access=ALL-UNNAMED",
    run / javaOptions += "--enable-native-access=ALL-UNNAMED",
    run / fork := true,
    run / connectInput := true,
    assembly / mainClass := Some("dev.capslock.z3s.Main"),
    assembly / assemblyJarName := "z3s.jar",
    assembly / packageOptions += Package.ManifestAttributes("Enable-Native-Access" -> "ALL-UNNAMED"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
      case "module-info.class"                                         => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties")        => MergeStrategy.first
      case x =>
        val old = (assembly / assemblyMergeStrategy).value
        old(x)
    }
  )

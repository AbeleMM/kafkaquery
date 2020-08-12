lazy val scala212 = "2.12.12"
lazy val supportedScalaVersions = List(scala212)

ThisBuild / scalaVersion := scala212


parallelExecution in Test := false

/// PROJECTS

val projectPrefix = "codefeedr-"
val utilPrefix = projectPrefix + "util-"

lazy val root = (project in file("."))
  .settings(settings)
  .aggregate(
    kafkatime,
    utilSchemaExposure
  )

lazy val kafkatime = (project in file("kafkatime"))
  .settings(
    name := "kafkatime",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.kafkaClient,
      dependencies.flinkKafka,
      dependencies.flinkClients,
      dependencies.flinkTableBridge,
      dependencies.flinkTablePlanner,
      dependencies.flinkJson,
      dependencies.avro4s,
      dependencies.scopt,
      dependencies.embeddedKafka,
      dependencies.flinkTestUtils,
      dependencies.flinkRuntime,
      dependencies.flinkStreamingJava
    ),
    packMain := Map("codefeedr" -> "org.codefeedr.kafkatime.CLI")
  ).dependsOn(utilSchemaExposure)
  .enablePlugins(PackPlugin)

lazy val utilSchemaExposure = (project in file("codefeedr-util/schema-exposure"))
  .settings(
    name := utilPrefix + "schema-exposure",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.avro,
      dependencies.zookeeper,
      dependencies.redis,
      dependencies.embeddedKafka,
      dependencies.embeddedRedis
    )
  )

lazy val dependencies =
  new {
    val flinkVersion       = "1.11.1"
    val log4jVersion       = "2.11.0"
    val log4jScalaVersion  = "11.0"


    val loggingApi         = "org.apache.logging.log4j"   % "log4j-api"                      % log4jVersion
    val loggingCore        = "org.apache.logging.log4j"   % "log4j-core"                     % log4jVersion      % Runtime
    val loggingScala       = "org.apache.logging.log4j"  %% "log4j-api-scala"                % log4jScalaVersion

    val flink              = "org.apache.flink"          %% "flink-scala"                    % flinkVersion      % Provided
    val flinkStreaming     = "org.apache.flink"          %% "flink-streaming-scala"          % flinkVersion      % Provided
    val flinkKafka         = "org.apache.flink"          %% "flink-connector-kafka"          % flinkVersion
    val flinkClients       = "org.apache.flink"          %% "flink-clients"                  % flinkVersion

    val redis              = "net.debasishg"             %% "redisclient"                    % "3.6"
    val kafkaClient        = "org.apache.kafka"           % "kafka-clients"                  % "2.4.0"
    val zookeeper          = "org.apache.zookeeper"       % "zookeeper"                      % "3.4.9"

    val scalactic          = "org.scalactic"             %% "scalactic"                      % "3.1.2"           % Test
    val scalatest          = "org.scalatest"             %% "scalatest"                      % "3.1.2"           % Test
    val scalamock          = "org.scalamock"             %% "scalamock"                      % "4.1.0"           % Test
    val mockito            = "org.mockito"               %% "mockito-scala"                  % "1.14.7"          % Test
    val embeddedRedis      = "com.github.sebruck"        %% "scalatest-embedded-redis"       % "0.3.0"           % Test

    val embeddedKafka      = "io.github.embeddedkafka"   %% "embedded-kafka"                 % "2.4.0"           % Test

    val avro               = "org.apache.avro"            % "avro"                           % "1.8.2"
    val avro4s             = "com.sksamuel.avro4s"       %% "avro4s-core"                    % "3.1.0"

    val flinkTableBridge   = "org.apache.flink"          %% "flink-table-api-scala-bridge"   % flinkVersion      % Provided
    val flinkTablePlanner  = "org.apache.flink"          %% "flink-table-planner-blink"      % flinkVersion      % Provided
    val flinkJson          = "org.apache.flink"           % "flink-json"                     % flinkVersion

    val flinkTestUtils     = "org.apache.flink"          %% "flink-test-utils"               % flinkVersion      % Test classifier "tests"
    val flinkRuntime       = "org.apache.flink"          %% "flink-runtime"                  % flinkVersion      % Test classifier "tests"
    val flinkStreamingJava = "org.apache.flink"          %% "flink-streaming-java"           % flinkVersion      % Test classifier "tests"

    val scopt              = "com.github.scopt"          %% "scopt"                          % "3.7.1"
  }

lazy val commonDependencies = Seq(
  dependencies.flink,
  dependencies.flinkStreaming,

  dependencies.loggingApi,
  dependencies.loggingCore,
  dependencies.loggingScala,

  dependencies.scalactic,
  dependencies.scalatest,
  dependencies.scalamock,
  dependencies.mockito
)

// SETTINGS

lazy val settings = commonSettings

lazy val commonSettings = Seq(
  test in assembly := {},
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "confluent"                               at "https://packages.confluent.io/maven/",
    "Apache Development Snapshot Repository"  at "https://repository.apache.org/content/repositories/snapshots/",
    "Artima Maven Repository"                 at "https://repo.artima.com/releases",
    Resolver.mavenLocal,
    Resolver.jcenterRepo
  )
)

lazy val compilerOptions = Seq(
  //  "-unchecked",
  //  "-feature",
  //  "-language:existentials",
  //  "-language:higherKinds",
  //  "-language:implicitConversions",
  //  "-language:postfixOps",
  //  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*)  => MergeStrategy.discard
    case "log4j.properties"             => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

// MAKING FLINK WORK

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)



name := "t2-sbt"

version := "0.1"

scalaVersion := "2.12.8"
sparkVersion := "2.4.4"
autoScalaLibrary := false

lazy val root = (project in file(".")).enablePlugins(PlayScala)
mainClass in assembly := Some("play.core.server.ProdServerStart")
fullClasspath in assembly += Attributed.blank(PlayKeys.playPackageAssets.value)


sparkComponents ++= Seq(
"sql",
"core"
)

libraryDependencies ++= Seq(
"org.opencypher"   %    "morpheus-spark-cypher"   %   "0.4.2" ,
"io.circe"         %%   "circe-yaml"              %   "0.12.0",
"io.circe"         %%   "circe-parser"            %   "0.12.3",
"io.circe"         %%   "circe-core"              %   "0.12.3",
"io.circe"         %%   "circe-generic"           %   "0.12.3",
"com.typesafe"      %   "config"                  %   "1.4.0"
)
libraryDependencies += guice

assemblyMergeStrategy in assembly := {
  // Take care of duplicate file from opencypher libs
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case manifest if manifest.contains("MANIFEST.MF") =>
    // We don't need manifest files since sbt-assembly will create
    // one with the given settings
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    // Keep the content for all reference-overrides.conf files
    MergeStrategy.concat
  case moduleInfo if moduleInfo.contains("module-info.class") =>
    MergeStrategy.discard
  case x =>
    // For all the other files, use the default sbt-assembly merge strategy
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


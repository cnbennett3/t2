

name := "t2-sbt"

version := "0.1"

scalaVersion := "2.12.12"
sparkVersion := "2.4.4"
sparkComponents ++= Seq(
  "sql",
  "core"
)
libraryDependencies ++= Seq(
  "org.opencypher"   %    "morpheus-spark-cypher"   %   "0.4.2"  %  "provided",
  "io.circe"         %%   "circe-yaml"              %   "0.12.0",
  "io.circe"         %%   "circe-parser"            %   "0.12.3",
  "io.circe"         %%   "circe-core"              %   "0.12.3",
  "io.circe"         %%   "circe-generic"           %   "0.12.3"
)
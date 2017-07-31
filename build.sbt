scalaVersion := "2.12.2"

assemblyJarName in assembly := "accessions_report.jar"

mainClass in assembly := Some("aspace.Main")

libraryDependencies ++= Seq(
  "org.json4s" % "json4s-jackson_2.12" % "3.5.0",
  "org.json4s" % "json4s-native_2.12" % "3.5.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.3",
  "com.typesafe" % "config" % "1.3.1"
)

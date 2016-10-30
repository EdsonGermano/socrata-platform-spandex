resolvers ++= Seq(
  "socrata releases" at "https://repo.socrata.com/artifactory/libs-release"
)

addSbtPlugin("com.mojolly.scalate" % "xsbt-scalate-generator" % "0.5.0")
addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "1.1.0")
addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.4.0")
addSbtPlugin("com.socrata" %% "socrata-sbt-plugins" % "1.5.6")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.1.15")

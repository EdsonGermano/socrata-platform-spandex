import com.earldouglas.xwp.XwpPlugin
import com.mojolly.scalate.ScalatePlugin.ScalateKeys._
import com.mojolly.scalate.ScalatePlugin._
import org.scalatra.sbt._
import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys.buildInfoPackage
import sbtbuildinfo.BuildInfoPlugin
import scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages

object SpandexBuild extends Build {
  val Name = "com.socrata.spandex"
  val JettyListenPort = 8042 // required for container embedded jetty

  lazy val commonSettings = Seq(
    fork in Test := true,
    testOptions in Test += Tests.Argument("-oF"),
    resolvers ++= Deps.resolverList
  )

  lazy val build = Project(
    "spandex",
    file("."),
    settings = commonSettings
  ).aggregate(spandexCommon, spandexHttp, spandexSecondary)
    .dependsOn(spandexCommon, spandexHttp, spandexSecondary)

  lazy val perf = Project(
    "perf",
    file("./spandex-perf/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.common ++ Deps.perf
    )
  ).enablePlugins(JmhPlugin).dependsOn(build % "compile;test->test")

  lazy val spandexCommon = Project (
    "spandex-common",
    file("./spandex-common/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondaryFiltered,
      fullClasspath in Runtime += Attributed.blank(baseDirectory.value / ".." / "esconfigs")
    )
  ).disablePlugins(JmhPlugin)

  lazy val spandexHttp = Project (
    "spandex-http",
    file("./spandex-http/"),
    settings = commonSettings ++ ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      buildInfoPackage := "com.socrata.spandex.http",
      libraryDependencies ++= Deps.socrata ++ Deps.http ++ Deps.test ++ Deps.common,
      coverageExcludedPackages := "<empty>;templates.*;" + coverageExcludedPackages.value,
      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext",
                importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
      },
      resourceGenerators in Compile <+= (resourceManaged, baseDirectory) map {
        (managed, base) =>
          val webappBase = base / "src" / "main" / "webapp"
          for {
            (from, to) <- webappBase ** "*" pair rebase(webappBase, managed / "main" / "webapp")
          } yield {
            Sync.copy(from,to)
            to
          }
      }
    ) ++ XwpPlugin.jetty(port = JettyListenPort)
  ).dependsOn(spandexCommon % "compile;test->test")
    .disablePlugins(JmhPlugin)
    .enablePlugins(BuildInfoPlugin)

  lazy val spandexSecondary = Project (
    "spandex-secondary",
    file("./spandex-secondary/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondary,
      fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d / "config") },
      fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d / "config") }
    )
  ).dependsOn(spandexCommon % "compile;test->test")
    .disablePlugins(JmhPlugin)

  lazy val gitSha = Process(Seq("git", "describe", "--always", "--dirty", "--long", "--abbrev=10")).!!.stripLineEnd
}

object Deps {
  val ScalatraVersion = "2.4.0.RC1"
  val JettyVersion = "9.2.10.v20150310"

  lazy val resolverList = Seq(
    "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases", // scalaz-stream used in scalatra 2.4.x
    "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"
  )

  lazy val socrata = Seq(
    "com.rojoma" %% "rojoma-json-v3" % "3.3.0",
    "com.rojoma" %% "simple-arm" % "1.2.0",
    "com.rojoma" %% "simple-arm-v2" % "2.1.0",
    "com.socrata" %% "soql-types" % "0.5.2"
      excludeAll(ExclusionRule(organization = "com.rojoma"),
                 ExclusionRule(organization = "commons-io"))
  )
  lazy val http = Seq(
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
    "org.scalatra" %% "scalatra-metrics" % ScalatraVersion,
    "ch.qos.logback" % "logback-classic" % "1.1.3" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % JettyVersion % "container;compile",
    "org.eclipse.jetty" % "jetty-plus" % JettyVersion % "container"
  )
  lazy val test = Seq(
    "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test"
  )
  lazy val perf = Seq()
  lazy val common = Seq(
    "javax.servlet" % "javax.servlet-api" % "3.1.0",
    "com.typesafe" % "config" % "1.2.1",
    "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
    "commons-io" % "commons-io" % "2.4",
    "org.elasticsearch" % "elasticsearch" % "1.7.2"
  )
  lazy val secondary = Seq(
    "com.socrata" %% "secondarylib" % "3.4.10"
  )
  lazy val secondaryFiltered =
    secondary.map(_.exclude("org.slf4j", "slf4j-log4j12")
                   .excludeAll(ExclusionRule(organization = "com.rojoma")))
}

import sbt._
import Keys._
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.earldouglas.xsbtwebplugin.PluginKeys._
import com.earldouglas.xsbtwebplugin.WebPlugin._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._

object BuildParameters {
  val Organization = "com.socrata"
  val Name = "spandex"
  val Version = "0.1.0-SNAPSHOT"
  val ScalaVersion = "2.10.3"
  val ScalatraVersion = "2.2.2"
  val Conf = config("container")
  val ListenPort = 8042
}

object SpandexBuild extends Build {
  import BuildParameters._
  import Dependencies._

  lazy val project = Project (
    "spandex",
    file("."),
    settings = ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      organization := Organization,
      name := Name,
      version := Version,
      scalaVersion := ScalaVersion,
      port in Conf := ListenPort,
      resolvers += Classpaths.typesafeReleases,
      libraryDependencies ++= socrataDeps ++ scalatraDeps ++ jettyDeps ++ rojomaDeps ++ testDeps,
      scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature"),
      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
      }
    )
  )
}

object Dependencies {
  import BuildParameters._

  lazy val socrataDeps = Seq(
  )
  lazy val scalatraDeps = Seq(
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
    "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test"
  )
  lazy val jettyDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % "9.1.5.v20140505" % "container",
    "org.eclipse.jetty" % "jetty-plus" % "9.1.5.v20140505" % "container",
    "javax.servlet" % "javax.servlet-api" % "3.1.0"
  )
  lazy val rojomaDeps = Seq(
    "com.rojoma" %% "rojoma-json" % "2.4.3",
    "com.rojoma" %% "simple-arm" % "1.2.0"
  )
  lazy val testDeps = Seq(
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
    "org.scalatest" %% "scalatest" % "2.1.0" % "test"
  )
}

package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import play.api.libs.json.Reads._
import play.api.libs.json._
import org.renci.t2.core.Core
import org.renci.t2.util.{KGXMetaData, Version}
import play.api.libs.json.JsValue
import services.T2Service
/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class IndexController @Inject()(val controllerComponents: ControllerComponents, t2Service: T2Service) extends BaseController {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index() = Action { implicit request: Request[AnyContent] =>
    val kgxServerRoot = "https://stars.renci.org/var/kgx_data" //Config.getConfig("kgxFiles.host")
    val kgxFilesGrabber: KGXMetaData = new KGXMetaData(kgxServerRoot)
    val versionMetadata: Version = kgxFilesGrabber.getVersionData("v0.1")
    Ok(versionMetadata.version)
  }

  def runCount() = Action { implicit request: Request[AnyContent] =>
    val core:Core = t2Service.initializeT2Core()
    val graph = core.makeGraph("v0.1")
    core.runCypherAndShow(cypherQuery = "MATCH (c) return count(c)", graph = graph)
    Ok("count is done")
  }
}

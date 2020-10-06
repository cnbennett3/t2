package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import play.api.libs.json.Reads._
import play.api.libs.json._
import org.renci.t2.core.Core
import org.renci.t2.util.{KGXMetaData, Version}
import play.api.libs.json.JsValue

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class IndexController @Inject()(val controllerComponents: ControllerComponents) extends BaseController {

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
    val core:Core = new Core()
    val graph = core.makeGraph("v0.1")
    core.runCypherAndShow(cypherQuery = "MATCH (c) return count(c)", graph = graph)
    Ok("count is done")
  }



  def runQuery() = Action { implicit request: Request[AnyContent] =>
    val core:Core = new Core()
//    val version = request.getQueryString("version")
//    val bodyJson: JsResult[QueryModel] = request.body.validate[QueryModel]
    val version = "v0.1"
    val graph = core.makeGraph(version = version)
//    result = bodyJson.fold()
    val res = core.runCypherAndReturnJsonString(graph=graph,cypherQuery="MATCH (c:disease) return c")
    Ok(res)
  }


}

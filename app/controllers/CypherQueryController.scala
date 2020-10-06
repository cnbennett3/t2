package controllers

import javax.inject.{Inject, Singleton}
import org.renci.t2.core.Core
import org.renci.t2.util.{KGXMetaData, Version}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import models.CypherQuery
import play.api.libs.json.Reads._
import play.api.libs.json._


/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class CypherQueryController @Inject()(val controllerComponents: ControllerComponents) extends BaseController {

  implicit  val queryReads: Reads[CypherQuery] = ((JsPath \ "query").read[String])map(CypherQuery(_))

  def runQuery(version: Option[String]) = Action(parse.json) { request =>
    val queryBody = request.body.validate[CypherQuery]
    val datasetVersion = version.get
    queryBody.fold(
      errors => {
        BadRequest(Json.obj("message" -> JsError.toJson(errors)))
      },
      query => {
        val core:Core = new Core()
        val graph = core.makeGraph(version = datasetVersion)
        val res = core.runCypherAndReturnJsonString(graph=graph,cypherQuery=query.query)
        Ok(res)
      }
    )

  }


}

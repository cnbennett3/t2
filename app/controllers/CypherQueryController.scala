package controllers

import javax.inject.{Inject, Singleton}
import org.renci.t2.core.Core
import org.renci.t2.util.{KGXMetaData, Version}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import models.CypherQuery
import play.api.libs.json.Reads._
import play.api.libs.json._
import services.T2Service

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class CypherQueryController @Inject()(val controllerComponents: ControllerComponents, t2Service: T2Service) extends BaseController {

  implicit  val queryReads: Reads[CypherQuery] = ((JsPath \ "query").read[String])map(CypherQuery(_))

  def runQuery(version: Option[String]) = Action(parse.json) { request =>
    val queryBody = request.body.validate[CypherQuery]
    val datasetVersion = version.get
    queryBody.fold(
      errors => {
        BadRequest(Json.obj("message" -> JsError.toJson(errors)))
      },
      query => {
        val res = t2Service.runCypher(cypher=query.query)
        Ok(res)
      }
    )

  }


}

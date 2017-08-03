package edu.nyu.libraries.acm

import java.io.FileWriter

import com.typesafe.config.ConfigFactory
import java.net.URI
import java.io.File

import org.apache.http.client.methods.{HttpGet, RequestBuilder}
import org.apache.http.conn.HttpHostConnectException
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ScallopCLI(arguments: Seq[String]) extends ScallopConf(arguments) {
  val repositoryId = opt[Int](required = true)
  val fy = opt[Int]()
  verify()
}

case class AccConfig(repositoryId: Int, username: String, password: String, aspaceUrl: String, csvWriter: FileWriter, errorWriter: FileWriter, var key: Option[String], aspaceClient: CloseableHttpClient);

object Main extends App {
  val header = "X-ArchivesSpace-Session"

  implicit val formats = DefaultFormats

  val config = ConfigFactory.parseFile(new File("aspace.conf"));
  val cli = new ScallopCLI(args)
  var errors = 0
  var success = 0

  var accConfig = new AccConfig(
    cli.repositoryId(),
    config.getString("aspace.username"),
    config.getString("aspace.password"),
    config.getString("aspace.url"),
    new FileWriter(new File("output.csv")),
    new FileWriter(new File("error.log")),
    None,
    HttpClients.createDefault())

  accConfig.key = getKey()
  accConfig.csvWriter.write("\"id1\",\"id2\",\"id3\",\"id4\",\"title\",\"accession_date\",\"extent_num\",\"extent_type\"\n")
  accConfig.csvWriter.flush()

  val accessions = getAccessions()
  println(s"Exporting ${accessions.size} accessions")

  accessions.foreach{ i =>
    getAccession(i)
  }

  println("export complete")
  println(s"export completed, $success records exported successfully, $errors errors")

  def getKey(): Option[String] = {
    println("getting key")
    try {
      val authenticate = RequestBuilder.post().setUri(new URI(accConfig.aspaceUrl + s"/users/${accConfig.username}/login")).addParameter("password", accConfig.password).build
      val response = accConfig.aspaceClient.execute(authenticate)
      val entity = response.getEntity
      val content = entity.getContent
      val data = scala.io.Source.fromInputStream(content).mkString
      val jarray = parse(data)
      val askey = (jarray \ "session").extract[String]
      EntityUtils.consume(entity)
      response.close()
      Some(askey)
    } catch {
      case h: HttpHostConnectException => {
        System.err.println("Cannot connect to ArchivesSpace, Exiting");
        accConfig.errorWriter.write("Cannot connect to ArchivesSpace, Exiting");
        None
      }
    }
  }

  def getAccessions(): List[Int] = {
    println("getting list of accessions")
    val accUrl = s"/repositories/${accConfig.repositoryId}/accessions"
	  val get = new HttpGet(accConfig.aspaceUrl + accUrl + "?all_ids=true")
	  get.addHeader(header, accConfig.key.get)
	  val response = accConfig.aspaceClient.execute(get)
	  val entity = response.getEntity
	  val content = entity.getContent
	  val data = scala.io.Source.fromInputStream(content).mkString
	  val jarray = parse(data)
	  val accs = jarray.extract[List[Int]]
	  EntityUtils.consume(entity)
	  response.close()
	  accs
  }

  def getAccession(id: Int) {
      val accUrl = s"/repositories/${accConfig.repositoryId}/accessions"
      val get = new HttpGet(accConfig.aspaceUrl + accUrl + "/" + id)
      get.addHeader(header, accConfig.key.get)
      val response = accConfig.aspaceClient.execute(get)
      val entity = response.getEntity
      val content = entity.getContent
      val data = scala.io.Source.fromInputStream(content).mkString
      val json = parse(data)
      var title_err: String = ""
      try {
      val id0 = json \ "id_0"
      val id1 = json \ "id_1"
      val id2 = json \ "id_2"
      val id3 = json \ "id_3"
      val title = (json \ "title").extract[String]
      title_err = title
      val aDate = (json \ "accession_date").extract[String]
      val extents = json \ "extents"
      val num = (extents(0) \ "number").extract[String].toDouble
      val extentType = (extents(0) \ "extent_type").extract[String]
      accConfig.csvWriter.write("\"" + getString(id0) + "\",\"" + getString(id1) + "\",\"" + getString(id2) + "\",\"" + getString(id3) + "\",\"" +  title + "\",\"" + aDate + "\",\"" + num + "\",\"" + extentType + "\"\n")
      accConfig.csvWriter.flush()
      EntityUtils.consume(entity)
      response.close()
      success = success + 1

	  } catch {
	    case e: Exception => {
        accConfig.errorWriter.write(s"$title_err \n")
        EntityUtils.consume(entity)
        response.close()
        errors = errors + 1
      }
	  }
  }

  def getString(j: JValue): String = {
    try {
      j.extract[String]	
    } catch {
      case e: Exception => ""
    }
  }

}

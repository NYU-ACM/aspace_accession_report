package edu.nyu.libraries.acm

import java.io.FileWriter

import com.typesafe.config.ConfigFactory
import java.net.URI
import java.io.File

import edu.nyu.libraries.acm.Main.accConfig
import org.apache.http.client.methods.{HttpGet, RequestBuilder}
import org.apache.http.conn.HttpHostConnectException
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}

case class AccConfig(repositoryId: Int, username: String, password: String, aspaceUrl: String, var key: Option[String], aspaceClient: CloseableHttpClient);
case class Accessions(count: Int, success: Int, errors: Int)

object Main extends App with WriterSupport {
  val header = "X-ArchivesSpace-Session"

  implicit val formats = DefaultFormats

  val cli = new ScallopCLI(args)
  val config = getConfig()

  var accConfig = new AccConfig(
    cli.repositoryId(),
    config.getString("aspace.username"),
    config.getString("aspace.password"),
    config.getString("aspace.url"),
    None,
    HttpClients.createDefault())

  getKey() match {
    case key: Some[String] => accConfig.key = key
    case None => {
      System.err.println("Cannot connect to ArchivesSpace, exiting");
      System.exit(1)
    }
  }


  val accessions = getAccessionIds()

  println(s"Exporting ${accessions.size} accessions")

  val results = getAccessions(0, 0, 0)

  println("export complete")
  println(s"export completed, ${results.success} records exported successfully, ${results.errors} errors")

  def getConfig(): com.typesafe.config.Config = {
    val configFile = new File("aspace.conf")
    configFile.exists match {
      case true => ConfigFactory.parseFile(configFile)
      case false => ConfigFactory.load()
    }
  }

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
      case e: Exception => {
        errorWriter.write(s"Cannot connect to ArchivesSpace\n$e");
        None
      }
    }
  }

  def getAccessionIds(): List[Int] = {
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

  def getAccessions(count: Int, success: Int, errors: Int): Accessions = {
      val accUrl = s"/repositories/${accConfig.repositoryId}/accessions"
      val get = new HttpGet(accConfig.aspaceUrl + accUrl + "/" + accessions(count))
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
      csvWriter.write(s"\"${getString(id0)}\",\"${getString(id1)}\",\"${getString(id2)}\",\"${getString(id3)}\",\"$title\",\"$aDate\",\"$num\",\"$extentType\"\n")
      csvWriter.flush()
      EntityUtils.consume(entity)
      response.close()

      count == accessions.size - 1 match {
        case true => Accessions(count, success + 1, errors)
        case false => getAccessions(count +1, success + 1, errors)
      }
	  } catch {
      case e: Exception => {
        errorWriter.write(s"$title_err \n")
        EntityUtils.consume(entity)
        response.close()
        count == accessions.size - 1 match {
          case true => Accessions(count, success, errors + 1)
          case false => getAccessions(count +1, success, errors + 1)
        }
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

trait WriterSupport {
  val errorWriter = new FileWriter(new File("errors.log"))
  val csvWriter = new FileWriter(new File("output.csv"))
  csvWriter.write("\"id1\",\"id2\",\"id3\",\"id4\",\"title\",\"accession_date\",\"extent_num\",\"extent_type\"\n")
  csvWriter.flush()
}

class ScallopCLI(arguments: Seq[String]) extends ScallopConf(arguments) {
  val repositoryId = opt[Int](required = true)
  val fy = opt[Int]()
  verify()
}


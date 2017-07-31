package aspace

import java.io.FileWriter

import com.typesafe.config.ConfigFactory
import java.net.URI
import java.io.File
import org.apache.http.client.methods.{HttpGet, RequestBuilder}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

object Main extends App {
	
  implicit val formats = DefaultFormats

  val conf = ConfigFactory.load("application.conf")
  val header = "X-ArchivesSpace-Session"
  val url = conf.getString("aspace.url")
  println(url)
  val accUrl = s"/repositories/${conf.getString("aspace.repoId")}/accessions"
  val client = HttpClients.createDefault
  val output = new java.io.File(conf.getString("aspace.csvFilename"))
  val writer = new java.io.FileWriter(output)
  val errorWriter = new java.io.FileWriter("errors.log")

  val key = getKey()
  val accs = getAccessions
  accs.foreach { accId => getAccession(accId) }

  writer.close
  errorWriter.close
  client.close

  def getKey(): String = {
    println("getting key")
  	val authenticate = RequestBuilder.post().setUri(new URI(url + s"/users/${conf.getString("aspace.username")}/login")).addParameter("password", conf.getString("aspace.password")).build
  	val response = client.execute(authenticate)
  	val entity = response.getEntity
	  val content = entity.getContent
	  val data = scala.io.Source.fromInputStream(content).mkString
	  val jarray = parse(data)
	  val askey = (jarray \ "session").extract[String]
	  EntityUtils.consume(entity)
	  response.close
	  askey
  }

  def getAccessions(): List[Int] = {
    println("getting accessions")
	  val get = new HttpGet(url + accUrl + "?all_ids=true")
	  get.addHeader(header, key)
	  val response = client.execute(get)
	  val entity = response.getEntity
	  val content = entity.getContent
	  val data = scala.io.Source.fromInputStream(content).mkString
	  val jarray = parse(data)
	  val accs = jarray.extract[List[Int]]
	  EntityUtils.consume(entity)
	  response.close
	  accs
  }

  def getAccession(id: Int): Unit = {
    println(s"getting: $id")
  	val get = new HttpGet(url + accUrl + "/" + id)
  	get.addHeader(header, key)
	  val response = client.execute(get)
	  val entity = response.getEntity
	  val content = entity.getContent
	  val data = scala.io.Source.fromInputStream(content).mkString
	  val json = parse(data)
	  var title_err: String = ""
	  try {
	  val id0 = (json \ "id_0")
	  val id1 = (json \ "id_1")
	  val id2 = (json \ "id_2")
	  val id3 = (json \ "id_3")
	  val title = (json \ "title").extract[String]
	  title_err = title
	  val aDate = (json \ "accession_date").extract[String]
	  val extents = json \ "extents"
	  val num = (extents(0) \ "number").extract[String].toDouble
	  val extentType = (extents(0) \ "extent_type").extract[String]
	  writer.write("\"" + getString(id0) + "\",\"" + getString(id1) + "\",\"" + getString(id2) + "\",\"" + getString(id3) + "\",\"" +  title + "\",\"" + aDate + "\",\"" + num + "\",\"" + extentType + "\"\n")
	  writer.flush
	  } catch {
	    case e: Exception => errorWriter.write(title_err)
	  }
	  EntityUtils.consume(entity)
	  response.close
  }

  def getString(j: JValue): String = {
    try {
      j.extract[String]	
    } catch {
      case e: Exception => ""
    }
  }
  
}

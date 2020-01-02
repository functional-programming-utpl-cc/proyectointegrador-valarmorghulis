import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap
import scala.math.Ordering
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import kantan.csv.java8.localDateTimeDecoder
import upickle.default._


val path2DataFile = "/Users/ods_1_1.csv"
val formatDateTime = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")


case class Tweet(
                  idStr: String,
                  fromUser: String,
                  text: String,
                  createdAt: String,
                  time: LocalDateTime,
                  geoCoordinates: String,
                  userLang: String,
                  inReply2UserId: String,
                  inReply2ScreenName: String,
                  fromUserId: String,
                  inReply2StatusId: String,
                  source: String,
                  profileImageURL: String,
                  userFollowersCount: Either[String, Int],
                  userFriendsCount: Either[String, Int],
                  userLocation: String,
                  statusURL: String,
                  entitiesStr: String
                )
implicit val decoder : CellDecoder[LocalDateTime] = localDateTimeDecoder(formatDateTime)
val dataSource = new File(path2DataFile).readCsv[List, Tweet](rfc.withHeader)
val values = dataSource.collect({ case Right(tweet) => tweet })
val fromUsersList = values.map(tweet => tweet.fromUser)
val fromUserList = values.map(tweet=>tweet.fromUser)
println(fromUserList.toSeq.size)
val countFromUsers = fromUserList.groupBy(identity).map({case(k,v)=>(k,v.length)})
val activeUserList= ListMap(countFromUsers.toSeq.sortWith(_._2 > _._2):_*)
activeUserList.foreach(println)

val activeDays = ListMap(values.map(tweet => tweet.time.getDayOfMonth).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)

val activeHours = ListMap(values.map(tweet => tweet.time.getHour).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)
//hecho por mi >:v
val filterTable = values.filter(x=> x.text.startsWith("RT"))
val filterTable2 = values.filterNot(x=> x.text.startsWith("RT"))

val activeHours2_rt = ListMap(filterTable.map(tweet => tweet.time.getHour).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)
val activeHours2 = ListMap(filterTable2.map(tweet => tweet.time.getHour).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)
val totalActiveHours = List(activeHours2_rt.map(x=>x),activeHours2.map(x=>x)).flatten

//escribir dias activos
val out = java.io.File.createTempFile("activHoursRT.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("hour","count"))
activeHours2_rt.foreach(writer.write(_))
writer.close()
//escribir horas activas
val out2 = java.io.File.createTempFile("activrHours.csv","csv")
val writer2 = out2.asCsvWriter[(Int,Int)](rfc.withHeader("hour","count"))
activeHours2.foreach(writer2.write(_))
writer2.close()
//escribir3
val out3 = java.io.File.createTempFile("activrHoursf.csv","csv")
val writer3 = out3.asCsvWriter[(Int,Int)](rfc.withHeader("hour","count"))
totalActiveHours.foreach(writer3.write(_))
writer3.close()

//

val numHashtag = values.map(tweet => ujson.read(tweet.entitiesStr).obj("hashtags").arr.length).groupBy(identity).map({case(k,v)=>
  (k,v.length)})
//escribir4
/*
val out4 = java.io.File.createTempFile("hashtags.csv","csv")
val writer4 = out4.asCsvWriter[(Int,Int)](rfc.withHeader("hashtags","count"))
numHashtag.foreach(writer4.write(_))
writer4.close()*/
values.flatMap(tweet => ujson.read(tweet.entitiesStr).obj("hashtags").arr).map(
  ht=> ht.obj("text").str).groupBy(identity).map({case(k,v) => (k,v.length)}).foreach(println)

//listOfList.flatMap(lista => lista)

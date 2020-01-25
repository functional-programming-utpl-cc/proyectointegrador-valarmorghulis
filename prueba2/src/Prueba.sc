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
import scala.util.Try
import scala.util.Success
import scala.util.Failure

val path2DataFile = "/Users/ods_1_1.csv"
val formatDateTime = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
//userFollowersCount: Either[String, Int],
//userFriendsCount: Either[String, Int],
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
                  userFollowersCount: Double,
                  userFriendsCount: Double,
                  userLocation: String,
                  statusURL: String,
                  entitiesStr: String
                )
implicit val decoder : CellDecoder[LocalDateTime] = localDateTimeDecoder(formatDateTime)
val dataSource = new File(path2DataFile).readCsv[List, Tweet](rfc.withHeader)
val values = dataSource.collect({ case Right(tweet) => tweet })
/*
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


      //primera consulta = dias activos de tweets y retweets
val filterTable = values.filter(x=> x.text.startsWith("RT"))
val filterTable2 = values.filterNot(x=> x.text.startsWith("RT"))

val activeDays = ListMap(filterTable2.map(tweet => tweet.time.getDayOfMonth).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)

val activeDays_rt = ListMap(filterTable.map(tweet => tweet.time.getDayOfMonth).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)
      //segunda consulta = horas activas de tweets y retweets

val activeHours_rt = ListMap(filterTable.map(tweet => tweet.time.getHour).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)

val activeHours = ListMap(filterTable2.map(tweet => tweet.time.getHour).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)
      //tercera consulta dispositivos usados
//escribir los dispositivos mas ultilizados
val devicesn = values.map(x=> x.source.split("<")).flatten
val devices2n = devicesn.map(x=>x.split(">").last)
val devices2 = ListMap(devices2n.groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)

val devices = ListMap(values.map(tweet => tweet.source.split(" ").last).groupBy(identity).map(
  {case(k,v) => (k, v.length)}).toSeq.sortWith(
  _._2 > _._2):_*)



    //cuarta consulta distribucion de hashtags
val numHashtag = values.map(tweet => ujson.read(tweet.entitiesStr).obj("hashtags").arr.length).groupBy(
      identity).map({case(k,v)=>
      (k,v.length)})



    //quinta consulta distribucion de menciones
val distribucion_user_mentions = values.map(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr.length).
  groupBy(identity).map({case(k,v)=>
      (k,v.length)})


    //sexta consulta distribucion de urls
val distribucion_urls = values.map(tweet => ujson.read(tweet.entitiesStr).obj("urls").arr.length).
  groupBy(identity).map({case(k,v)=> (k,v.length)})


    //septima consulta coeficiente de pearson
    
//val count = List(values.map(tweet => tweet.userFriendsCount match{case Left(s) => s})).flatten
//val count2 = List(values.map(tweet => tweet.userFollowersCount match{case Left(s) => s})).flatten
//count2.map(x=> x.toIntOption)
val distribucion = values.map(tweet => tweet.userFriendsCount.toInt) zip values.map(
  tweet => tweet.userFollowersCount.toInt)
val x2 = (a:List[(Int,Int)]) => a.map(x=>x._1*1.0)
val y2 = (a:List[(Int,Int)]) => a.map(x=>x._2*1.0)
val mediav = (a:List[(Int,Int)]) => (x2(a).map(a=> a).sum /a.length).toFloat
val mediav2 = (a:List[(Int,Int)]) => (y2(a).map(a=> a).sum /a.length).toFloat
val x_marginal = (a:List[(Int,Int)])=> Math.sqrt(x2(a).map(x=>Math.pow(x,2)).sum/a.length - Math.pow(mediav(a),2) )
val y_marginal = (a:List[(Int,Int)])=> Math.sqrt(a.map(x=>Math.pow(x._2,2)).sum/a.length - Math.pow(mediav2(a),2))
val xpory = (a:List[(Int,Int)]) => (x2(a),y2(a)).zipped.map(_*_)
val covarianza = (a:List[(Int,Int)]) =>((xpory(a)).sum / a.length) -mediav(a)*mediav2(a)
val coef = (a:List[(Int,Int)]) => covarianza(a) / (x_marginal(a)*y_marginal(a))
val coef2 = coef(distribucion)



    //octava consulta


val metodo = values.map(tweet => Try(ujson.read(tweet.entitiesStr).obj("media")) match {case Success(s)=>"success"
case Failure(f)=>"failure"}).groupBy(identity).map({case(k,v) => (k , v.length)})

//val count2 = List(values.map(tweet => tweet.userFollowersCount match{case Left(s) => s})).flatten
//count2.map(x=> x.toIntOption)


    //novena cosulta
val distribucion = values.map(tweet => tweet.userFriendsCount.toInt) zip values.map(
      tweet => tweet.userFollowersCount.toInt)

val fromUsersList = filterTable.map(tweet => tweet.fromUser).groupBy(identity).map({case(k,v) => (k , v.length)})
val fromUsersListPrueba = filterTable.map(tweet => tweet.fromUser).distinct.length
val fromUsersListRT = filterTable2.map(tweet => tweet.fromUser).groupBy(identity).map({case(k,v) => (k , v.length)})
val fromUsersListRTPrueba = filterTable2.map(tweet => tweet.fromUser).distinct.length
val todosLosUsuarios = values.map(x => x.fromUser).distinct.length
val prueba= values.map(tweet => tweet.fromUser).groupBy(identity).map({case(k,v) => (k , v.length)})

val amigos = values.map(tweet => tweet.fromUser) zip values.map(tweet => tweet.userFollowersCount)
val todosLosAmigos = values.map(x => x.userFriendsCount).length
val funcion2 = (a:List[(String,Int)],b:List[String])=>

val seguidores = values.map(tweet => tweet.fromUser) zip values.map(tweet => tweet.userFriendsCount)

val seguidores2 = seguidores.length
print(seguidores2)
val todosLosAmigos = values.map(x => x.userFollowersCount).length

    //decima consulta cuantas veces se ah mencionado un usuario

val distribucion_mentions = values.flatMap(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr).map(
  ht=> ht.obj("screen_name").str).groupBy(identity).map({case(k,v) => (k,v.length)})

//escribir primera conslta
val out = java.io.File.createTempFile("activeDays.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("day","count"))
activeDays.foreach(writer.write(_))
writer.close()

val out = java.io.File.createTempFile("activeDaysRT.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("day","count"))
activeDays_rt.foreach(writer.write(_))
writer.close()

//segunda consulta
val out = java.io.File.createTempFile("activeHours.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("day","count"))
activeHours.foreach(writer.write(_))
writer.close()

val out = java.io.File.createTempFile("activeHoursRt.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("day","count"))
activeHours_rt.foreach(writer.write(_))
writer.close()

//tercera consulta
val out = java.io.File.createTempFile("devices.csv","csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("device","count"))
devices.foreach(writer.write(_))
writer.close()


//cuarta consulta
val out = java.io.File.createTempFile("numHashtag.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("number_hashtags","count"))
numHashtag.foreach(writer.write(_))
writer.close()


//quinta consulta
val out = java.io.File.createTempFile("distribucion_user_mentions.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("number_user_mentions","count"))
distribucion_user_mentions.foreach(writer.write(_))
writer.close()


//sexta consulta
val out = java.io.File.createTempFile("distribucion_urls.csv","csv")
val writer = out.asCsvWriter[(Int,Int)](rfc.withHeader("numer_urls","count"))
distribucion_urls.foreach(writer.write(_))
writer.close()



//septima consulta
//val out = java.io.File.createTempFile("coef2.csv","csv")
//val writer = out.asCsvWriter[(Double)](rfc.withHeader("relacion_amigos_seguidores"))
//coef2.writer.write(_)
//writer.close()







//decima consulta
val out = java.io.File.createTempFile("distribucion_mentions.csv","csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("usuario","count"))
distribucion_mentions.foreach(writer.write(_))
writer.close()




//escribir4
/*ww
val out4 = java.io.File.createTempFile("hashtags.csv","csv")
val writer4 = out4.asCsvWriter[(Int,Int)](rfc.withHeader("hashtags","count"))
numHashtag.foreach(writer4.write(_))
writer4.close()*/
values.flatMap(tweet => ujson.read(tweet.entitiesStr).obj("hashtags").arr).map(
  ht=> ht.obj("text").str).groupBy(identity).map({case(k,v) => (k,v.length)}).foreach(println)

//listOfList.flatMap(lista => lista)
*/


def processFFCounters2(ffData: List[Tuple3[String,String,String]],isFollowers:Boolean):Int ={

  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isFollowers)
    (countersList.flatMap(t2 => List(t2._1)).filter(x=>x.startsWith("RT"))).length
  else
    (countersList.flatMap(t2 => List(t2._2)).filterNot(x=>x.startsWith("RT"))).length
}
val uno = values.map(tweet => (tweet.fromUser,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1, processFFCounters2(kv._2,true),processFFCounters2(kv._2, false)))



def processFFCounters(ffData: List[Tuple3[String,Double,Double]],isFollowers:Boolean):Double ={
  val avg = (nums:List[Double])=> nums.sum / nums.length
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isFollowers)
    avg(countersList.flatMap(t2 => List(t2._1)))
  else
    avg(countersList.flatMap(t2 => List(t2._2)))
}
val dos = values.map(tweet => (tweet.fromUser,tweet.userFollowersCount, tweet.userFriendsCount)).
groupBy(_._1).map(kv=>(kv._1, processFFCounters(kv._2,true),processFFCounters(kv._2, false)))

val completo =  values.map(tweet => (tweet.fromUser,tweet.userFollowersCount, tweet.userFriendsCount,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1, processFFCounters(kv._2,true),
  processFFCounters(kv._2, false)))
val bb = (a:List[(String,Int,Int)], b:[(String,Double,Double)]) => (a q)
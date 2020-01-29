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



      //primera consulta = dias activos de tweets y retweets
val filterTable = values.filter(x=> x.text.startsWith("RT"))
val filterTable2 = values.filterNot(x=> x.text.startsWith("RT"))
def tweet_rtweet_dia(ffData: List[Tuple3[Int,String,String]],isRTweet:Boolean):Int ={

  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isRTweet)
    (countersList.flatMap(t2 => List(t2._1)).filter(x=>x.startsWith("RT"))).length
  else
    (countersList.flatMap(t2 => List(t2._2)).filterNot(x=>x.startsWith("RT"))).length
}
val tweets_dia = values.map(tweet => (tweet.time.getDayOfMonth,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1, tweet_rtweet_dia(kv._2,true),tweet_rtweet_dia(kv._2, false)))






      //segunda consulta = horas activas de tweets y retweets

def tweet_rtweet_hora(ffData: List[Tuple3[Int,String,String]],isRTweet:Boolean):Int ={

  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isRTweet)
    (countersList.flatMap(t2 => List(t2._1)).filter(x=>x.startsWith("RT"))).length
  else
    (countersList.flatMap(t2 => List(t2._2)).filterNot(x=>x.startsWith("RT"))).length
}
val tweets_hora = values.map(tweet => (tweet.time.getHour,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1, tweet_rtweet_dia(kv._2,true),tweet_rtweet_dia(kv._2, false)))





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




    //septima consulta media
val media = values.map(tweet => Try(ujson.read(tweet.entitiesStr).obj("media")) match {case Success(s)=>"success"
case Failure(f)=>"failure"}).groupBy(identity).map({case(k,v) => (k , v.length)})
    
//val count = List(values.map(tweet => tweet.userFriendsCount match{case Left(s) => s})).flatten
//val count2 = List(values.map(tweet => tweet.userFollowersCount match{case Left(s) => s})).flatten
//count2.map(x=> x.toIntOption)


      //octava consulta coeficiente de pearson
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



    //novena cosulta


def processFFCounters2(ffData: List[Tuple3[String,String,String]],isRTweet:Boolean):Int ={
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isRTweet)
    (countersList.flatMap(t2 => List(t2._1)).filter(x=>x.startsWith("RT"))).length
  else
    (countersList.flatMap(t2 => List(t2._2)).filterNot(x=>x.startsWith("RT"))).length
}

def processFFCounters(ffData: List[Tuple3[String,Double,Double]],isFollowers:Boolean):Int ={
  val avg = (nums:List[Double])=> nums.sum / nums.length
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isFollowers)
    avg(countersList.flatMap(t2 => List(t2._1))).toInt
  else
    avg(countersList.flatMap(t2 => List(t2._2))).toInt
}
def completo(ffData: List[Tuple5[String, Double, Double, String, String]],isTrue:Boolean,tweetOrFriends:Boolean):Int ={
  val datos1 = ffData.map(x=>(x._1,x._2,x._3))
  val datos2 = ffData.map(x=>(x._1,x._4,x._5))
  if(tweetOrFriends:Boolean)
    processFFCounters(datos1,isTrue)
  else
    processFFCounters2(datos2,isTrue)
}
val consulta =  values.map(tweet => (tweet.fromUser,tweet.userFollowersCount, tweet.userFriendsCount,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1, completo(kv._2,true,true),completo(kv._2, false,true)
  ,completo(kv._2,true,false),completo(kv._2, false,false)))



    //decima consulta cuantas veces se ah mencionado un usuario

//val distribucion_mentions = values.flatMap(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr).map(
//  ht=> ht.obj("screen_name").str).groupBy(identity).map({case(k,v) => (k,v.length)})

def process(a:String, b:Int,lista:List[(String)]):Int ={
  if (lista contains(a))
    0  else
    b}

val aux = values.flatMap(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr).map(
  ht=> ht.obj("screen_name").str)
val aux2 = ((values.map(x => x.fromUser)).distinct) diff (aux.distinct)
var aux3 = aux2.union(aux).groupBy(identity).map({case(k,v) => (k,v.length)})
var distribucion_mentions = aux3.map(x=> (x._1,process(x._1,x._2,aux2)))


//escribir primera conslta
val out = java.io.File.createTempFile("activeDays.csv","csv")
val writer = out.asCsvWriter[(Int,Int,Int)](rfc.withHeader("day","tweets","retweets"))
tweets_dia.foreach(writer.write(_))
writer.close()


//segunda consulta
val out = java.io.File.createTempFile("activeHours.csv","csv")
val writer = out.asCsvWriter[(Int,Int,Int)](rfc.withHeader("hour","tweets","retweets"))
tweets_hora.foreach(writer.write(_))
writer.close()


//tercera consulta
val out = java.io.File.createTempFile("devices.csv","csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("device","count"))
devices2.foreach(writer.write(_))
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
val out = java.io.File.createTempFile("septima.csv","csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("opcion","count"))
media.foreach(writer.write(_))
writer.close()


//novena consulta
val out = java.io.File.createTempFile("novena.csv","csv")
val writer = out.asCsvWriter[(String,Int,Int,Int,Int)](rfc.withHeader("usuario","followers","friends","tweet","retweets"))
consulta.foreach(writer.write(_))
writer.close()

//decima consulta
val out = java.io.File.createTempFile("decima.csv","csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("usuario","mentions"))
distribucion_mentions.foreach(writer.write(_))
writer.close()








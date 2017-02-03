import java.time._

object HelperFunctions {
    def getDateTime(timestamp: String): LocalDateTime = {
    val year = timestamp.substring(0,4).toInt
    val month = timestamp.substring(4,6).toInt
    val dayOfMonth = timestamp.substring(6,8).toInt
    val hour = timestamp.substring(8,10).toInt
    val minute = timestamp.substring(10,12).toInt
    val second = timestamp.substring(12, 14).toInt
    val nanoOfSecond = (timestamp.substring(14)++"000000").toInt
    
    LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond)
  }
    
  def eventToString(e: CampaignBidEvent): String = {
    val b_id = e.result.b_id
    val timestamp = e.result.timestamp
    val privateValue = e.privateValuation
    val eCTR = e.eCTR
    val eWP = e.eWP
    val bid = e.result.bid
    val impression = e.result.is_imp
    val click = e.result.is_click
    val conversion = e.result.is_conversion
    val payingPrice = e.result.payingPrice
     
    s"$b_id,$timestamp,$privateValue,$eCTR,$eWP,$bid,$impression,$click,$conversion,$payingPrice\n"     
  }
   
  def nextHour(day: String, hour: Int): (String, Int) = (hour+1) match {
    case newHour if newHour < 24 => (day, newHour)
    case 24 => (nextDay(day), 0)
  }
  
  def prevHour(day:String, hour: Int): (String, Int) = hour match {
    case 0 => (prevDay(day), 23)
    case _ => (day, hour-1)
  }  
  
  def nextHour(tup: (String, Int)): (String, Int) = nextHour(tup._1, tup._2)
  def prevHour(tup: (String, Int)): (String, Int) = prevHour(tup._1, tup._2)
  def getDayHour(time: LocalDateTime): (String, Int) = (time.getDayOfWeek.toString, time.getHour)
  
  
  
  val nextDay = Map(
       "MONDAY" -> "TUESDAY",
       "TUESDAY" -> "WEDNESDAY",
       "WEDNESDAY" -> "THURSDAY",
       "THURSDAY" -> "FRIDAY",
       "FRIDAY" -> "SATURDAY",
       "SATURDAY" -> "SUNDAY",
       "SUNDAY" -> "MONDAY"
       )
    
  val prevDay = nextDay.map(_.swap)     
  
  def getHoursBetween(now: LocalDateTime, then: LocalDateTime) ={
    if (now isAfter then) Array[(String,Int)]()
    else {
      var time = getDayHour(now)
      var hours = Array(getDayHour(now))
      var numHours =Duration.between(now, then).toHours

      while (numHours>0){
        numHours -= 1
        time = nextHour(time)
        hours :+= time
      }
      hours
    }
  }
  
  def uuid = java.util.UUID.randomUUID.toString
       
}

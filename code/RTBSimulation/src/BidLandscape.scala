import breeze.stats.distributions._

/*
 * Class representing a bid landscape,
 * assuming a log-normal distribution of winning prices
 * 
 * @params
 * requestRate : incoming bid requests per hour
 * mu : population mean of winning price (identical to cpm)
 * sigma : population std-deviation of winning price
 * ctr : avg click through rate
 */

class BidLandscape(val requestRate: Double,  val mu: Double, val sigma: Double,  val ctr: Double){
 
  
  val location = math.log(mu / math.sqrt(1+ sigma*sigma/(mu*mu)))
  val scale = math.sqrt(math.log(1 + sigma*sigma/(mu*mu)))
  
  val distr = new LogNormal(location, scale)
  
  def getWinProbability(bid: Double) = distr.cdf(bid)
  
  def cpm = mu
  def cpc = mu/ctr
  
  /*
   * returns a new BidLandscape that is a mixture of this and that with weights (lambda, 1-lambda)
   */
  def mix(that: BidLandscape, lambda: Double =0.5): BidLandscape = {
    //define some vals for better readability
    val m1 = this.mu
    val m2 = that.mu
    
    val s1 = this.sigma
    val s2 = that.sigma   
    
    val n1 = lambda*this.requestRate
    val n2 = (1-lambda)*that.requestRate       
    
    val n = (n1 + n2)
    val m = (n1 * m1 + n2 * m2)/(n1 + n2)
    val s = math.sqrt( 
          ( n1*s1*s1 + n2*s2*s2 + n1*(m1-m)*(m1-m) + n2*(m2-m)*(m2-m) )/ 
      // --------------------------------------------------------------------- (Bruchstrich)
                                    (n1 + n2 -1 ) 
        )        
    val new_ctr = (n1*this.ctr + n2 * that.ctr)/n    
    
    new BidLandscape(n, m, s, new_ctr)
  }
  
  override def toString = f"BidLandscape($requestRate, $mu, $sigma%1.3f, $ctr%1.5f)"
}



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType};
/*
 * Singleton object retreiving hourly priors from spark data frame
 */
object BidLandscapeServer{
  private  val data_dir = "file:///c:/Users/Stefan/Desktop/Thesis/data/simulation_data/"
  private val filename = "20160919_training_metrics_hourly.csv"
  private val spark =  SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", data_dir)
    .appName("WP-Prediction")
    .master("local[1]")
    .getOrCreate()
    
  import spark.implicits._
  
  case class LandscapeRow(Advertiser: String, Weekday: String, Hour: Int, Impressions: Double, muPrice: Double, sigmaPrice: Double, CTR: Double)
  
  /*
   * A map of the landscape *badumm-tsss*
   * contains every landscape that has already been extracted from spark
   * (no line will be read from spark twice)
   */  
  private val theMap = scala.collection.mutable.Map[(String, String, Int), BidLandscape]()
  
  def getBidLandscape(advertiser:String, weekday:String, hour:Int) = 
      theMap.getOrElseUpdate(
          (advertiser, weekday, hour),
          getBidLandscapeFromSpark(advertiser,weekday,hour))
          
  def getBidLandscape(advertiser: String, timeTuple: (String,Int)): BidLandscape =
    getBidLandscape(advertiser, timeTuple._1, timeTuple._2)
  
  
  private val df_bl = spark
    .read
    .format("csv")
    .option("header", true)
    .option("nullValue", "NA")
    .option("inferSchema", "true")
    .csv( data_dir ++ filename)
    .select("Advertiser", "Weekday", "Hour", "Impressions", "muPrice", "sigmaPrice", "CTR")
    .map(r => LandscapeRow(r(0).toString,
        r(1).toString.toUpperCase,
        r(2).toString.toInt,
        r(3).toString.toDouble,
        r(4).toString.toDouble,        
        r(5).toString.toDouble,
        r(6).toString.toDouble))
    .persist()    
    
  private def getBidLandscapeFromSpark(advertiser: String, weekday: String, hour: Int): BidLandscape = {
    try{
      val row = df_bl.filter($"Advertiser" === advertiser && $"Weekday" === weekday && $"Hour" === hour)
                .collect()(0)
                
      new BidLandscape(row.Impressions, row.muPrice, row.sigmaPrice, row.CTR)
    }
    catch{
      case e: Exception => {
        new BidLandscape(0, 75, 50, 8e-4) 
      }
    }
  } 
  
  def init = println("Started BidLandscapeServer") //will load the data frame and keep it in memory
}
                
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._

object TrainingMetrics extends App {
  
  val debug = true

  val home_dir = "file:///c:/Users/Stefan/Desktop/Thesis/data/"
  

  if (debug) println("Creating Spark Session...")
  
  
  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", home_dir)
    .appName("TrainingMetrics")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  
  if (debug) println("Loading Data...")
    
  val df_raw = spark
                .read
                .format("csv")
                .option("header", true)
                .option("nullValue", "NA")
                //.option("sep", "\t")
                //.csv( home_dir ++ "/data_processed_0908_with_CTR/Samples/sample_500k.csv")
                .csv(home_dir ++ "data_processed_0908_with_CTR/Full/*.csv")
  
  if (debug) println("Making plan...")
                
  val timeOfDay = udf{hour: Int =>
    hour match{
      case night if 1<= hour && hour <= 6 => "night"
      case morning if 6<hour && hour <= 12 => "morning"
      case afternoon if 12<hour && hour <= 18 => "afternoon"
      case evening if hour==0 || 18 < hour => "evening"
    }
  }
     
/*  import breeze.stats.distributions._

  val mu = 69.03564
  val sigma = 53.72485
  val sigmaSquared = sigma*sigma
  //val location = exp(mu)
  
  // parameters for the logNormal distr.
  val location = math.log(mu / math.sqrt(1+ sigmaSquared/(mu*mu)))
  val shape = math.sqrt(math.log(1 + sigmaSquared/(mu*mu)))
 
  
  
  val lnd = new LogNormal(location, shape)
  val norm = new Gaussian(0, 1)
  lnd.cdf(20)
  norm.pdf((math.log(20)-mu)/sigma)
  lnd.mean
  lnd.variance
  */
  def lessThan(x: Double) = udf{(v: Int) => if (v<=x) 1.0 else 0.0 }
  
  val df_pre = Preprocessing.mapIntoSchema(df_raw)              
  val df_grouped = df_pre
    .filter("Impression == 1.0")
    .withColumn("timeOfDay", timeOfDay(col("Hour")))
    .withColumn("less250", lessThan(250)($"PayingPrice"))
    .withColumn("less200", lessThan(200)($"PayingPrice"))
    .withColumn("less150", lessThan(150)($"PayingPrice"))
    .withColumn("less125", lessThan(125)($"PayingPrice"))
    .withColumn("less100", lessThan(100)($"PayingPrice"))
    .withColumn("less75", lessThan(75)($"PayingPrice"))
    .withColumn("less50", lessThan(50)($"PayingPrice"))
    .withColumn("less25", lessThan(25)($"PayingPrice"))
    .groupBy("Advertiser", "Weekday","Hour")
    .agg(count("Impression").as("Impressions"),
        sum("Click").as("Clicks"),
        sum("PayingPrice").as("Spending"),
        avg("PayingPrice").as("muPrice"),
        stddev("PayingPrice").as("sigmaPrice"),
        variance("PayingPrice").as("varPrice"),
        max("PayingPrice").as("maxPrice"),
        min("PayingPrice").as("minPrice"),
        avg("Less250").as("shareLess250"),
        avg("Less200").as("shareLess200"),
        avg("Less150").as("shareLess150"),
        avg("Less125").as("shareLess125"),
        avg("Less100").as("shareLess100"),
        avg("Less75").as("shareLess75"),
        avg("Less50").as("shareLess50"),
        avg("Less25").as("shareLess25"))
    .sort("Advertiser", "Weekday", "Hour")
    .withColumn("ImpsPerHour", $"Impressions"/6)
    .withColumn("CTR", $"Clicks"/$"Impressions")
    .withColumn("CPC", $"Spending"/$"Clicks")
    .withColumn("CPM", $"Spending"/$"Impressions")
    
  val output_path = home_dir ++ "simulation_data/20160919_training_metrics_hourly.csv"
  
  if (debug) println("calculating and writing...")
  
  df_grouped
      .coalesce(1)
      .write
      .format("csv")
      .option("header", true)
      .option("nullValue","NA")
      .mode("overwrite")
      .save(output_path)
}
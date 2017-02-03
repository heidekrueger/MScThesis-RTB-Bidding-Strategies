import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import scala.collection.mutable.Map



object Preprocessing {
  /**
   * Singleton Object that provides methods for preprocessing the raw data.
   * (Different subset of these will be used for WP-Prediction and Bidder Simulation
   */
  
  val debug: Boolean = false
  

  val region_map_txt = "C:/Users/Stefan/Desktop/Thesis/iPinYou/ipinyou.contest.dataset/region.en.txt"
  val city_map_txt = "C:/Users/Stefan/Desktop/Thesis/iPinYou/ipinyou.contest.dataset/city.en.txt"
  val user_tags_map_txt = "C:/Users/Stefan/Desktop/Thesis/iPinYou/ipinyou.contest.dataset/user.profile.tags.en.txt"
  val city_pop_txt = "C:/Users/Stefan/Desktop/Thesis/code/WP-Prediction/CitiesPopulation.txt"
  
  def dropColumns(
        input_df: DataFrame, unnecessaryCols: Seq[String]): DataFrame ={
    var df = input_df
    for (col <- unnecessaryCols){
      df = df.drop(col)
    }
    df
  }
  
  def renameUTColumns(input_df: DataFrame): DataFrame = {
    /** Renames the UT-Columns in the raw data frame by preceding a "UT"
     *  
     *  This is necessary as the original names start with numbers and this is
     *    illegal for class field names (which we will use in Bidding)
     *  
     *  @param input_df
     */
    val UTcols = input_df.columns.filter(_.startsWith("1"))    
    var df = input_df
    
    for (col <- UTcols)
      df = df.withColumnRenamed(col, "UT"++col)

    df
  }
  
  def mapIntoSchema(input_df: DataFrame): DataFrame = {
    /**
     * Casts columns in raw data to their respective types.
     */
    
    /** old classes (old dataset)
    val intCols = Array("_c0","index","hour","AdSlotW", "AdSlotH", "Region", "City", "AdEx", "AdSlotFormat")
    val floatCols = Array("AdSotFloorPrice", "BidPrice", "PayingPrice")
    val booleanCols = Array("is_imp", "is_cl", "is_cv") ++ input_df.columns.filter(_.startsWith("UT"))
    */
    
    val intCols = Array(
        "Hour",
        "Month",
        "Year",
        "City_Pop",
        "AdSlotW",
        "AdSlotH",
        "AdSlotSize"
        )
    val floatCols = Array(
        "Impression",
        "Click",
        "Conversion",
        "City_Lat",
        "City_Long",
        "AdSlotFloorPrice",
        "UT_10006",
        "UT_10024",
        "UT_10031",
        "UT_10048",
        "UT_10052",
        "UT_10057",
        "UT_10059",
        "UT_10063",
        "UT_10067",
        "UT_10074",
        "UT_10075",
        "UT_10076",
        "UT_10077",
        "UT_10079",
        "UT_10083",
        "UT_10093",
        "UT_10102",
        "UT_10110",
        "UT_10111",
        "UT_10684",
        "UT_11092",
        "UT_11278",
        "UT_11379",
        "UT_11423",
        "UT_11512",
        "UT_11576",
        "UT_11632",
        "UT_11680",
        "UT_11724",
        "UT_11944",
        "UT_13042",
        "UT_13403",
        "UT_13496",
        "UT_13678",
        "UT_13776",
        "UT_13800",
        "UT_13866",
        "UT_13874",
        "UT_14273",
        "UT_16593",
        "UT_16617",
        "UT_16661",
        "UT_16706",
        "prediction",
        "GBRT_prediction_International e-commerce",
        "GBRT_prediction_Oil",
        "GBRT_prediction_Software",
        "GBRT_prediction_Tire",
        "GBRT_prediction_Chinese vertical e-commerce",
        "BidPrice",
        "PayingPrice"      
    )
    
    // no more boolean columns in current version of the dataset
    val booleanCols = Array[String]()
    
    var df = input_df.select(
      input_df.columns.map {
        case intCol if intCols.contains(intCol) => input_df(intCol).cast(IntegerType).as(intCol)
        case floatCol if floatCols.contains(floatCol) => input_df(floatCol).cast(DoubleType).as(floatCol)
        case booleanCol if booleanCols.contains(booleanCol) => (input_df(booleanCol).cast(BooleanType)).as(booleanCol)
        case anyStringCol              => input_df(anyStringCol)
      }: _*
    )
    
    implicit def bool2double(b: Boolean) = if (b) 1.0 else 0.0
    val bool2doubleUDF = udf((b: Boolean) => b:Double)
    
    for (col <- booleanCols)
      df = df.withColumn(col, bool2doubleUDF(df(col)))
    
    
    df
  }
  
  
  def mapCities(input_df: DataFrame, cityCol: String): DataFrame = {
    /**
     * Maps cities in city_colfrom indices to city names  
     */
    var citiesMap: Map[Int,String] = scala.collection.mutable.Map()
    Source.fromFile(city_map_txt).getLines.map(_.split("\t")).foreach(r => citiesMap += (r(0).toInt -> r(1)))
    def getCities = udf(
      (key: Int) =>  {
        if ( (!citiesMap.contains(key)) || (citiesMap(key) == "unknown") )
          "NA"
        else
          citiesMap(key)
      }
    )
    input_df.withColumn(cityCol, getCities(input_df(cityCol)))   
  }
  
  def mapRegions(input_df: DataFrame, regionCol: String): DataFrame = {
    /**
     * Maps regions in regionCol from indices to regions names 
     */
    var regionsMap: Map[Int,String] = scala.collection.mutable.Map()
    Source.fromFile(region_map_txt).getLines.map(_.split("\t")).foreach(r => regionsMap += (r(0).toInt -> r(1)))
    def getRegions = udf(
      (key: Int) =>  {
        if ( (!regionsMap.contains(key)) || (regionsMap(key) == "unknown") )
          "NA"
        else
          regionsMap(key)
      }
    )
    input_df.withColumn(regionCol, getRegions(input_df(regionCol)))   
  }
  
  def mapAdExes(input_df: DataFrame, adExCol: String): DataFrame = {
    val adExMap = Map(
      1 -> "DoubleClick",
      2 -> "TANX",
      3 -> "Baidu",
      4 -> "Youku",
      5 -> "Tencent"
    )
    val adExUDF = udf((index: Int) => adExMap(index))
    
    input_df.withColumn(adExCol, adExUDF(input_df(adExCol)))
  }

  def mapSlotFormat(input_df: DataFrame, formatCol: String): DataFrame = {  
    val slotFormatMap = Map(0 -> "Fixed", 1 -> "Pop", 2 -> "Background", 5 -> "Float")
    val slotFormatUDF = udf((index: Int) => slotFormatMap(index))    
    input_df.withColumn(formatCol, slotFormatUDF(input_df(formatCol)))
  }
  
  def mapSlotVisibility(input_df: DataFrame, visibilityCol: String): DataFrame = {  
    val slotVisibilityMap = Map(
      0 -> "NA",
      1 -> "First View",
      2 -> "Second View",
      3 -> "Third View",
      4 -> "Fourth View",
      5 -> "Fifth View",
      6 -> "Sixth View",
      255 -> "Other View"
    )
    val slotVisibilityUDF = udf((index: Int) => slotVisibilityMap(index))    
    input_df.withColumn(visibilityCol, slotVisibilityUDF(input_df(visibilityCol)))
  }
  
  def mapIndexCols(input_df: DataFrame,
                   cityCol: String,
                   regionCol: String,
                   adExCol: String,
                   formatCol: String,
                   visibilityCol: String
                   ): DataFrame = {     
    var df = mapCities(input_df, cityCol)
    df = mapRegions(df, regionCol)
    df = mapAdExes(df, adExCol)
    df = mapSlotFormat(df, formatCol)
    df = mapSlotVisibility(df, visibilityCol)
    df
  } 
  
 
  def preprocessAll(input_df: DataFrame): DataFrame ={
    var df = renameUTColumns(input_df)
    df = mapIntoSchema(df)
    df = mapIndexCols(
      df,
      "City",
      "Region",
      "AdEx",
      "AdSlotFormat",
      "AdSlotVis"
    )
    
    val unnecessaryColumns = Seq(
      "_c0",
      "index",
      //"b_id",
      //"timestamp",
      "u_id",
      "UA",
      "AdS",
      "CreativeID",
      "f_timestamp",
      "UserTags",
      "is_cl",
      "is_cv",
      "f_time_diff",
      "AdSlotID",
      "AdvertiserID" // Assume WP is independent of who is bidding. TODO
      // TODO: Actually, train these independently as advertisers only see different impressions.
    )
    df = dropColumns(df, unnecessaryColumns)  
     
    df
  }
  
}
import org.apache.spark.ml.feature._
import org.apache.spark.ml._//Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.param._



object FeaturePreparation {
  
  val numHashed = 1000
  
  def getLog =
    "-------------------------\n"++
    "-- Feature Preparation --\n"++
    "-------------------------\n\n"++
    "1a. Categorical Features (Safe): \n" ++
    categoricalColumnsSafe.reduce(_++"\t"++_) ++
    "1b. Categorical Features (Unsafe): \n" ++
    categoricalColumnsUnsafe.reduce(_++"\t"++_) ++
    "\n\n" ++
    "2. Hashing Features: \n"++
    hashingColumns.reduce(_++"\t"++_) ++
     "\nnHashedFeatures: " ++ numHashed.toString ++ "\n\n" ++
    "3. Numerical Features: \n"++
    numericalColumns.reduce(_++"\t"++_) ++ "\n\n\n"
  
  
  
  
  /* Sets the categorical columns that are
   * 	1. _NOT_ going to be hashed and 
   *  2. where no new values may appear
   */
  val categoricalColumnsSafe = Array(
     "Weekday",
     "Hour",
     //"Month",
     //"Year",
     "AdEx",
     "AdSlotVis",
     "AdSlotFormat",
     "Advertiser",
    //"UA_os_fam", <-- even this is unsafe aaaarrrghhh
    //"UA_os_ver", <-- unsafe
    //"UA_br_fam", <-- also unsafe :(
    //"UA_br_ver",
    //"UA_dev_fam", <-- unsafe
    //"UA_dev_brand",
    //"UA_dev_mod", <-- unsafe
    //"Region",
    //"City",
    "UA_os_type"

    //"Site_URL"
)

  /* Sets the categorical columns that are
   * 	1. _NOT_ going to be hashed and 
   *  2. where unseen values _may_ appear
   */
  val categoricalColumnsUnsafe = Array(
    "UA_os_fam",
    "UA_os_ver",
    "UA_br_fam",
    "UA_br_ver",
    "UA_dev_fam",
    "UA_dev_brand",
    "UA_dev_mod",
    "Region",
    "City"
    //"Site_URL"
)
      
  // Sets the categorical columns that will be hashed using the feature hasher
  val hashingColumns = Array[String](
/*    "User_IP",
    "Region",
    "City",
    "Site_Dom",
    "Site_URL",
    "Site_AnonURL",
    "UA_os_fam",
    "UA_os_ver",
    "UA_br_fam",
    "UA_br_ver",
    "UA_dev_fam",
    "UA_dev_brand",
    "UA_dev_mod", */
    //"UA_os_type"
    "Site_URL",
    "Site_Dom"
)    
      
  val numericalColumns = Array(
     "City_Pop",
     "City_Lat",
     "City_Long",
     "AdSlotW",
     "AdSlotH",
     "AdSlotSize",
     "AdSlotFloorPrice"
      )
      
  val utCols  =  Array(
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
     "UT_16706")
  
     
  /*
   * Define Transformers that will be applied   
   */
  
  val numVecAssembler = new VectorAssembler()
    .setInputCols(numericalColumns)
    .setOutputCol("FT_num")
  
  val utVecAssembler = new VectorAssembler()
    .setInputCols(utCols)
    .setOutputCol("FT_ut")
 
  
  val stringIndexersSafe = categoricalColumnsSafe.map {
    col => new MyStringIndexer()
    .setInputCol(col)
    .setOutputCol("ID_"++col)
    .setHandleInvalid("error")
  }
  
  val stringIndexersUnsafe = categoricalColumnsUnsafe.map {
    col => new MyStringIndexer()
    .setInputCol(col)
    .setOutputCol("ID_"++col)
    .setHandleInvalid("setNA")
  }
  
  val oheEncoders = (categoricalColumnsSafe ++ categoricalColumnsUnsafe).map { 
    col => new OneHotEncoder()
      .setInputCol("ID_"++col)
      .setOutputCol("FT_"++col)
  }
  
  val ftHasher = new FeatureHasher()
    .setNumFeatures(numHashed)
    .setInputCols(hashingColumns)
    .setOutputCol("FT_hashed")
  
  val ftAssembler = new FtColumnAssembler()
    .setOutputCol("features")
  
  /*
   * Define Pipeline. This pipeline will be called from the main script.
   */
    
  val pipe = new Pipeline()
    .setStages(
      Array(numVecAssembler, ftHasher) ++
      stringIndexersSafe ++ stringIndexersUnsafe ++ 
      oheEncoders ++ 
      Array(utVecAssembler, ftAssembler)
    )


      
}
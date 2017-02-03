  import org.apache.spark.ml.regression._  
  import org.apache.spark.ml._
  import org.apache.spark.sql.DataFrame
  import scala.Exception
  

object Training {
  
  val lrMaxIter = 100
  val lrRegParam = 1
  val lrElasticNetParam = 0.1
  
  
  
  def getLog: String =
    "-------------------------\n" ++
    "--        Training     --\n" ++
    "-------------------------\n\n" ++
    "1. LR Parameters:\n" ++
    "MaxIter\t" ++ lrMaxIter.toString ++ "\n" ++
    "RegParam\t" ++ lrRegParam.toString ++ "\n" ++
    "lrElasticNetParam\t" ++ lrElasticNetParam.toString ++ "\n\n"

  
    
  var active_models = Array(true, true ,true)
    
  var models = Array()
  
  // Train a RandomForest model.
  val  rf_spec = new RandomForestRegressor()
    .setLabelCol("PayingPrice")
    .setFeaturesCol("features")
  
  
    // and a GBT model
  val  gbt_spec = new GBTRegressor()
  .setLabelCol("PayingPrice")
  .setFeaturesCol("features")
  .setMaxIter(10)
  
  
  // and linear regression
  val lr_spec = new LinearRegression()
    .setFeaturesCol("features")
    .setLabelCol("PayingPrice")
    .setMaxIter(lrMaxIter)
    .setRegParam(lrRegParam)
    .setElasticNetParam(lrElasticNetParam)
    
    
  val model_Specs = Array(lr_spec, rf_spec, gbt_spec)
  
  
  
  def trainLRModel(df: DataFrame) = {  
     lr_spec.setLabelCol("PayingPrice").fit(df)
  }
  
  def trainGBTmodel(df: DataFrame) = {
    gbt_spec.setLabelCol("PayingPrice").fit(df)
  }
    
}
  
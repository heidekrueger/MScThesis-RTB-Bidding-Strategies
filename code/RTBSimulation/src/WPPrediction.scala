import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation._

object WPPrediction extends App {
  
  val run_id = "LR L2_1 Full Model with Hashed URL NumHash5000"
  val run_time =   java.time.LocalDateTime.now
  
  var log: String = run_id ++ "\n" ++
                    run_time.toString ++ "\n\n"
  
  val total_tick = System.currentTimeMillis  
  
  val debug = true
  val home_dir = "file:///c:/Users/Stefan/Desktop/Thesis/data/"
  val log_dir = "C:/Users/Stefan/Desktop/Thesis/data/logs/WP-Prediction/"
  
  val test_set_path = home_dir ++ "TestDataPredictions/*.csv" // This refers to the iPinYou test set, not the test set in this machine learning task!
  
  

  if (debug) println("Creating Spark Session...")
  
  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", home_dir)
    .appName("WP-Prediction")
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
                .csv( home_dir ++ "/data_processed_0908_with_CTR/Samples/sample_500k.csv")
  
  
  val preprocessing_tick = System.currentTimeMillis                
  if (debug) println("Preprocessing...")                  
  
  // Preprocessing step no longer needed when using Yannis's prepipelined data with CTR Attached
  //val df_preprocessed = Preprocessing.preprocessAll(df_raw)
  val df_preprocessed = Preprocessing.mapIntoSchema(df_raw)
  
  val preprocessing_tock = System.currentTimeMillis  
  
  if (debug) println("Filtering Impressions...")
  
  // No longer necessary --> working only on impressions to begin with
  val df_filtered = df_preprocessed //.filter("Impression == 1.0").drop("Impression")
    // only do this for now, since unique in this file
    //.drop("Weekday")
 
  
  
  val features_tick = System.currentTimeMillis
  if (debug) println("Preparing Features...")
  
  var pipeModel = FeaturePreparation.pipe.fit(df_filtered)

  if (debug) println("\tPipeline has been fit...")
  
  val df_featured= pipeModel.transform(df_filtered)//.persist()
  
  val features_tock = System.currentTimeMillis
  
  log ++= FeaturePreparation.getLog
  
  //df_featured.show()
  
  if (debug) println("Train test split")
    
  
  // TODO: get rid of the throwaway (used for faster training during prototyping)
  val Array(df_train, df_val, df_test, df_throwaway) = df_featured
        .select("timestamp", "Bid_id","PayingPrice","features")
        .randomSplit(weights=Array(0.7, 0.0, 0.3, 0.0), seed=42 )
        
        
  

  //df_val.persist()
  //df_test.persist()
  //df_train.cache()
  
  //df_featured.unpersist()
  
  val training_tick = System.currentTimeMillis
  
  if (debug) println("Training")
  
  /* Train a model */
  
  val model = Training.trainLRModel(df_train)
              .setPredictionCol("WP-Prediction")
              
  val model2 = Training.trainGBTmodel(df_train)
              .setPredictionCol("WP-Prediction2")
  
  val training_tock = System.currentTimeMillis
  val total_tock = training_tock
  println("done")
  
  /* Alternatively, load an existing model: */
  
  
  log++= Training.getLog
  
  println(model.summary.rootMeanSquaredError)
    
  val prediction_tick = System.currentTimeMillis
  val df_test_predicted = model.transform(df_test)
  val prediction_tock = System.currentTimeMillis
  

  val evaluator = new RegressionEvaluator()
    .setLabelCol("PayingPrice")
    .setPredictionCol("WP-Prediction")
    .setMetricName("rmse")
    
  val test_RMSE = evaluator.evaluate(df_test_predicted)
  
  println(test_RMSE)
  
  val preprocessing_time = preprocessing_tock - preprocessing_tick
  val features_time = features_tock - features_tick
  val training_time = training_tock - training_tick
  val total_time = total_tock - total_tick
  val prediction_time = prediction_tock - prediction_tick
  
  log ++= "-----------------\n\n" ++
    "Times:\n" ++
    "Preprocessing, FeaturePrep, Training, PredictingTest, Total\n" ++
    List(preprocessing_time, features_time, training_time, prediction_time, total_time).map(_.toString++"\t").reduce(_++_) ++
    "\n\n"  
 
  
  
    
    
  log ++= "nFeatures: "++ model.numFeatures.toString ++ "\n\n" ++
          "---------------------\n" ++
          "--   Results       --\n"++
          "---------------------\n\n"++
          "Objective History:\t" ++
           model.summary.objectiveHistory.map(_.toString).reduce(_++"\t"++_)  ++"\n" ++
          "Train RMSE: \t" ++ model.summary.rootMeanSquaredError.toString ++"\n"++
          "Test RMSE: \t" ++ test_RMSE.toString ++ "\n\n"++
          "Nonzero Coefficients:\t"++ model.coefficients.numNonzeros.toString
  print(log)
  
  
  //model.params.map(p => p.name.toString() ++ "\t" ++ p.))
  
  scala.tools.nsc.io.File(log_dir ++ run_time.toString.replaceAll(":", "") ++ " "++ run_id++".txt").writeAll(log) 
  
  
  
  println("Now working on Simulation set...")
  
  /* Add the predictions to the full iPinYouSeason2 Test Set */
  /*
  val df_sim_raw = spark
                .read
                .format("csv")
                .option("header", true)
                .option("nullValue", "NA")
                .option("sep", "\t")
                .csv( test_set_path)
  val df_sim_preprocessed = Preprocessing.mapIntoSchema(df_sim_raw)
  val sim_feature_tick = System.currentTimeMillis()
  val df_sim_featured= pipeModel.transform(df_sim_preprocessed)
  val sim_feature_tock = System.currentTimeMillis()
  
  println(f"Featuring Sim Data time (s): ${(sim_feature_tock-sim_feature_tick)/1000}")
  
  
  val df_sim_full_pred = model.transform(df_sim_featured)
    .select("Bid_id",
        "Timestamp",
          "Impression", 
          "Click", 
          "Conversion",
          "Advertiser",
          "AdSlotFloorPrice",
          "prediction",
          "GBRT_prediction_International e-commerce",
          "GBRT_prediction_Oil",
          "GBRT_prediction_Software",
          "GBRT_prediction_Tire",
          "GBRT_prediction_Chinese vertical e-commerce",
          "BidPrice",
          "PayingPrice",
          "WP-Prediction"
          )
  val output_dir = home_dir ++ "simulation_data/"
  
  
  println("writing...")
  
  df_sim_full_pred
      .orderBy("Timestamp")
      //.coalesce(1)
      .write
      .format("csv")
      .option("header", true)
      .option("nullValue","NA")
      .mode("overwrite")
      .save(output_dir ++ "20160916_sim_data_fullTestSet.csv")
  
      
  println("done.")
  model.save(log_dir ++  run_time.toString.replaceAll(":", "") ++ " "++ run_id++".LRmodel")*/
}
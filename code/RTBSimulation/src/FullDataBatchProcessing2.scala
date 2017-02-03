import scala.io.Source
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql._

/*
 * This script was used as a template for various ETL steps such as preparing earlier versions of the data
 * or creating subsamples for training purposes.
 * In it's current form, it was used to filter only impressions in the data created by "CreateJoinedDataset.scala"
 * and write them to disk as these were used for WP-Prediction training.
 * 
 * This script is obsolete, as the interface between Stefan and Yannis was later refined and thus this script
 * is not needed to test the software.
 */


object FullDataBatchProcessing2 extends App {
  
  val data_dir = "file:///c:/Users/Stefan/Desktop/Thesis/data/data_processed_0908_with_CTR/"
  val data_dir2 = "C:/Users/Stefan/Desktop/Thesis/data/data_processed_0908_with_CTR/"

  val output_dir = data_dir ++ "Samples/"
  val logFileName = "log.txt"
  
  var log = "file\tnrow\n"

  println("Creating Spark Session...")
  
  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", data_dir)
    .appName("Create Sample Sets")
    .master("local[*]")
    .getOrCreate()
  

  val filenames = new File(data_dir2++"Full/")
    .listFiles
    //.filter(_.isFile())
    .map(_.getName)
    .filter(_.endsWith(".csv")) 
  
  val numFiles = filenames.length
  
  var current = 0
    
  //for (name <- filenames){
    
  //  current += 1
    
  //  println(s"processing file $current of $numFiles ($name) ...")
    
    val df_in = spark
      .read
      .format("csv")
      .option("header", true)
      .option("nullValue", "NA")
      //.option("sep", "\t")
      .csv(data_dir ++ "Full/data.201306*")
      
      
      
    val df_out1 = df_in.filter("Impression==1.0").sample(false,  .008)
    val df_out2 = df_in.filter("Impression==1.0").sample(false,  .004)
    
  //  log++= name ++ "\t" ++ df_out.count.toString ++ "\n"
    
    df_out1
      .coalesce(1)
      .write
      .format("csv")
      .option("header", true)
      .option("nullValue","NA")
      .save(data_dir ++ "Samples/" ++ "sample_100k.csv")
      
    df_out2
      .coalesce(1)
      .write
      .format("csv")
      .option("header", true)
      .option("nullValue","NA")
      .save(data_dir ++ "Samples/" ++ "sample_050k.csv")
      
   // df_out.unpersist()
  //}

//  scala.tools.nsc.io.File(data_dir2 ++ "Samples/" ++ logFileName).writeAll(log)  
  
  println("done")
  

}
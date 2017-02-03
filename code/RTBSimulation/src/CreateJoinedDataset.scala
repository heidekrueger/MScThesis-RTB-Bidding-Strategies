import scala.io.Source
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql._

/*
 * This script was used for merging CTR predictions provided by Yannis with additional data from the
 * iPinYou dataset that is required for simulation, but was not contained in Yannis' data (paying price etc)
 * 
 * This was only required at an earlier stage, as Yannis' and I refined the interface later on.
 * Thus this script is now obsolete and no longer needed for Testing the software.
 */

object CreateJoinedDataset extends App {
  
  val home_dir = "file:///c:/Users/Stefan/Desktop/Thesis/data/"
  val main_data_dir = home_dir++"data_preprocessed_0906_with_CTR/Impressions/"
  val main_data_dir2 = "C:/Users/Stefan/Desktop/Thesis/data/data_preprocessed_0906_with_CTR/Impressions/"
  val out_dir = main_data_dir++"joined/"
  val extra_data_dir =   home_dir ++ "data_processed/Impressions/"
  
  
  val filenames = new File(main_data_dir2)
    .listFiles
    //.filter(_.isFile())
    .map(_.getName)
    .filter(_.endsWith(".csv")) 
  
  
  
    val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", home_dir)
    .appName("Create Sample Sets")
    .master("local[*]")
    .getOrCreate()
    
  val noOfFiles = filenames.length
  var current = 0
  
  for (name <- filenames){
    current +=1
    println(s"processing file $current of $noOfFiles ($name) ...")
    
    val df_main = spark
      .read
      .format("csv")
      .option("header", true)
      .option("nullValue", "NA")
      //.option("sep", "\t")
      .csv(main_data_dir ++ name)
    
    val df_extra = spark
      .read
      .format("csv")
      .option("header", true)
      .option("nullValue", "NA")
      //.option("sep", "\t")
      .csv(extra_data_dir ++ name)
      .withColumnRenamed("b_id", "Bid_id")
      .select("Bid_id", "BidPrice", "PayingPrice")
   
    val df_join = df_main.join(df_extra, "Bid_id")
    
    df_join
      .coalesce(1)
      .write
      .format("csv")
      .option("header", true)
      .option("nullValue","NA")
      .save(out_dir ++ name)
  }
  println("done")
}

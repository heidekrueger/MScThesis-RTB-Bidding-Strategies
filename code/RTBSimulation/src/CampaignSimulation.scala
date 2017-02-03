import scala.io.Source
import java.time._
import com.github.marklister.collections.io.CsvParser
import HelperFunctions._
import java.io._

object CampaignSimulation extends App {
  
  val tick = System.currentTimeMillis()
  println("Starting up...")
  
  /* All .csv files in the input directory will be read and used for the simulation.*/
  val input_path = 
    "C:/Users/Stefan/Desktop/Thesis/data/simulation_data/20160916_sim_data_fullTestSet/"  
  val loggingPath = "C:/Users/Stefan/Desktop/Thesis/data/logs/Simulations/"
  val sysdate = LocalDate.now().toString
  
  val dataset = "FullTestSet"
  val budgetFactors = Array(1, 0.5, 0.33, 0.25, 0.125, 0.1, 0.05, 0.01)
  //val budgetFactors = Array(0.1)
  val biddertype = "ShadedValueBidder"
  val comment = ""
  
  val enableEventLogging = true //Use with caution! needs 400+MB of RAM per campaign
  
  val output_dir  = loggingPath ++ 
      Array(sysdate, dataset, biddertype).reduce(_ ++ " " ++ _) ++ "/"
                     
  
   
  println("Setting up BidLandscapeServer")
  BidLandscapeServer.init  
  
      
      
  // Metrics in Original Data
  val fullBudget_oil =        46356518.0
  val fullBudget_software =   34159766.0      
  val fullBudget_tire =       43627585.0   
  val fullBudget_eCommChina = 45216454.0      
  val fullBudget_eCommInt =   45715525.0   
  
  // Click Values per Campaign. This is arbitrary. Chosen such that original campaign has 0% ROI.
  val clickValue_oil = 126657.0 //CPC: 126.657
  val clickValue_software = 131384.0 //CPC: 131.384
  val clickValue_tire = 152012.0 //CPC: 152.012
  val clickValue_eCommChina = 87799.0 // 87.799
  val clickValue_eCommInt = 102732.0 //  102.732
  
  // True CTRs from training set (used for calc. avg value)
  val ctr_oil = 6.8E-4
  val ctr_software = 8.6E-4
  val ctr_tire = 5.5E-4
  val ctr_eCommChina = 8.4E-4
  val ctr_eCommInt = 8.2E-4
  
  // eCTR - Scaling factors <-> needed to recalibrate CTR estimation to actual levels
  // this factor describes, by how much the overall CTR is overestimated with the CTR estimator
  // Note: This is necessary due to Yannis' data output format.
  val eCTR_factor_oil = 91.778    
  val eCTR_factor_software = 85.841    
  val eCTR_factor_tire = 86.855     
  val eCTR_factor_eCommChina = 60.409   
  val eCTR_factor_eCommInt = 111.742
  
  val startTime = getDateTime("20130613000000001")
  val endTime = getDateTime(  "20130615235959999")
  val endTime_software = getDateTime("20130615135959999")

  
  
  println("Setting up Campaigns...")
  
  /*
   * Returns an array of bidders objects (one for each advertiser)
   */
  def getBidders: Array[BiddingStrategy] = biddertype match {
    case "OriginalBidding" => {
      val bidder = new ConstantBidder(1000)
      Array(bidder, bidder, bidder, bidder, bidder) // no state => can use same object
    }
    case "ConstantAvgValue" => {
      val bidder_oil = new ConstantBidder(clickValue_oil*ctr_oil)
      val bidder_software = new ConstantBidder(clickValue_software*ctr_software)
      val bidder_tire = new ConstantBidder(clickValue_tire*ctr_tire)
      val bidder_eCommChina = new ConstantBidder(clickValue_eCommChina*ctr_eCommChina)
      val bidder_eCommInt = new ConstantBidder(clickValue_eCommInt*ctr_eCommInt)
      Array(bidder_oil, bidder_software, bidder_tire, bidder_eCommChina, bidder_eCommInt)
    }
    case "TrueValue" =>
      Array(new ValueBidder, new ValueBidder, new ValueBidder, new ValueBidder, new ValueBidder)
    case "RandomUpToTrueValue" =>{
      val bidder_oil = new DynamicRandomBidder((br,cp)=> 0, (br,cp)=> cp.getPrivateValue(br))
      val bidder_software = new DynamicRandomBidder((br,cp)=> 0, (br,cp)=> cp.getPrivateValue(br))
      val bidder_tire = new DynamicRandomBidder((br,cp)=> 0, (br,cp)=> cp.getPrivateValue(br))
      val bidder_eCommChina = new DynamicRandomBidder((br,cp)=> 0, (br,cp)=> cp.getPrivateValue(br))
      val bidder_eCommInt =new DynamicRandomBidder((br,cp)=> 0, (br,cp)=> cp.getPrivateValue(br))
      Array(bidder_oil, bidder_software, bidder_tire, bidder_eCommChina, bidder_eCommInt)
    }
    case "ORTB" =>
      Array(new ORTBidder(comment),new ORTBidder(comment),new ORTBidder(comment),new ORTBidder(comment),new ORTBidder(comment))
    case "LinearBidder" => {
      val bidder_oil = new LinearBidder(clickValue_oil*ctr_oil)
      val bidder_software = new LinearBidder(clickValue_software*ctr_software)
      val bidder_tire = new LinearBidder(clickValue_tire*ctr_tire)
      val bidder_eCommChina = new LinearBidder(clickValue_eCommChina*ctr_eCommChina)
      val bidder_eCommInt = new LinearBidder(clickValue_eCommInt*ctr_eCommInt)
      Array(bidder_oil, bidder_software, bidder_tire, bidder_eCommChina, bidder_eCommInt) 
    }
    case "LinearWithPacingPastCPM" => {
      val bidder_oil = new LinearBidderWithPacing(clickValue_oil*ctr_oil)
      val bidder_software = new LinearBidderWithPacing(clickValue_software*ctr_software)
      val bidder_tire = new LinearBidderWithPacing(clickValue_tire*ctr_tire)
      val bidder_eCommChina = new LinearBidderWithPacing(clickValue_eCommChina*ctr_eCommChina)
      val bidder_eCommInt = new LinearBidderWithPacing(clickValue_eCommInt*ctr_eCommInt)
      Array(bidder_oil, bidder_software, bidder_tire, bidder_eCommChina, bidder_eCommInt) 
    }  
    case "ShadedValueBidder" => {
      val bidder_oil = new ShadedValueBidder(0.0)
      val bidder_software = new ShadedValueBidder(0.0)
      val bidder_tire = new ShadedValueBidder(0.0)
      val bidder_eCommChina = new ShadedValueBidder(0.0)
      val bidder_eCommInt = new ShadedValueBidder(0.0)
      Array(bidder_oil, bidder_software, bidder_tire, bidder_eCommChina, bidder_eCommInt) 
    }  
    case any => throw new NoSuchElementException(s" $any is not a valid Bidder.")
  } 
  
  
  val campaigns_per_budget: Array[Array[Campaign]] = budgetFactors.map(budgetFactor => {
    var Array(bidder_oil, bidder_software, bidder_tire, bidder_eCommChina, bidder_eCommInt) =
      getBidders
    val oil = new Campaign("Oil", startTime, endTime, budgetFactor*fullBudget_oil, bidder_oil, clickValue_oil, eCTR_factor_oil, budgetFactor, enableEventLogging)
    val software = new Campaign("Software", startTime, endTime_software, budgetFactor*fullBudget_software, bidder_software, clickValue_software, eCTR_factor_software, budgetFactor, enableEventLogging)         
    val tire =     new Campaign("Tire", startTime, endTime, budgetFactor*fullBudget_tire, bidder_tire, clickValue_tire, eCTR_factor_tire, budgetFactor, enableEventLogging)
    val eCommChina =  new Campaign("Chinese vertical e-commerce", startTime, endTime, budgetFactor*fullBudget_eCommChina, bidder_eCommChina, clickValue_eCommChina, eCTR_factor_eCommChina,budgetFactor, enableEventLogging)
    val eCommInt =    new Campaign("International e-commerce", startTime, endTime, budgetFactor*fullBudget_eCommInt, bidder_eCommInt, clickValue_eCommInt, eCTR_factor_eCommInt,budgetFactor, enableEventLogging)
    Array(oil, software, tire, eCommChina, eCommInt)
    }
  )
  
  val campaigns = campaigns_per_budget.reduce(_++_)
  
 
  
  
  println("Starting Simulation...")
  
  
  val filenames = new File(input_path)
    .listFiles
    .filter(_.isFile())
    .map(_.getName)
    .filter(_.endsWith(".csv"))     
  
  val numOfFiles = filenames.length
  var i: Int = 0
  
  for (filename <- filenames){
    i += 1
    println(s"Processing file $i of $numOfFiles...")
    val input_file = Source.fromFile(input_path ++ filename)
    for (line <- input_file.getLines.drop(1)){ //drop first line as each file contains header
      val raw = RawBidRequestReader.readLine(line)
      campaigns.foreach(_.handleBidRequest(raw))
    }
    input_file.close()  
  }
  println("Simulation done. Writing logs...")
  
  /*
   * Write Campaign wide Logs
   */
  
  val logs = campaigns.map(_.getCampaignMetrics).reduce (_ ++ _)
  
  scala.tools.nsc.io.Path(output_dir).createDirectory()
  scala.tools.nsc.io.File(output_dir ++ "Campaign Metrics.txt").writeAll(logs) 
  
  /*
   * Write Line to Spreadsheet
   */
  val logLines = campaigns.map(c => s"$sysdate,$dataset,$biddertype," ++ c.getLogLine).reduce(_++_)
  
  val spreadsheetPath = loggingPath ++ "allSimulations.csv"
  //val spreadsheetPath = loggingPath ++ "Shader.csv" // was used for fine tuning Bid Shading
  scala.tools.nsc.io.File(spreadsheetPath).appendAll(logLines)
  
  /*
   * Write Detailed Event Logs
   */
  if (enableEventLogging){
    for (campaign <- campaigns){
      val logname = campaign.uid ++ "_EventLog.csv"
      val file = new File(output_dir++logname)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("dataset,budgetFactor,campaign,b_id,timestamp,privateValue,eCTR,eWP,bid,impression,click,conversion,payingPrice\n")
      
      for (event <- campaign.events)
        bw.write(s"$dataset,${campaign.budgetFactor},$campaign," ++ eventToString(event))
      
      bw.close()
    }
  }
  
  println("done.")
}


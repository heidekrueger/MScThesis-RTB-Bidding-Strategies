import java.time._
import scala.collection.mutable.Queue
import HelperFunctions._
import scala.math._

/*
 * Defines an ad-campaign
 */

class Campaign(
    val advertiserID: String,
    val startTime: LocalDateTime,
    val scheduledEndTime: LocalDateTime,
    val originalBudget: Double,
    val biddingStrategy: BiddingStrategy,
    val clickValue: Double,
    val eCtrFactor: Double,
    val budgetFactor: Double, //only used for logging purposes! TODO: doesn't belong here conceptually
    val loggingEnabled: Boolean //Requires ~400MB of RAM per campaign! Only use when running few campaings simultaneously
    ) {

  val uid = Array(advertiserID, budgetFactor.toString, biddingStrategy.uid).reduce(_ ++ "_" ++ _)
  
  /* update parameter for Landscape in [0,1]:
   * 1: Take training landscape only
   * 0: Take previous hour's landscape only
   * 0 < lambda < 1: mixture
   */  
  val landscapeTrainingWeight: Double = 0.8 
  
  /*
   * State
   */  
  
  
  var remainingBudget: Double = originalBudget
  var currentTime: LocalDateTime = getDateTime("20130613000000001")
  var outOfBudget = false
  var endTime = scheduledEndTime
  
  var currentDay = currentTime.getDayOfWeek.toString
  var currentHour = currentTime.getHour
  
  var prevBidLandscape = new BidLandscape(0, 50, 1, 0.1)
  var currentBidLandscape = BidLandscapeServer.getBidLandscape(advertiserID, currentDay, currentHour)
  
  var eClicks = 0.0 //Expected number of clicks won (a priori)
  var numBids = 0
  var numImpressions = 0
  var numRequests = 0
  var numClicks = 0
  var numConversions =0 
  var numLost = 0 //lost Auctions
  var numPassed = 0 //Auctions not taken part in
  var numMissed = 0 // Auctions not seen (campaign inactive) 
  var aPrioriValue = 0.0 //A priori Surplus based on Exp. Value
  val events: scala.collection.mutable.Queue[CampaignBidEvent] = Queue()
  
  var campaignMu = 0.0
  var campaignSS = 0.0
  
  /*
   * Hourly State for Bid-Landscaping
   */
  var numRequestsHourly = 0
  var numImpressionsHourly = 0.0
  var numClicksHourly = 0
  var budgetSpentHourly = 0.0
  var muHourly = 0.0
  var SSHourly = 0.0 // sums of squares of errors for calculating sigma
  
  biddingStrategy.tune(this)
  
  
  
  private def updateCampaignLandscape(click: Boolean, conv: Boolean, price: Double) = {
    numImpressions +=1       
    if (click) numClicks += 1
    if (conv) numConversions +=1
    
    val delta = price - campaignMu
    campaignMu += delta/numImpressions
    campaignSS += delta * (price - campaignMu)
  }
  
  def getCampaignWideLandscape: BidLandscape = {
    if (numImpressions == 0) 
      return currentBidLandscape //return dummy for error handling
    var sigma = 0.0
    try {
      if (numImpressions < 2) throw new RuntimeException("Less than 2 Impressions. Cannot create distribution.")
      else if (campaignSS == 0.0) throw new RuntimeException("Observations have no variance, cannot create distribution.")
      
      sigma = math.sqrt(campaignSS/(numImpressions -1))
    }
    catch{
      case e: Exception => {
        sigma = 1.0
        println(e.getMessage() ++ " Setting arbitrary sigma of 1.0 instead.")
      }
    }    
    new BidLandscape(numRequests.toDouble / hoursEvolved, campaignMu, sigma, numClicks/numImpressions)
  }
  
  def campaignWideCTR = {
    val res = numClicks.toDouble/numImpressions
    if (res>0)
      res
    else 8e-4 // guesstimate
  }
  
  // called when an impression is won
  private def updateHourlyLandscape(click: Boolean, price: Double){
    numImpressionsHourly += 1
    if (click) numClicksHourly += 1
    budgetSpentHourly += price
    
    val delta = price - muHourly
    muHourly += delta/numImpressionsHourly
    SSHourly += delta * (price - muHourly)
  }
  
  private def resetHourlyLandscape ={
    numRequestsHourly =0
    numImpressionsHourly = 0.0
    numClicksHourly =0
    budgetSpentHourly = 0.0
    muHourly = 0.0
    SSHourly = 0.0
  }
  
  private def getHourlyLandscape = {
    var sigmaHourly = 0.0
    try {
      if (numImpressionsHourly < 2) throw new RuntimeException("Less than 2 Impressions. Cannot create distribution.")
      else if (SSHourly == 0.0) throw new RuntimeException("Observations have no variance, cannot create distribution.")
      
      sigmaHourly = math.sqrt(SSHourly/(numImpressionsHourly -1))
    }
    catch{
      case e: Exception => {
        sigmaHourly = 1.0
        //println(e.getMessage() ++ " Setting arbitrary sigma of 1.0 instead.")
      }
    }
    
    new BidLandscape(numRequestsHourly, muHourly, sigmaHourly, numClicksHourly/numImpressionsHourly)
  }
  
  /*  
   *  Campaign State Checkers
   */
  
  def hasEnded = currentTime.isAfter(endTime) || remainingBudget <= 0.0
  def hasStarted = currentTime.isAfter(startTime) 
  def isActive = !outOfBudget && hasStarted && !hasEnded
  
  def hoursRemaining = Duration.between(currentTime, endTime).toHours+1 // count partial hours as well
  def hoursEvolved = Duration.between(startTime, currentTime).toHours+1 // count started hours
  
  
  /*
   * Only this method should be used to change the current time of the campaign!
   * Gets the new time and, if a new hour has been started, calls the updateHour method.
   */
  private def updateCurrentTime(newTime: LocalDateTime) = {
    currentTime = newTime
    
    if (newTime.getHour != currentHour && isActive) //don't bother updating this anymore when inactive
      updateHour
  }
  
  /*
   * Updates currentBidLandscape and tunes bidding strategy
   * Called by updatecurrentTime when a new hour has started.
   */
  private def updateHour = {
    prevBidLandscape = getHourlyLandscape
    resetHourlyLandscape
    
    //println(s"Training: $currentBidLandscape")
    //println(s"Actual: $prevBidLandscape")
    currentHour = currentTime.getHour
    currentDay = currentTime.getDayOfWeek.toString
    
    currentBidLandscape = 
      BidLandscapeServer.getBidLandscape(advertiserID, currentDay, currentHour)
      .mix(prevBidLandscape, landscapeTrainingWeight)
      
    biddingStrategy.tune(this)
  }
  
  /*
   * Bid Landscape Checker
   */
  def getCurrentBaseCTR = currentBidLandscape.ctr //avg CTR of current Landscape
  def getCPM = (originalBudget - remainingBudget)/numImpressions //gets current CPM of the campaign
  
  
  /* -----------------------------------------------
   * bidRequest specific Methods
   */

  def getPrivateValue(bidRequest: BidRequest): Double = getCTREstimate(bidRequest)*clickValue
  
  def getCTREstimate(bidRequest: BidRequest): Double = 
    if (bidRequest.ctr_prediction>0.0) min(bidRequest.ctr_prediction / eCtrFactor, 1.0) else 0.0 
  def getWPEstimate(bidRequest: BidRequest): Double = bidRequest.wp_prediction
  
  
  /*
   * Reads a RawBidRequest and casts it to a BidRequest that contains the relevant information
   */
  def convertRawBidRequest(raw: RawBidRequest): BidRequest = {
    val ctr_estimator = advertiserID match {
      case "Oil" => raw.GBRT_prediction_Oil
      case "International e-commerce" => raw.GBRT_prediction_International_eCommerce
      case "Chinese vertical e-commerce" => raw.GBRT_prediction_Chinese_vertical_eCommerce
      case "Software"=> raw.GBRT_prediction_Software
      case "Tire" => raw.GBRT_prediction_Tire
      case _ => throw new IllegalArgumentException("No such advertiser")
    }
    
    val is_click = raw.Click>0
    val is_conversion = raw.Conversion>0
    
    new BidRequest(raw.Bid_id,
        raw.Timestamp,
        is_click,
        is_conversion, 
        raw.AdSlotFloorPrice,
        raw.PayingPrice,
        ctr_estimator,
        raw.WpPrediction)        
  }
    
  /* gets the _expected_ KPI for a bid-request */
  final def getKPI(kpi: String, bidRequest: BidRequest): Double = kpi match {
    case "Surplus" => getPrivateValue(bidRequest) - getWPEstimate(bidRequest)
    case "ROI"  => getKPI("Surplus", bidRequest)/getWPEstimate(bidRequest) - 1.0
    case other => throw new IllegalArgumentException(s"No such KPI: $other")
  }
    
  // Evaluates a bidRequest thru the current BiddingStrategy  
  private final def getBidAmount(bidRequest: BidRequest) = biddingStrategy.bid(this, bidRequest)  
  // Submits a bid to the auctioneer
  private final def submitBid(bidRequest: BidRequest, bid: Double): BidResult = bidRequest.checkBid(bid)
  
  
  /*
   * Logic that deals with an incoming bid-request:
   * 1. Is the campaign currently taking requests?
   * 2. Does it decide to bid?
   * 3. Does it win the auction?
   */
  final def handleBidRequest(rawBidRequest: RawBidRequest) = {
    if (rawBidRequest.Advertiser == advertiserID){ // request belongs to this campaign    
      val bidRequest: BidRequest = convertRawBidRequest(rawBidRequest)
      
      numRequests += 1      
      
      assert(!bidRequest.time.isBefore(currentTime), 
          //duplicate times exist in the dataset -> allow equality
          s"$bidRequest time is in the past for $uid!\n" ++
          s"Campaign time: $currentTime, request: "++bidRequest.time.toString)
      
      // update the internal clock of the campaign
      updateCurrentTime(bidRequest.time)
      
      numRequestsHourly += 1
      
      if(isActive){// campaign is running, consider bidding on the request 
        
        val bid = getBidAmount(bidRequest)          
        if (bid > 0.0) { // making a bid for this request  
          
          val privateValue = getPrivateValue(bidRequest) // for logging purposes
          val result = submitBid(bidRequest, bid)
          val event =
            CampaignBidEvent(currentTime, privateValue, getCTREstimate(bidRequest),
              bidRequest.wp_prediction, result)
          
          if (loggingEnabled) events += event
          numBids+=1
          
          if (result.is_imp){ // auction has been won! 
            updateCampaignLandscape(result.is_click, result.is_conversion, result.payingPrice)
            eClicks += event.eCTR
            aPrioriValue += privateValue
            remainingBudget -= result.payingPrice
            
            updateHourlyLandscape(result.is_click, result.payingPrice)
            
            if (remainingBudget <= 0.0){
              outOfBudget = true
              endTime = currentTime
              println(s"$currentTime\t $advertiserID $budgetFactor\t Out Of Budget!, ending campaign")
            }
          } else {/* auction has been lost :( */
            numLost += 1
          }
          
        } else {/* choose not to bid on this request */
          numPassed += 1
        }
        
      } else {/* campaign is inactive: not started yet, ended, or no budget left */
        numMissed += 1
      }
    } else {/* request belongs to another campaign */}
    
  }
  
  /*
   * --------------------------------------------------------------------------------------
   * Logging Methods
   * --------------------------------------------------------------------------------------
   */
  
  /*
   * Gets a one-line log of the campaign to be added to the analysis spreadsheet.
   */
  def getLogLine: String ={
    val budgetSpent = originalBudget -remainingBudget
    val ctr = numClicks.toDouble/numImpressions
    val cvr = numConversions.toDouble/numClicks
    val cpi = budgetSpent/numImpressions
    val cpc = budgetSpent/numClicks
    val bidRateTotal = (numImpressions + numLost).toDouble/(numRequests)
    val bidRateSeen = (numImpressions + numLost).toDouble/(numRequests-numMissed)
    val winRate = numImpressions.toDouble / (numImpressions+numLost)
    val actualValue = numClicks*clickValue
    val aPrioriSurplus = aPrioriValue - budgetSpent
    val actualSurplus = actualValue - budgetSpent
    val aPrioriROI = aPrioriSurplus/budgetSpent
    val actualROI = actualSurplus/budgetSpent
    val eCTR = eClicks/numImpressions
    
    s"$budgetFactor,$advertiserID,${biddingStrategy.uid},${startTime.toString},${scheduledEndTime.toString},${endTime.toString},"++
    s"$numRequests,$numImpressions,$numLost,$numPassed,$numMissed,$numClicks,$numConversions,"++
    f"$originalBudget%1.2f,$budgetSpent%1.2f,$remainingBudget%1.2f,$bidRateSeen%1.3f,$bidRateTotal%1.3f,$winRate%1.3f,"++
    f"$ctr%1.5f,$cvr%1.5f,$cpi%1.2f,${cpc/1000.0}%1.3f,$actualValue%1.2f,$actualSurplus%1.2f,$actualROI%1.4f," ++
    f"$eClicks%1.1f,$eCTR%1.5f,$aPrioriValue%1.1f,$aPrioriSurplus%1.1f,$aPrioriROI%1.4f"++
    "\n"
  }
  
  /*
   * Gets human-readable summary of the campaign
   */  
  def getCampaignMetrics: String = {   
    val budgetSpent = originalBudget -remainingBudget
    val ctr = numClicks.toDouble/numImpressions
    val cvr = numConversions.toDouble/numClicks
    val cpi = budgetSpent/numImpressions
    val cpc = budgetSpent/numClicks
    val bidRateTotal = (numImpressions + numLost).toDouble/(numRequests)
    val bidRateSeen = (numImpressions + numLost).toDouble/(numRequests-numMissed)
    val winRate = numImpressions.toDouble / (numImpressions+numLost)
    val actualValue = numClicks*clickValue
    val aPrioriSurplus = aPrioriValue - budgetSpent
    val actualSurplus = actualValue - budgetSpent
    val aPrioriROI = aPrioriSurplus/budgetSpent
    val actualROI = actualSurplus/budgetSpent
    val eCTR = eClicks/numImpressions
    
    
    "-------------------------------\n"++
    "-------------------------------\n"++
    "-------------------------------\n\n"++
    "Campaign Status\n" ++
    s"$uid\n\n" ++
    s"Bidder: ${biddingStrategy.uid}\n\n" ++
    s"Advertiser: $advertiserID\n" ++
    "Start Time: " ++ startTime.toString ++ "\n" ++
    "Scheduled End Time: " ++ scheduledEndTime.toString ++ "\n" ++
    "Actual End Time: " ++ endTime.toString ++ "\n\n" ++
    s"Total Requests: $numRequests\n"++
    s"Impressions Won: $numImpressions\n" ++
    s"Impressions Lost: $numLost\n" ++
    s"Requests passed on: $numPassed\n" ++
    s"Requests Missed: $numMissed\n" ++    
    s"Clicks: $numClicks \n" ++
    s"Conversions: $numConversions\n\n"++
    f"a priori exp. clicks: $eClicks%1.2f\n"++
    f"a priori eCTR: $eCTR%1.5f\n\n"++
    f"Original Budget: $originalBudget%1.0f\n"++
    f"Budget Spent: $budgetSpent%1.1f\n" ++
    f"Remaining budget: $remainingBudget%1.1f\n\n" ++
    "--- Metrics: ---\n\n" ++
    f"BidRate (Seen): $bidRateSeen%1.3f\n"++
    f"BidRate (Total): $bidRateTotal%1.3f\n" ++
    f"winRate: $winRate%1.3f\n\n"++
    f"CTR: $ctr%1.5f\n"++
    f"CVR: $cvr%1.5f\n"++
    f"CPM: $cpi%1.2f\n"++
    f"CPC: ${cpc/1000.0}%1.2f\n\n"++ 
    f"a priori value: $aPrioriValue%1.1f\n"++
    f"actual value: $actualValue%1.1f\n"++
    f"a priori Surplus: $aPrioriSurplus%1.1f\n"++
    f"actual Surplus: $actualSurplus%1.1f\n"++
    f"a priori ROI: $aPrioriROI%1.3f\n"   ++
    f"Actual ROI: $actualROI%1.3f\n\n\n" 
  }    
}


/*
 * Class representing an event where the campaign submitted a bid. Used for logging purposes.
 */
case class CampaignBidEvent(time: LocalDateTime, privateValuation: Double, eCTR: Double, eWP: Double, result: BidResult)

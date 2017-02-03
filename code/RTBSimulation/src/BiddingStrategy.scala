import HelperFunctions._

/*
 * superclass for defining BiddingStrategies

 */

abstract class BiddingStrategy {
  val uid: String
  val n_decimals = 2
  
  def getBid(campaign: Campaign, bidRequest: BidRequest): Double
    
  /* Gets expected spending based on estimated future  */  
  def getExpectedSpending(campaign: Campaign, bidLevel: Double) ={
    val hours = getHoursBetween(campaign.currentTime, campaign.endTime)
    val landscapes = hours.map(BidLandscapeServer.getBidLandscape(campaign.advertiserID, _))
    /*Variant 1: *based on landscape cpm*/
    //val hourly_spend = landscapes.map {bl => bl.requestRate * bl.getWinProbability(bidLevel) * bl.mu  
    /*Variant 2: based on campaign cpm */
    val hourly_spend = landscapes.map {bl => bl.requestRate * bl.getWinProbability(bidLevel) * campaign.getCPM 
    }
    hourly_spend.reduce(_ + _)
  }
  
  def tune(campaign: Campaign) = {    
  }
  
  
  /*
   * Gets the bid. 
   * Note: The actual logic is contained in the (private getBid method.)
   * This is a wrapper to round it to 2 decimals
   */
  final def bid(campaign: Campaign, bidRequest: BidRequest): Double =
    "%.2f".format(getBid(campaign, bidRequest)).toDouble   
  
}


/* Bids randomly (uniformly distr.) in a specified Interval */
class RandomBidder(minBid: Double, maxBid: Double) extends BiddingStrategy{ 
  val uid = s"RandomBidder _m$minBid _M$maxBid"
  val rand = new scala.util.Random()
  
  def getBid(campaign: Campaign, bidRequest: BidRequest) =
    minBid + (maxBid-minBid)*rand.nextDouble
}

/*
 * Bids Randomly with dynamic (request specific) boundaries
 */
class DynamicRandomBidder(
    minBidRule: ((BidRequest,Campaign) => Double),
    maxBidRule: ((BidRequest,Campaign) => Double)) 
  extends BiddingStrategy{
  val comment = "UpToValue"
  val uid = s"DynamicRandomBidder_$comment"
  
  
  def getBid(campaign: Campaign, bidRequest: BidRequest) ={    
    val minBid = minBidRule(bidRequest, campaign)
    val maxBid = maxBidRule(bidRequest, campaign)
    new RandomBidder(minBid, maxBid).getBid(campaign, bidRequest)
  }    
}


/* Always bids a constant amount */
class ConstantBidder(val bidLevel: Double) extends BiddingStrategy {
  val uid = f"ConstantBidder_$bidLevel%1.2f"
  def getBid(campaign: Campaign, bidRequest: BidRequest) = 
    bidLevel
}


/* Bids Proportional to a Baseline according to eCTr and current Campaign ctr */
class LinearBidder(var baselineLevel: Double) extends BiddingStrategy {
  val uid = s"LinearBidder_$baselineLevel"
  def getBid(campaign: Campaign, bidRequest: BidRequest) =
    baselineLevel * campaign.getCTREstimate(bidRequest)/campaign.currentBidLandscape.ctr
}

class LinearBidderWithPacing(initialBaselineLevel: Double) extends LinearBidder(initialBaselineLevel) {
  override val uid= s"LinearBidderWithPacing"
  
  override def tune(c: Campaign) = {
    val tol = 1e3
    val remainingBudget = c.remainingBudget
    var baseline = baselineLevel
    var expectedSpend = getExpectedSpending(c, baseline)
    var iters = 0
    var min = 0.0
    var max = 310.0
    do {
      iters += 1
      
      if (expectedSpend < remainingBudget){
        min = baseline
        baseline = (baseline+max)/2 
      }
      else {
        max = baseline
        baseline = (baseline+min)/2
      }      
      expectedSpend = getExpectedSpending(c, baseline)         
    } while (math.abs(expectedSpend - remainingBudget) > tol && iters<20)
    baselineLevel = baseline
  }

}


/* Always bids the campaigns private value. */
class ValueBidder extends BiddingStrategy{
  val uid = "ValueBidder"
  def getBid(campaign: Campaign, bidRequest: BidRequest) = 
    campaign.getPrivateValue(bidRequest)
}

/* Bids the campaigns private value, IF some KPI exceeds a certain threshold.
 * 
 * This might be used for example to bid on the top x% of requests according to ROI.
 */
class ThresholdValueBidder(kpi: String, initialThreshold: Double) extends BiddingStrategy{
  val uid = s"ThresholValueBidder_$kpi"
  
  var threshold = initialThreshold
  
  def getBid(campaign: Campaign, bidRequest: BidRequest) = 
    if (campaign.getKPI(kpi, bidRequest) >= threshold)
      campaign.getPrivateValue(bidRequest)
    else 0.0 //do not bid  
}
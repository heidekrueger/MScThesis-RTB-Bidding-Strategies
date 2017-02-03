import breeze.stats.distributions._
import HelperFunctions._

class ORTBidder(val comment: String) extends BiddingStrategy {
  
  def this() = this("Default")
  
  val uid = s"ORTBidder_$comment"
  
  val stn = new Gaussian(0,1)
  
  var lambda_ctr = 5.2e-7 // original value from Zhang et al 2014, used as initial value before tuning
    
  var location = 3.997866 // initial values for location and scale, correspond to overall Training landscape
  var scale = 0.6881228
  
  
  val upperBidBound = 310
    
  /*
   * Tunes Lambda according to Current Campaign state  
   */
  override def tune(c: Campaign){
    val debug = false 
    
    location = c.currentBidLandscape.location
    scale = c.currentBidLandscape.scale
    
    if (c.currentTime == c.startTime){
      //no tuning at initialization)
    }
    else comment match {
      case "DynamicLambdaPastCPC" =>{
        if (c.numClicks >0) 
          lambda_ctr = c.numClicks / (c.originalBudget-c.remainingBudget)
        else {} //keep initial value
      }
      case "DynamicDualProblem" =>{
        /* Find lambda by Primal-Dual /EM method:
         * - Update lambda
         * - get expected spend
         * - repeat until eSpend matches mudget
         */
        
        val maxIter = 20
        val tol = 1e2
        var minLambda = 1e-10
        var maxLambda = 1e-3
        
        if(c.hoursRemaining == 0) //should never happen, in this case, just take previous lambda
          throw new Exception("remaining time is 0")
        
        val avgHourlyBudget = c.remainingBudget / c.hoursRemaining
        val landscape = c.getCampaignWideLandscape
        
        
        def updateBidLevel = solveEulerLagrange(
              math.min(upperBidBound, c.remainingBudget),
              c.campaignWideCTR,
              lambda_ctr,
              landscape.location,
              landscape.scale)
        
        var avgBidLevel = updateBidLevel
        
        var expectedHourlySpend = getExpectedSpending(c, avgBidLevel)/c.hoursRemaining    
        
        var i = 0
        do{ // find lambda by bisection
          i+= 1
          
          if (expectedHourlySpend < avgHourlyBudget){
            maxLambda = lambda_ctr
            lambda_ctr = (lambda_ctr+minLambda)/2
          } else{
            minLambda = lambda_ctr
            lambda_ctr = (lambda_ctr+maxLambda)/2
          }
        
        avgBidLevel = updateBidLevel
          
        expectedHourlySpend = getExpectedSpending(c, avgBidLevel)/c.hoursRemaining   
          
        if (debug)
          println(s"Iteration: $i, budget: $avgHourlyBudget, spend: $expectedHourlySpend")
          
        } while (math.abs(avgHourlyBudget - expectedHourlySpend)>tol && i<=maxIter)

        
      }
    }
    
    
      if (debug) println(s"${c.uid}, ${getDayHour(c.currentTime)}, lambda: $lambda_ctr")
  }
  
  
  def getBid(campaign: Campaign, bidRequest: BidRequest) = {
    /* Set upper bound for bidding. 300 is highest price in dataset, but set
     * slightly higher due to asymptotic behavior of the solution of euler Lagrange equation
     * (if unconstrained, will approach upperBound but never reach it)  */

    solveEulerLagrange(
        math.min(upperBidBound, campaign.remainingBudget),
        campaign.getCTREstimate(bidRequest),
        lambda_ctr,
        location,
        scale)
  }
  
  
  def solveEulerLagrange(
      maxBid: Double, // maximum allowed Bid
      kpi: Double,   // Estimated KPI
      lambda: Double, // Lagrange Multiplicator, depends on KPI. --> Tuned using Training
      mu: Double, // Location Parameter of current Bid Landscape
      sigma: Double // scale Parameter of current Bid Landscape
      ) ={
    val debug = false
    val maxIter = 20
    // check if lambda >0, otherwise infinite loop.
    //if (lambda <= 0.0) throw new IllegalArgumentException("Illegal Lagrange multiplicator.")
    
    var min = 0.0
    var max = maxBid
    
    var b = max /2
    
    var x= (math.log(b)-mu)/sigma
    var left = lambda*stn.cdf(x)
    var right = (kpi - lambda*b)/(sigma*b)*stn.pdf(x)
    
    var count = 0
    
    do {     
      count+= 1
      if (left<right){
        min = b
        b = (max+b)/2        
      }
      else {
        max = b
        b = (min+b)/2
      }
        
      var x= (math.log(b)-mu)/sigma
      
      left = lambda*stn.cdf(x)
      right = (kpi - lambda*b)/(sigma*b)*stn.pdf(x)
      
      if (debug){
        println(count)
        println(s"\tb: $b")
        println(s"\tleft: $left")
        println(s"\tright:$right")
      }
    } while (math.abs(left - right) > 1e-10*lambda && count < maxIter)

    b
  }
  
}
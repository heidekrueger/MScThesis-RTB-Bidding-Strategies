

class ShadedValueBidder(var lambda: Double) extends BiddingStrategy {
  override val uid= s"ShadedValueBidder"
  
  def getBid(campaign: Campaign, bidRequest: BidRequest) = 
    campaign.getPrivateValue(bidRequest)/(1.0+lambda)
  
  
  /*
   * Tunes lambda by solving the dual problem, i.e. matching expected spending with the remaining
   * budget.
   * This is done in an EM-Fashion by updating lambda using binary search and re-evalating 
   * the expected spend until spending and budget are the same (within tolerance) or max number
   * of iterations is reached.
   */
    
  override def tune(c: Campaign) = {
    val tol = 1e2
    val remainingBudget = c.remainingBudget
    var newlambda = lambda
    /*
     * The following line is a heuristic that has been tuned manually (factor 0.3) to yield 
     * satisfactory results.
     * 
     * Should in the future be replaced by some more warranted version that represents
     * the expected average bid in the remaining future bid landscape.
     */
    def getAvgBid(lambda: Double) = (c.clickValue * 0.3*c.campaignWideCTR)/(newlambda +1)
    
    var avg_bid = getAvgBid(newlambda)
    var expectedSpend = getExpectedSpending(c, avg_bid)
    var iters = 0
    var min = 0.0
    var max = 10.0
    do {
      iters += 1
      
      if (expectedSpend < remainingBudget){
        max = newlambda
        newlambda = (newlambda+min)/2 
      }
      else {
        min = newlambda
        newlambda = (newlambda+max)/2
      }
      avg_bid = getAvgBid(newlambda)
      expectedSpend = getExpectedSpending(c, avg_bid)         
    } while (math.abs(expectedSpend - remainingBudget) > tol && iters<20)
    lambda = newlambda
    //println(lambda)
  }

}
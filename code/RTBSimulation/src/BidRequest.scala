/*
 * Represents a BidRequest as seen by a campaign
 */
import java.time._
import HelperFunctions._

class BidRequest(
    val b_id: String,
    val timestamp:String,
    val is_click: Boolean,
    val is_conversion: Boolean,
    val floorPrice: Double,
    val winning_price: Double,
    val ctr_prediction: Double,
    val wp_prediction: Double) {

  lazy val time: LocalDateTime = getDateTime(timestamp)

  
  /*
   * Simulates the auction corresponding to this bid request,
   * returns the BidResult when submitting a bid
   */
  
  def checkBid(bid: Double): BidResult = {
    if (bid >= winning_price && bid >= floorPrice) // Won in Second Price Auction
      BidResult(b_id, timestamp, bid, winning_price, true, is_click, is_conversion)
    else if (bid >= winning_price) // Below Floor --> Won in First Price Auction
      BidResult(b_id, timestamp, bid, bid, true, is_click, is_conversion)
    else // Lost
      BidResult(b_id, timestamp, bid, 0.0, false, false, false)
  }  
}


case class BidResult(
    b_id: String,
    timestamp: String,
    bid: Double,
    payingPrice: Double,
    is_imp:Boolean, 
    is_click: Boolean,
    is_conversion: Boolean
    )
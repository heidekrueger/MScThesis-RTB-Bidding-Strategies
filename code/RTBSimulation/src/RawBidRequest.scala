import com.github.marklister.collections.io.CsvParser

/* A bid request as read in from disk.
 * Note that the actual logic on campaign level will be done using the class
 * BidRequest.
 */

case class RawBidRequest(
    Bid_id: String,
    Timestamp: String,
    Impression: Double,
    Click: Double,
    Conversion: Double,
    Advertiser: String,
    AdSlotFloorPrice: Double,
    prediction: Double,
    GBRT_prediction_International_eCommerce: Double,
    GBRT_prediction_Oil: Double,
    GBRT_prediction_Software: Double,
    GBRT_prediction_Tire: Double,
    GBRT_prediction_Chinese_vertical_eCommerce: Double,
    BidPrice: Double,
    PayingPrice: Double,
    WpPrediction: Double    
    )

    // Companion object with factory function
final object RawBidRequestReader {
  def readLine(line: String) = {
    val collSeq = (CsvParser[
        String,String,
        Double,Double,Double,
        String,
        Double,Double,Double,Double,Double,Double,Double,Double,Double, Double]
        .parse(new java.io.StringReader(line)))
    (RawBidRequest.apply _).tupled(collSeq(0))
  }
}
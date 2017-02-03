package org.apache.spark.ml


import scala.util.hashing.MurmurHash3

import org.apache.spark.ml.linalg.{Vector,Vectors,VectorUDT}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

import breeze.linalg.{ Vector => BV, SparseVector => BSV, DenseVector => BDV }


/**
 * Implements Feature Hashing/The Hashing Trick for categorical string columns in DataFrames.
 * (http://alex.smola.org/papers/2009/Weinbergeretal09.pdf)
 *  
 * 
 * Parameters:  inputCols  - Array of Input Columns names to be hashed.
 * 							outputCol  - name of output column
 * 							numFeatures- size of the hashed feature vector
 * 
 * @note: This requires the breeze/scalanlp library, as spark vectors do not
 * 				support vector arithmetic.
 * 
 * 
 * @author Stefan Heidekrüger
 * @version 2016.08.28
 */

final class FeatureHasher(override val uid:String)
  extends Transformer with HasOutputCol with HasInputCols with  DefaultParamsWritable {
  
  
  def this() = this(Identifiable.randomUID("FeatureHasher"))

  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)
  
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0) after hashing",
    ParamValidators.gt(0))
  
  setDefault(numFeatures -> 1000, outputCol -> "hashed_features")
  
  

  
  
  // hashes (categorial) string value to a integer in [0, nFeatures -1]
  private val hashString = udf(
    (str: String) => MurmurHash3.stringHash(str) % $(numFeatures) match {
      case negResult if negResult < 0 => negResult + $(numFeatures)
      case posResult                  => posResult
    }
  )
  
  /* colName and Value should be concatenated before hashing
   * (otherwise NA from each column will be hashed into the same bucket)
   */
  def concatColNameAndValue(name:String) = udf ( (value:String) => name++value)
  
  
  /* maps an index (in [0, nFeatures-1])
   * to a sparse vector of length nFeatures with an entry 1 at the index
   * 
   * This is obsolete: ---> (((We use Breeze Vectors here, as Spark Vectors do not
   * 												 support arithmetic operations, and we
   * 											   will need to add some vectors in the transform method.
   * 									      )))
   * It turns out, you can't store Breeze Vectors in Data Frames, thus we will only
   *  do temporary conversion in the reduce step. 
   * 
   * 
   * Example: nFeatures = 10:   5 => SparseVector(10, Array(5), Array(1.0))
   * 
   */
  private val vecUDF = udf(
      (in: Int) => Vectors.sparse(
        $(numFeatures),
        Array(in),
        Array(1.0)        
      )
  )
  
  
  private def breezeToSpark(breezeVec: BV[Double]) = Vectors.fromBreeze(breezeVec)  
  private def sparkToBreeze(sparkVec: Vector) = sparkVec.asBreeze  
  
  // udf to get the sum of two columns
  private val vectorSum = udf( 
      (vec1: Vector, vec2: Vector) => breezeToSpark(vec1.asBreeze+vec2.asBreeze))


  
  override def transform(input_df: Dataset[_]): DataFrame = {
    
    var df = input_df
    // hash and vectorize each input column into a temporary new column
    for (colName <- $(inputCols))
      df = df.withColumn(
          "VEC_"++colName,
          vecUDF(
              hashString(
                  concatColNameAndValue(colName)(col(colName))
              )
          )
      )
    
      
    val vecCols = $(inputCols).map( "VEC_"++_ ) 
    
    
    // return DataFrame with the sum of vectors attached but without the temporary columns
    df.withColumn(
        "FT_hashed",
        vecCols
          .map(name => col(name))
          .reduce (vectorSum(_,_))        
      )     
      .drop(vecCols : _*)

  }
    
  override def transformSchema(schema: StructType): StructType = {
    val outputColName = $(outputCol)

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, true))
  }
  
  override def copy(extra: ParamMap): FeatureHasher = defaultCopy(extra)
}
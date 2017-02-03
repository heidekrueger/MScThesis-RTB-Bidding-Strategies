package org.apache.spark.ml

import org.apache.spark.ml.feature._
import org.apache.spark.ml._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.ml.linalg.VectorUDT


/**
 * In essence, this is simply a VectorAssembler, that takes as input columns all columns
 * that begin with "FT_".
 * 
 * @author Stefan Heidekrüger
 * @date 2016-08-27
 */


final class FtColumnAssembler(override val uid: String)  
  extends Transformer with HasOutputCol {

  def this() = this(Identifiable.randomUID("FtColumnAssembler"))

  def setOutputCol(value: String): this.type = set(outputCol, value)
  

  override def transform(df: Dataset[_]): DataFrame = {
    val ftCols = df.columns.filter(_.startsWith("FT_"))
    
    val Assembler = new VectorAssembler()
      .setInputCols(ftCols)
      .setOutputCol($(outputCol))
    
    Assembler.transform(df)    
  }
  
  
  override def transformSchema(schema: StructType): StructType = {
    val outputColName = $(outputCol)

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, true))
  }
  
  override def copy(extra: ParamMap): FtColumnAssembler = defaultCopy(extra)
}

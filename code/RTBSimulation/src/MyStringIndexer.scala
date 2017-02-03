/*
 * Modified original MyStringIndexer implementation that can deal with unseen values.
 * If such a value is encountered, it is encoded as if it were "NA".
 * Furthermore, if no NA is encountered in the training set, this implementation will add a 
 * corresponding index nevertheless (note that this might introduce linear dependency!, this case
 * does not happen in our dataset however).
 */



/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

/**
 * Base trait for [[MyStringIndexer]] and [[MyStringIndexerModel]].
 */
private[feature] trait MyStringIndexerBase extends Params with HasInputCol with HasOutputCol
    /*with HasHandleInvalid*/ with  DefaultParamsWritable {
    
  
  
  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}

/**
 * A label indexer that maps a string column of labels to an ML column of label indices.
 * If the input column is numeric, we cast it to string and index the string values.
 * The indices are in [0, numLabels), ordered by label frequencies.
 * So the most frequent label gets index 0.
 *
 * @see [[IndexToString]] for the inverse transformation
 */
@Since("1.4.0")
class MyStringIndexer @Since("1.4.0") (
    @Since("1.4.0") override val uid: String) extends Estimator[MyStringIndexerModel]
  with MyStringIndexerBase with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("myStrIdx"))

  val handleInvalid = new Param[String](this, "handleInvalid", "How to handle invalid entries, possible values \"error\",,\"skip\",\",setNA\"",
  ParamValidators.inArray(Array("skip", "error", "setNA")))

  
  /** @group setParam */
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, "error")

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): MyStringIndexerModel = {
    transformSchema(dataset.schema, logging = true)
    val counts = dataset.select(col($(inputCol)).cast(StringType))
      .rdd
      .map(_.getString(0))
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    copyValues(new MyStringIndexerModel(uid, labels).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): MyStringIndexer = defaultCopy(extra)
}

@Since("1.6.0")
object MyStringIndexer extends DefaultParamsReadable[MyStringIndexer] {

  @Since("1.6.0")
  override def load(path: String): MyStringIndexer = super.load(path)
}

/**
 * Model fitted by [[MyStringIndexer]].
 *
 * NOTE: During transformation, if the input column does not exist,
 * [[MyStringIndexerModel.transform]] would return the input dataset unmodified.
 * This is a temporary fix for the case when target labels do not exist during prediction.
 *
 * @param labels  Ordered list of labels, corresponding to indices to be assigned.
 */
@Since("1.4.0")
class MyStringIndexerModel (
    @Since("1.4.0") override val uid: String,
    @Since("1.5.0") val labels: Array[String])
  extends Model[MyStringIndexerModel] with MyStringIndexerBase with MLWritable {

  import MyStringIndexerModel._

  @Since("1.5.0")
  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), labels)
  
  val handleInvalid = new Param[String](this, "handleInvalid", "How to handle invalid entries, possible values \"error\",,\"skip\",\",setNA\"",
  ParamValidators.inArray(Array("skip", "error", "setNA")))
  
  def getHandleInvalid: String = $(handleInvalid)
  
  /** @group setParam */
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, "error")
  
  
  private val labelToIndex: OpenHashMap[String, Double] = {
    val n = labels.length
    val map = new OpenHashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.update(labels(i), i)
      i += 1
    }
    /* ensure "NA" is a label when using setNA
     * (This will also ensure that NA is the last index, thus the NA column
     * will be the one that will later be dropped by the OHE, even if it is not the rarest.
     * This in turn will lead to further sparsity.)
     */
    if (getHandleInvalid == "setNA") map.update("NA", i)
    
    map
  }


  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip MyStringIndexerModel.")
      return dataset.toDF
    }
    transformSchema(dataset.schema, logging = true)

    val indexer = udf { label: String =>
      if (labelToIndex.contains(label)) {
        labelToIndex(label)
      } else {
        getHandleInvalid match {
          case "setNA" => labelToIndex("NA")
          case _ => throw new SparkException(s"Unseen label: $label.")
        }
      }
    }
    
    val metadata = NominalAttribute.defaultAttr
      .withName($(outputCol)).withValues(labels).toMetadata()
    // If we are skipping invalid records, filter them out.
    val filteredDataset = getHandleInvalid match {
      case "skip" =>
        val filterer = udf { label: String =>
          labelToIndex.contains(label)
        }
        dataset.where(filterer(dataset($(inputCol))))
      case "setNA" => {
        //val labelsSet = labels.toSet //transform to set for constant time lookup
        val toNAifUnseen = udf {label: String =>
          if (labelToIndex.contains(label)) label else "NA"
        }
        val inputColName = $(inputCol)
        dataset.withColumn(inputColName, toNAifUnseen(dataset(inputColName)))
      }
      case _ => dataset
    }
    //filteredDataset.show()
    filteredDataset.select(col("*"),
      indexer(filteredDataset($(inputCol)).cast(StringType)).as($(outputCol), metadata))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip MyStringIndexerModel.
      schema
    }
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): MyStringIndexerModel = {
    val copied = new MyStringIndexerModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: StringIndexModelWriter = new StringIndexModelWriter(this)
}

@Since("1.6.0")
object MyStringIndexerModel extends MLReadable[MyStringIndexerModel] {

  private[MyStringIndexerModel]
  class StringIndexModelWriter(instance: MyStringIndexerModel) extends MLWriter {

    private case class Data(labels: Array[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.labels)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MyStringIndexerModelReader extends MLReader[MyStringIndexerModel] {

    private val className = classOf[MyStringIndexerModel].getName

    override def load(path: String): MyStringIndexerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("labels")
        .head()
      val labels = data.getAs[Seq[String]](0).toArray
      val model = new MyStringIndexerModel(metadata.uid, labels)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[MyStringIndexerModel] = new MyStringIndexerModelReader

  @Since("1.6.0")
  override def load(path: String): MyStringIndexerModel = super.load(path)
}

/**
 * A [[Transformer]] that maps a column of indices back to a new column of corresponding
 * string values.
 * The index-string mapping is either from the ML attributes of the input column,
 * or from user-supplied labels (which take precedence over ML attributes).
 *
 * @see [[MyStringIndexer]] for converting strings into indices
 */
@Since("1.5.0")
class IndexToString private[ml] (@Since("1.5.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("1.5.0")
  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
   * Optional param for array of labels specifying index-string mapping.
   *
   * Default: Not specified, in which case [[inputCol]] metadata is used for labels.
   * @group param
   */
  @Since("1.5.0")
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")

  /** @group getParam */
  @Since("1.5.0")
  final def getLabels: Array[String] = $(labels)

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val outputFields = inputFields :+ StructField($(outputCol), StringType)
    StructType(outputFields)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputColSchema = dataset.schema($(inputCol))
    // If the labels array is empty use column metadata
    val values = if (!isDefined(labels) || $(labels).isEmpty) {
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      $(labels)
    }
    val indexer = udf { index: Double =>
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    }
    val outputColName = $(outputCol)
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(DoubleType)).as(outputColName))
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): IndexToString = {
    defaultCopy(extra)
  }
}

@Since("1.6.0")
object IndexToString extends DefaultParamsReadable[IndexToString] {

  @Since("1.6.0")
  override def load(path: String): IndexToString = super.load(path)
}
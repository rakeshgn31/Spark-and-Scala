package questions

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext


/**
* GamingProcessor is used to predict if the user is a subscriber.
* You can find the data files from /resources/gaming_data.
* Data schema is https://github.com/cloudwicklabs/generator/wiki/OSGE-Schema
* (first table)
*
* Use Spark's machine learning library mllib's SVM(support vector machine) algorithm.
* http://spark.apache.org/docs/1.6.1/mllib-linear-methods.html#linear-support-vector-machines-svms
*
* Use these features for training your model:
*   - gender 
*   - age
*   - country
*   - friend_count
*   - lifetime
*   - citygame_played
*   - pictionarygame_played
*   - scramblegame_played
*   - snipergame_played
*
*   -paid_subscriber(this is the feature to predict)
*
* The data contains categorical variables, so you need to
* change them accordingly.
* https://spark.apache.org/docs/1.6.1/ml-features.html
*
*/

case class Customer(gender: Double,
                    age: Double,
                    country: String,
                    friend_count: Double,
                    lifetime: Double,
                    game1: Double,
                    game2: Double,
                    game3: Double,
                    game4: Double,
                    paid_customer: Double)

class GamingProcessor() {

  // these parameters can be changed
  val conf = new SparkConf()
  .setAppName("gaming")
  .setMaster("local[*]")
  .set("spark.driver.memory", "3g")
  .set("spark.executor.memory", "2g")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._


  /**
  * convert creates a DataFrame, removes unnecessary columns and converts the rest to right format.
  * http://spark.apache.org/docs/1.6.1/sql-programming-guide.html#creating-datasets
  * Data schema:
  *   - gender: Double (1 if male else 0)
  *   - age: Double
  *   - country: String
  *   - friend_count: Double
  *   - lifetime: Double
  *   - game1: Double (citygame_played)
  *   - game2: Double (pictionarygame_played)
  *   - game3: Double (scramblegame_played)
  *   - game4: Double (snipergame_played)
  *   - paid_customer: Double (1 if yes else 0)
  *
  * @param path to file
  * @return converted DataFrame
  */
  def convert(path: String): DataFrame = {

    sc.textFile(path).map(_.split(","))
      .map(row => {
                      val gender = if(row(3).trim.equalsIgnoreCase("male")) 1 else 0
                      val isPaidCustomer = if(row(15).trim.equalsIgnoreCase("yes")) 1 else 0
                      Customer(gender.toDouble,
                          row(4).toDouble, row(6), row(8).toDouble, row(9).toDouble,
                          row(10).toDouble, row(11).toDouble, row(12).toDouble, row(13).toDouble,
                          isPaidCustomer.toDouble)
                  }).toDF()
  }

  /**
  * indexer converts categorical variables into doubles.l
  * https://en.wikipedia.org/wiki/Categorical_variable
  * https://spark.apache.org/docs/1.6.1/ml-features.html
  * 'country' is the only categorical variable, because
  * it has only 7 possible values (USA,UK,Germany,...).
  * Use some kind of encoder to transform 'country' Strings into
  * 'country_index' Doubles.
  * After these modifications schema should be:
  *
  *   - gender: Double (1 if male else 0)
  *   - age: Double
  *   - country: String
  *   - friend_count: Double
  *   - lifetime: Double
  *   - game1: Double (citygame_played)
  *   - game2: Double (pictionarygame_played)
  *   - game3: Double (scramblegame_played)
  *   - game4: Double (snipergame_played)
  *   - paid_customer: Double (1 if yes else 0)
  *   - country_index: Double
  *
  * @param df DataFrame which has been converted using 'converter'
  * @return Dataframe
  */
  def indexer(df: DataFrame): DataFrame = {

      val countryIndexer = new StringIndexer()
                                  .setInputCol("country")
                                  .setOutputCol("country_index")

      countryIndexer.fit(df).transform(df)
  }

  /**
  * toLabeledPoints converts DataFrame into RDD of LabeledPoints
  * http://spark.apache.org/docs/1.6.1/mllib-data-types.html
  * Label should be 'paid_customer' field and features the rest.
  *
  * To improve performance data also need to be standardized, so use
  * http://spark.apache.org/docs/1.6.1/mllib-feature-extraction.html#standardscaler
  *
  * @param df DataFrame which has been already indexed
  * @return RDD of LabeledPoints
  */
  def toLabeledPoints(df: DataFrame): RDD[LabeledPoint] = {

    val labeledPointRDD = df.map(row => LabeledPoint(row.getDouble(9), Vectors.dense(row.getDouble(0),
                              row.getDouble(1), row.getDouble(3), row.getDouble(4), row.getDouble(5),
                              row.getDouble(6), row.getDouble(7), row.getDouble(8), row.getDouble(10))))

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(labeledPointRDD.map(row => row.features))
    labeledPointRDD.map(row => LabeledPoint(row.label, scaler.transform(Vectors.dense(row.features.toArray))))
  }

  /**
  * createModel creates a SVM model
  * When training, 5 iterations should be enough.
  *
  * @param training RDD containing LabeledPoints (created using previous methods)
  * @return trained SVMModel
  */
  def createModel(training: RDD[LabeledPoint]): SVMModel = {

      val svmModel = new SVMWithSGD()
      svmModel.optimizer
              .setRegParam(0.2)
              .setStepSize(0.5)
              .setUpdater(new L1Updater())
              .setNumIterations(5)

      svmModel.run(training)
  } 


      /**
      * Given a transformed and normalized dataset
      * this method predicts if the customer is going to
      * subscribe to the service. Predicted values MUST be in the same
      * order as the given RDD.
      *
      * @param model trained SVM model
      * @param dataToPredict normalized data for prediction (created using previous toLabeledPoints)
      * @return RDD[Double] predicted scores (1.0 == yes, 0.0 == no)
      */
      def predict(model: SVMModel, dataToPredict: RDD[LabeledPoint]): RDD[Double] = {

          val data = dataToPredict.map(dataPoint => dataPoint.features)
          model.predict(data)
      }
}

/**
*
*  Change the student id
*/
object GamingProcessor {

    val studentId = "546289"
}

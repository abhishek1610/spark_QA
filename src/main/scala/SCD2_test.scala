import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Created by abhishek on 2016-04-18.
 */
class SCD2_test {


  def scd2test (src : DataFrame, tgt : DataFrame, sc: SparkContext) : DataFrame =
  {


   // val out = cust1.cogroup(exist_cust1)//
    // .mapValues(x => (x._1 ++ x._2)) //.flatMap{case (x,y) =>  y.map(z =>  (x,z))}


    //using datframes api
    val joinkey = Array("id")
  //to do
    //val final_oup = tgt.join(src, tgt("id") === src("id"),"inner" )
    //final_oup
    src
  }
}


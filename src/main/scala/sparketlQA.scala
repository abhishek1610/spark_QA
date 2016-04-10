import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.rdd
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._





object sparketlQA {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[2]"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext1 = new org.apache.spark.sql.hive.HiveContext(sc)

    import sqlContext1.implicits._
    val schemaString: String = "id name age"

    val schema1 = new Array[String](3)
    schema1(0) = "id"
    schema1(1) = "name"
    schema1(2)  = "age"
    //val threshold = args(1).toInt
    val feildtype = new Array[String](5)
     feildtype(0) = "String"
     feildtype(1) = "String"
    feildtype(2)  = "int"

    val feild : Array[DataType] = feildtype.filter( x => x != null).map( x =>  Stringtodatatype(x))


  val schema_datatype : Array[Tuple2[String,DataType]] =
    for ((a,b) <- schema1 zip feild)
   /* for {  x <- schema1
          y <- feild
  } */ yield  ( a,b )




   // val feildtype1 : DataType = StringType
    val schema = StructType(schema_datatype.map(fieldName ⇒ StructField(fieldName._1, fieldName._2, true)))

    // split each document into words
    val inp = sc.textFile(args(0))




    //val snpsht = sc.textFile(args(1))
  val test = inp.map(feild => Row.fromSeq(feild.split(",")))
    val inp1 = sqlContext1.createDataFrame(test,schema )
    //.map{x =>(x._1, x._2, x._3, x._4, x._5, x._6, x._7)  }//.toDF()

   inp1.show()
  inp1.registerTempTable("test")
   sqlContext1.sql("CREATE TABLE new_test row format delimited fields terminated by '|' " +
      "STORED AS RCFile AS select id,name,age from test where id=1" )

    sqlContext1.sql("CREATE TABLE new_test_target " +
      " AS select id,name,age from test where id=1" )


    val m1 = Map[String, String]("1"->"2", "2"->"3", "4"->"50")

    val qry_comp = new compare()
    val out= qry_comp.comparequery(m1,"2")
    System.out.println(out)

    }

  def Stringtodatatype (x :String) : DataType = {
    x match {
      case "String" =>  StringType
      case "int" =>  IntegerType
      // case other => other
    }

  }

}
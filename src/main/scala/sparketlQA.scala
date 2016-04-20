import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.rdd
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._



case class Customer ( id : Int, name:String, end_date : String)

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

    //changing col name for target to support join in spark 1.3 not a problem thereafter
    val schematgt = new Array[String](3)
    schematgt(0) = "id1"
    schematgt(1) = "name1"
    schematgt(2)  = "age1"
    //val threshold = args(1).toInt
    //there is some issue while inserting data for int datatype hence keeping as string for time being
    val feildtype = new Array[String](5)
     feildtype(0) = "int"
     feildtype(1) = "String"
    feildtype(2)  = "String"

    val feild : Array[DataType] = feildtype.filter( x => x != null).map( x =>  Stringtodatatype(x))

    val schema_tgt_datatype : Array[Tuple2[String,DataType]] =
      for ((a,b) <- schematgt zip feild)
      /* for {  x <- schema1
             y <- feild
     } */ yield  ( a,b )



    val schema_datatype : Array[Tuple2[String,DataType]] =
    for ((a,b) <- schema1 zip feild)
   /* for {  x <- schema1
          y <- feild
  } */ yield  ( a,b )




   // val feildtype1 : DataType = StringType
    val schema = StructType(schema_datatype.map(fieldName ⇒ StructField(fieldName._1, fieldName._2, true)))

    val schema_tgt = StructType(schema_tgt_datatype.map(fieldName ⇒ StructField(fieldName._1, fieldName._2, true)))

    // split each document into words
    val inp = sc.textFile(args(0))




    //val snpsht = sc.textFile(args(1))
  val test = inp.map(feild => Row.fromSeq(feild.split(",")))
    val inp1 = sqlContext1.createDataFrame(test,schema )
    //.map{x =>(x._1, x._2, x._3, x._4, x._5, x._6, x._7)  }//.toDF()
    inp1.printSchema();
   inp1.show()
  /*inp1.registerTempTable("test")
   /*sqlContext1.sql("CREATE TABLE new_test row format delimited fields terminated by ',' " +
      "STORED AS Textfile AS select id,name,age from test " ) */
    sqlContext1.sql("CREATE TABLE new_test " +
      " AS select id,name,age from test " )

    sqlContext1.sql("CREATE TABLE new_test_target " +
      " AS select id,name,age from test " )  */

    sqlContext1.sql("DROP TABLE sample")
    sqlContext1.sql("CREATE TABLE  if not exists sample(id1 int, name String,val  String) row format delimited fields terminated by ','")
      sqlContext1.sql("LOAD DATA LOCAL INPATH 'C:/Users/abhishek/workspace/spark_QA/test1.txt' OVERWRITE INTO TABLE  sample")

    val m1 = Map[String, String]("id"->"id", "name"->"name", "age"->"age")
    val pk = Array("id")
    val src = sqlContext1.sql("Select * from new_test")
    val tgt = sqlContext1.sql("Select * from sample")
    
    //below is to convert to SQL context datframes else was facing some issues while joining later
    val tgt_rdd = tgt.rdd

    val tgt_new =  sqlContext.createDataFrame(tgt_rdd,schema_tgt )
    val src_rdd = src.rdd
    val src_new =  sqlContext.createDataFrame(tgt_rdd,schema )
    val qry_comp = new compare()
    src.show()
    tgt.show()
    val out= qry_comp.comparequery(m1,"2",src,tgt,sqlContext1)
    out.show()
   // out.registerTempTable("table")
    //val res = sqlContext.sql("select group_concat(id,name) from table")
    //res.show()


    val tgt_feilds = Array("id")
    val src_tgt_feilds : Array[Tuple2[String,String]] = Array(("id","id1"))
    val compare_col : Array[Tuple2[String,String]] = Array(("name","name1"), ("age","age1"))


    val test1 = src_new.join( tgt_new ,src_tgt_feilds.map { x => src_new(x._1) === tgt_new(x._2) }.reduce(_ && _) , "inner"  )




  }

  def Stringtodatatype (x :String) : DataType = {
    x match {
      case "String" =>  StringType
      case "int" =>  IntegerType
      // case other => other
    }


  }

}
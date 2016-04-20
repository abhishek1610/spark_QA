import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
 * Created by abhishek on 2016-04-10.
 */
class compare  {

  def comparequery(S2T : Map[String,String] , key: String, src : DataFrame, tgt : DataFrame,sqlcontext1 : HiveContext ): DataFrame =
  {
    val m1 = S2T
    val src_key = key
    val tgt_key = m1 getOrElse (src_key, "no key")
    var join_qry = "inner join on " + src_key + " = " + tgt_key
    val cntr = m1.size
    var qry :String = " where "
    var i = 1
   for ((k,v) <- m1)
     {
       qry = qry+ " " + k +" != " + v
       if (i != cntr)
      qry = qry + " or "
     i=  i +1

     }
    val tgt_feilds = Array("id")
    val src_tgt_feilds : Array[Tuple2[String,String]] = Array(("id","id1"))
    val compare_col : Array[Tuple2[String,String]] = Array(("name","name1"), ("age","age1"))
  //return join_qry+qry
  val src1 = src.registerTempTable("src")
    val tgt1 = tgt.registerTempTable("tgt")
   // tgt.printSchema()
    //below is supported from Spark 1.3.1
//  val tgt1 = tgt.select(tgt_feilds.map {x => src(x).as(x+ "1")} :_*)

    //val out = src1.join( tgt1 ,src_tgt_feilds.map { x => src1(x._1) === tgt1(x._2) }.reduce(_ && _) , "inner"  )
    // .where(compare_col.map { x => src1(x._1) !== tgt1(x._2) }.reduce(_ || _))
    //  .select(src1("id"),tgt("id1"))
   // out
   val out1 = sqlcontext1.sql("select id , concat(id,tgt.name) from src inner join tgt on src.id=tgt.id1")

out1

  }

}

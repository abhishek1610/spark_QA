import org.apache.spark.sql.DataFrame

/**
 * Created by abhishek on 2016-04-10.
 */
class compare extends Serializable {

  def comparequery(S2T : Map[String,String] , key: String, src : DataFrame, tgt : DataFrame ): DataFrame =
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

    val src_tgt_feilds : Array[Tuple2[String,String]] = Array(("id","id"),("name","name"))
  //return join_qry+qry
  val src1 = src.as("A")
    val tgt1 = tgt.as("B")

    //src_tgt_feilds.map {x => src1(x._1) === tgt1(x._2)}.reduce(_ && _)
   val out = src1.join( tgt1 ,src1("age") === tgt1("age") , "inner"  ).select(src("id"))
    out

  }

}

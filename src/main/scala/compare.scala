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
    val tgt_feilds = Array("id" ,"name","age")
    val src_tgt_feilds : Array[Tuple2[String,String]] = Array(("id","id_1"),("name","name_1"))
  //return join_qry+qry
  val src1 = src.as("A")
    val tgt1 = tgt.as("B")

  //Below hack to make the join work we have to renme tgt cols just in case tgt and source column matches
    //not required from spark 1.3
    val tgt1_new = tgt1.select(tgt_feilds.map{x => tgt1(x).as(x+"_1")} : _*)
    //src_tgt_feilds.map {x => src1(x._1) === tgt1(x._2)}.reduce(_ && _)
   val out = src1.join( tgt1_new ,src_tgt_feilds.map { x => src1(x._1) === tgt1_new(x._2) }.reduce(_ && _) , "inner"  ).select(src("id"))
    out

  }

}

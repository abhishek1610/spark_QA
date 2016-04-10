/**
 * Created by abhishek on 2016-04-10.
 */
class compare {

  def comparequery(S2T : Map[String,String] , key: String): String =
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
  return join_qry+qry
  }

}

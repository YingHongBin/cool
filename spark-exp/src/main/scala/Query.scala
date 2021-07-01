import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Query {
  val q1a = """SELECT  pid         AS pid
                       ,MIN(date)  AS date
               FROM    data 
               WHERE   EVENT = 'observation start' 
               GROUP BY pid"""

  val q1b = """SELECT  pid         AS pid
                       ,MIN(date)  AS date
               FROM    data 
               WHERE   EVENt = 'operating' 
               GROUP BY pid"""

  val q1c = """SELECT  view2.pid   AS pid
                       ,view2.date AS date
               FROM    view1
               JOIN    view2 ON view1.pid = view2.pid
               WHERE   view1.date < view2.date"""

  val q1d = """SELECT  pid         AS pid
                       ,date       AS date
               FROM    data
               WHERE   EVENT = 'effective'"""

  val q1e = """SELECT  data.institution           AS institution
                       ,view3.pid                  AS pid
                       ,view4.date - view3.date    AS age
               FROM    view3
               JOIN    view4 ON view3.pid = view4.pid
               JOIN    data ON view3.pid = data.pid
               WHERE   view3.date < view4.date"""

  val q1f = """SELECT  institution AS cohort
                       ,age        AS age
                       ,COUNt(*)   AS retention
               FROM    view5
               WHERE   institution in ('NUS', 'NHGP')
               GROUP BY institution, age"""

  val q2 = """select N_NAME, R_NAME, count(*), SUM(O_TOTALPRICE)
              from data
              where O_ORDERPRIORITY = '2-HIGH' and R_NAME = 'EUROPE' and 
                    O_ORDERDATE between DATE('1993-1-1') and DATE('1994-1-1')
              GROUP BY N_NAME, R_NAME"""

  def main (args: Array[String]) {
    if (args(0) == "q1") {
      run_q1()
    } else if (args(0) == "q2") {
      run_q2()
    }
  }

  def run_q1 () {
    val st = System.nanoTime()
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.json("/sparks/q1/*.json")
    df.createOrReplaceTempView("data")
 
    spark.sql(q1a).createOrReplaceTempView("view1")
    spark.sql(q1b).createOrReplaceTempView("view2")
    spark.sql(q1c).createOrReplaceTempView("view3")
    spark.sql(q1d).createOrReplaceTempView("view4")
    spark.sql(q1e).createOrReplaceTempView("view5")
    spark.sql(q1f)
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("/sparks/q1/results")
    val ed = System.nanoTime()
    println((ed - st).toDouble/1000000000)
    spark.stop()
  }

  def run_q2 () {
    val st = System.nanoTime()
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.json("/sparks/q2/*.json")
    df.createOrReplaceTempView("data")
    
    spark.sql(q2)
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("/sparks/q2/results")
    val ed = System.nanoTime()
    println((ed - st).toDouble/1000000000)
    spark.stop()
  }
}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
def transformRow(x:String) : Array[String] = {
      var a = x.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*\"$)",-1)
      a(0) = a(0).replace("\"","")
      a(16) = a(16).replace("\"\"\"","\"\"")
      a
}
//1
//val data = sc.textFile("Project_dataset_bank-full.csv").map(x => x.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*\"$)",-1))
val data = sc.textFile("Project_dataset_bank-full.csv").map(x =>transformRow(x))

val header = data.first()
val filtered = data.filter(x => x(0)!= header(0))
val rdds = filtered.map(x => Row(x(0).toInt, x(1),x(2),x(3),x(4), x(5).toInt,x(6),x(7),x(8), x(9).toInt,x(10),x(11).toInt,x(12).toInt, x(13).toInt,x(14).toInt,x(15),x(16)  ))

val schema = StructType( List(StructField("age", IntegerType, true),StructField("job", StringType, true) ,StructField("marital", StringType, true),StructField("education", StringType, true) ,StructField("default", StringType, true),StructField("balance", IntegerType, true) ,StructField("housing", StringType, true) 	,StructField("loan", StringType, true) ,StructField("contact", StringType, true) ,StructField("day", IntegerType, true) ,StructField("month", StringType, true) ,StructField("duration", IntegerType, true) ,StructField("campaign", IntegerType, true) ,StructField("pdays", IntegerType, true) ,StructField("previous", IntegerType, true) ,StructField("poutcome", StringType, true) ,StructField("y", StringType, true)) )

val df = sqlContext.createDataFrame(rdds, schema)
//2
val success_rate = (df.filter($"y" === "\"\"yes\"\"").count).toDouble / (df.count).toDouble
//2a
val failure_rate = (df.filter($"y" === "\"\"no\"\"").count).toDouble / (df.count).toDouble

//3
df.select(max($"age"), min($"age") , mean($"age")).show

df.createOrReplaceTempView("df")
//4
sqlContext.sql("select avg(balance), percentile(balance, 0.50) from df").show
//5
df.groupBy("age","y").count.show()
df.groupBy("y").agg(avg($"age")).show
//6
df.groupBy("y").agg(count($"marital")).show
df.groupBy("marital","y").count.show

sqlContext.sql("select percentile(age, 0.50) from df").show
sqlContext.sql("select max(age), min(age), avg(age) , percentile(age, 0.50) from df").show

val df_new = df.withColumn("age_cat", when($"age" < 25 , "young").otherwise( when($"age" > 60 , "old").otherwise("mid_age")  ))
df_new.groupBy("age_cat","y").count.show()
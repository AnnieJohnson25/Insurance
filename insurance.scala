package org.anniejohnson.insurance
import org.apache.spark._;
import org.apache.spark.sql._;
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._ // Importing package to use break condition
object insurance {
  
case class insureClass(id1:String,id2:String,yr:String,stcd:String,srcnm:String,network:String,url:String)

def main(args:Array[String])
{
  val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Spark SQL learning")
  val sc=new SparkContext(sparkconf)
  val sqlc=new SQLContext(sc);
  import sqlc.implicits._;//for allowing creation of dataframes
  
  val sparksess=org.apache.spark.sql.SparkSession.builder().appName("This is a spark session application").master("local[*]").enableHiveSupport().getOrCreate()
  sparksess.sparkContext.setLogLevel("ERROR")
  
  
  //loading and filtering insuranceinfo1.csv as RDD
  val rdd1 = sparksess.sparkContext.textFile("hdfs://localhost:54310/user/hduser/hadoop/insuranceinfo1.csv")
  System.out.println("\nrdd1 created")
  
  System.out.println("\nThe header to be removed is:")
  val header1=rdd1.first
  System.out.println(header1)
  
  System.out.println("\nrddNew after removing the header: ")
  val rddNew1 = rdd1.filter(l=>l!=header1)
  System.out.println("Count and few rows of rddNew1")
  System.out.println(rddNew1.count+"\n")
  val x1=rddNew1.take(10)  
  println(x1.map(_.mkString("")).mkString("\n"))
  
  //loading and filtering insuranceinfo2.csv as RDD
  val rdd2 = sparksess.sparkContext.textFile("hdfs://localhost:54310/user/hduser/hadoop/insuranceinfo2.csv")
  System.out.println("\nrdd2 created")
  
  System.out.println("\nThe header to be removed is:")
  val header2=rdd2.first
  System.out.println(header2)
  
  System.out.println("\nrddNew after removing the header: ")
  val rddNew2 = rdd2.filter(l=>l!=header1)
  System.out.println("Count and few rows of rddNew2")
  System.out.println(rddNew2.count+"\n")
  val x2=rddNew2.take(10)  
  println(x2.map(_.mkString("")).mkString("\n"))

  //merging both rddNew1 and rddNew2
  //10)Merge both the header removed RDDs rddNew1 and rddNew2 to insuredatamerged
  val insuredatamerged =rddNew1.union(rddNew2)
  
  //11)cache the merged data using any persistence level
  //display only first few rows
  insuredatamerged.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
  System.out.println("\ninsuredatamerged has been persisted")
  System.out.println("\nTaking first 10 rows of merged data")
  val x3=insuredatamerged.take(10)  
  println(x3.map(_.mkString("")).mkString("\n"))
  
  //12)Display the count of whole data sets
  System.out.println("\nCount of the merged data:")
  System.out.println(insuredatamerged.count)

  //13)Remove duplicates from this merged RDD, increase the number of partitions to 8 and name it as insuredatarepart
  val uniqueInsuredatamerged = insuredatamerged.distinct
  System.out.println("\nCount of unique elements in the merged data:")
  System.out.println(uniqueInsuredatamerged.count)

  val insuredatarepart=uniqueInsuredatamerged.repartition(8)
  System.out.println("\nNumber of partitions has been increased to:")
  System.out.println(insuredatarepart.partitions.size)

  //14)Load the file3 (custs_states.csv) from the HDFS location, using textfile API in an RDD custstates, this file contains 2 type of data one with 5 columns contains customer master info and other data with statecode and description of 2 columns.
  val custstates = sparksess.sparkContext.textFile("hdfs://localhost:54310/user/hduser/hadoop/custs_states.csv")
  System.out.println("\nCount of custstates:")
  System.out.println(custstates.count)
  
  //15)Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with 5 columns data and second RDD namely statesfilter should be only loaded with 2 columns data.
  val custfilter= custstates.map(_.split(",")).filter(l => l.length>2)
  val statesfilter=custstates.map(_.split(",")).filter(l => l.length==2)
  System.out.println("\nCount of custfilter:")
  System.out.println(custfilter.count)
  System.out.println("\nCount of statesfilter:")
  System.out.println(statesfilter.count)
  
  //16)Create a function namely masking with 1 parameter of string type and return also string type, using this function you have to mask the input param with (a as x, b as y, c as z, d as l, m as n, n as o, and so on and all numerical values with 1 or some other values as you wish.
  
  def maskingudf(word:String):String=
{
  //LOGIC: if word has a it is replaced by the next element in array m that is b, and 9 in word, is replaced by a 
  var m=Array('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','0','1','2','3','4','5','6','7','8','9')
  var y=""
  for(i<-0 until word.length)
  {
    breakable{
    for(j<-0 until m.length)
    {
      if (word.charAt(i)=='9')
      {
        y=y+"a"
        break
      }
      else if(word.charAt(i)==m(j))
      {
        y=y+m(j+1)
        break
      }
      else
      {
        y=y+word.charAt(i)
        break
      }
    }
    }
  }
  
  return y
}
  System.out.println("\nTesting maskingudf function with abcdAZ29")
  System.out.println(maskingudf("abcdAZ29"))
  

  //17)Register the above udf in the name of maskingudf using spark udf registration.

  sparksess.udf.register("maskingudf",maskingudf _)
  
  //18)Convert the insuredatarepart RDD to DF after applying schema with columns such as ("id1","id2","yr","stcd","srcnm","network","url") and register as a temporary table.

  
  val insure2 = insuredatarepart. map(_.split(",")).map(x=>x(0)+","+x(1)+","+x(2)+","+x(3)+","+x(4)+","+x(5)+","+x(6))
  val insuredatarepart2 = insure2.map(x=>x.split(",")).filter(l => l.length==7).map(p => insureClass(p(0), p(1), p(2), p(3), p(4), p(5), p(6)))
  System.out.println("FILTERED UNECESSARY COLUMNS WITH EMPTY LAST 2 COLUMNs")
  System.out.println("The number of rows in insuredatapart2 is:")
  System.out.println(insuredatarepart2.count)

  //We can split by comma but then some of the rows have last 2 columns as empty.
  // so we create insure which contains all the rows but omits the last 3 columns
  
  
  val insuredatarepartDF = sparksess.createDataFrame(insuredatarepart2)
  
  //We can split by comma but then some of the rows have last 2 columns as empty.
  
  
  //val insuredatarepartDF=insuredatarepart.toDF can also be done
  System.out.println("\nData frame created")
  insuredatarepartDF.distinct.show
  
  System.out.println("\ndf created")

  insuredatarepartDF.createOrReplaceTempView("insuredataTABLE")

  System.out.println("\nTemporary view called insuredataTABLE created")

  //19)Select id1, id2, concatenation of stcd and network after converting into upper case and apply masking on the url column.

  val SQL = sparksess.sql("SELECT id1,id2, CONCAT(UPPER(stcd),UPPER(srcnm)) as concatenatedOutput, maskingudf(url) as maskedURL FROM insuredataTABLE")
  SQL.show(10)

  //20)Store the above selected Dataframe in JSON and Parquet formats in a HDFS location as a single file.
  
  SQL.write.mode("overwrite").json("file:/home/hduser/hackathon/hack2/SQL.json");
  SQL.write.mode("overwrite").parquet("file:/home/hduser/hackathon/hack2/SQL2.parquet");

}

}
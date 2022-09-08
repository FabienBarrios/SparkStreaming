import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object stream extends App{
    val sparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("streaming")
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val tweetdfa = sparkSession
        .read
        .format("csv")
        .option("inferSchema","true")
        .option("header","true")
        .load("/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/Dataset/*.csv")

    val retailSchema = tweetdfa.schema

    val tweetdf = sparkSession.readStream
        .schema(retailSchema)
        .option("maxFilesPerTrigger",1)
        .format("csv")
        .option("header","true")
        .load("/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/Dataset/*.csv")

    println("isStreaming : " + tweetdf.isStreaming.toString)

    ////////////////////////////////////////////////AGG//////////////////////////////////////////////////
    val  allAvg = tweetdf.agg(sum("like") as "Total like", sum("retweet") as "Total retweet", max("like") as "Max de Like",  max("retweet") as "Max de retweet")
    val deviseCount = tweetdf.groupBy("source").agg(count("source") as "Nombre de tweet").sort(col("Nombre de tweet").desc)
    val sum_like_per_hashtag = tweetdf.groupBy("hashtags0").agg(count("like") as "Nombre de likes", mean("like") as "Moyenne de like").sort(col("Nombre de likes").desc)
    val sum_like_per_hashtag1 = tweetdf.groupBy("hashtags1").agg(count("like") as "Nombre de likes", mean("like") as "Moyenne de like").sort(col("Nombre de likes").desc)
    val sum_like_per_hashtag2 = tweetdf.groupBy("hashtags2").agg(count("like") as "Nombre de likes", mean("like") as "Moyenne de like").sort(col("Nombre de likes").desc)
    val mean_like_per_post = tweetdf.agg(mean("like") as "Like Moyen par post")
    val mean_retweet_per_post = tweetdf.agg(mean("retweet") as "Retweet Moyen par post")

    ////////////////////////////////////////////////Filter//////////////////////////////////////////////////
    val hastag0 = tweetdf.filter(col("like") > 0)

    val avg_like_per_hashtag_filter = hastag0.groupBy("hashtags0").agg(count("like") as "Nombre de likes", mean("like") as "Moyenne de like").sort(col("Nombre de likes").desc)

    ////////////////////////////////////////////////WaterMark//////////////////////////////////////////////////////

    //val  allAvg_watermark = tweetdf.withWatermark("timestamp", "10 minutes").groupBy(window(, "10 minutes")).agg(count("like") as "Nombre de likes", mean("like") as "Moyenne de like").sort(col("Nombre de likes").desc)

    ////////////////////////////////////////////////WriteStream//////////////////////////////////////////////////

    mean_like_per_post.writeStream.format("memory").queryName("mean_like").outputMode("complete").start()

    mean_retweet_per_post.writeStream.format("memory").queryName("mean_retweet").outputMode("complete").start()

    allAvg.writeStream.format("memory").queryName("total_likes").outputMode("complete").start()

    deviseCount.writeStream.format("memory").queryName("DEVICE").outputMode("complete").start()

    sum_like_per_hashtag.writeStream.format("memory").queryName("lakers").outputMode("complete").start()

    sum_like_per_hashtag1.writeStream.format("memory").queryName("lakers1").outputMode("complete").start()

    sum_like_per_hashtag2.writeStream.format("memory").queryName("lakers2").outputMode("complete").start()

    // allAvg_watermark.writeStream.format("memory").queryName("watermark").outputMode("complete").start()


    for (_ <- 1 to 50 ) {
        //Total
        sparkSession.sql("select * from total_likes".stripMargin).show(false)

        //Watermark
        //sparkSession.sql("select * from watermark".stripMargin).show(false)

        // Nombre de tweet par device
        sparkSession.sql(("select * from DEVICE").stripMargin).show(false)

        // like per hashtag0
        sparkSession.sql("select * from lakers".stripMargin).show(false)

        // like per hashtag1
        sparkSession.sql("select * from lakers1".stripMargin).show(false)

        // like per hashtag2
        sparkSession.sql("select * from lakers2".stripMargin).show(false)

        // Moyenne de like par post
        sparkSession.sql("select * from mean_like".stripMargin).show(false)

        //Moyenne de retweet par post
        sparkSession.sql("select * from mean_retweet".stripMargin).show(false)

        Thread.sleep(1000)
    }

    tweetdfa.printSchema()
    tweetdfa.show(false)
}
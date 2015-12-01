package sci2s.sparkfingerprint

/**
 * @author daniel
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io.Text
import org.apache.spark.HashPartitioner

import sci2s.mrfingerprint.LSCylinderArray
import sci2s.mrfingerprint.GenericLSWrapper
import sci2s.mrfingerprint.LocalStructureCylinder
import sci2s.mrfingerprint.LocalStructure
import sci2s.mrfingerprint.PartialScoreLSS

import sci2s.mrfingerprint.LSCylinderArray


object SparkMatcherNB {  
  val usage = """
    Usage: SparkMatcherNB
      [--matcher m]
      [--partial-score ps]
      [--template-file path]
      [--info-file path]
      [--map-file path]
      [--output-dir path]
      [--num-partitions num]
  """
  
  type OptionMap = Map[Symbol, Any]
  
  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    
    list match {
      case Nil => map
      case "--matcher" :: value :: tail =>
                             nextOption(map ++ Map('matcher -> value), tail)
      case "--partial-score" :: value :: tail =>
                             nextOption(map ++ Map('partialscore -> value), tail)
      case "--template-file" :: value :: tail =>
                             nextOption(map ++ Map('templatefile -> value), tail)
      case "--info-file" :: value :: tail =>
                             nextOption(map ++ Map('infofile -> value), tail)
      case "--map-file" :: value :: tail =>
                             nextOption(map ++ Map('mapfile -> value), tail)
      case "--output-dir" :: value :: tail =>
                             nextOption(map ++ Map('outputdir -> value), tail)
      case "--num-partitions" :: value :: tail =>
                             nextOption(map ++ Map('numpartitions -> value.toInt), tail)
      case option :: tail => println("Unknown option "+option) 
                             System.exit(1)
                             map
    }
  }

	def main(args: Array[String]): Unit = {
   
      if (args.length == 0) {
        println(usage)
        System.exit(-1)
      }
      
      val options = nextOption(Map(), args.toList)
      
      println(options)
      
			val initialtime = System.currentTimeMillis
      
      val DEBUG = false

			// TODO the number of processes and/or partitions may be passed as a parameter
			val conf = new SparkConf().setMaster("local[1]").setAppName("Generic Matcher")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.hadoop.cloneConf", "true")

			conf.registerKryoClasses(Array(
          classOf[sci2s.mrfingerprint.LSCylinderArray],
					classOf[org.apache.hadoop.io.Text],classOf[sci2s.mrfingerprint.GenericLSWrapper],
          classOf[sci2s.mrfingerprint.LocalStructureCylinder],
          classOf[sci2s.mrfingerprint.LocalStructureJiang],
          classOf[Array [scala.Tuple2[Any,Any]]],
          classOf[org.apache.hadoop.mapred.JobConf]))

			val sc = new SparkContext(conf)

      // Parameters
      val matcher = options.get('matcher).get.toString
      val partialScore = options.get('partialscore).get.toString
      val templateFile = options.get('templatefile).get.toString
      val outputDir = options.getOrElse('outputdir, "output_matching_" + partialScore + "_" + System.currentTimeMillis).toString
      val mapFileName = options.get('mapfile).get.toString
      val infoFileName = options.get('infofile).get.toString
      val numPartitions = options.get('numpartitions).getOrElse(10).asInstanceOf[Int]


			// Read template database
			val templateLS = sc.sequenceFile[String, LocalStructureCylinder](templateFile).partitionBy(new HashPartitioner(numPartitions))
          .mapValues(new LocalStructureCylinder(_)).persist

			// FOR DEBUGGING
      if(DEBUG) {
		    println("Number of template LS: %s".format(templateLS.count()))
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }

			// Read input fingerprint(s)
	    val inputLSRDD = sc.sequenceFile[String, LSCylinderArray](mapFileName)
          .flatMapValues(x => x.get()).persist

			// FOR DEBUGGING
      if(DEBUG) {
		    println("Number of input LS: %s".format(inputLSRDD.count()))
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }

      /* This stage is critical for efficiency.
      In the best scenario, templatenum
      */
	    val cart = templateLS.cartesian(inputLSRDD).map{case((k1,v1),(k2,v2)) => ((k1,k2),(v1,v2))}
			 
      
      // Compute the local matches
      val localmatches = cart.mapValues(lspair => lspair._1.similarity(lspair._2.asInstanceOf[LocalStructure]))
      .filter({case(k,v) => v > 0}).persist
      
      // Apply consolidation
      // TODO: the consolidation is passed as parameter
      // TODO: re-use classes
      val inputnumls = sc.parallelize(inputLSRDD.countByKey().toSeq)
      val templatenumls = sc.parallelize(templateLS.countByKey().toSeq)
      
      val npbykey = templatenumls.cartesian(inputnumls).map{case((k1,v1),(k2,v2)) => ((k1,k2),(v1,v2))}
      .mapValues(numls => PartialScoreLSS.computeNP(numls._1.toInt, numls._2.toInt)).persist
            
      // Apply global matching
      val scores = localmatches.join(npbykey).aggregateByKey(new Array[Double](100))(
            (acc, value) => topNs(acc :+ value._1, value._2),
            (acc1, acc2) => topNs(acc1 ++ acc2, acc1.size))
          .mapValues(bestls => bestls.sum / bestls.size)

      if(DEBUG) {
        println("Number of local matches: %s".format(localmatches.count()))
        println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
 
        scores.map(item => item.swap).sortByKey().map(item => item.swap).foreach(println(_))
      }


      // Sort by score and write output
      scores.map(item => item.swap).sortByKey().map(item => item.swap).saveAsTextFile(outputDir)
      
      // Print time
      println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
	}

  
  def topNs(xs: TraversableOnce[Double], n: Int) = {
    var ss = Array[Double]()

    for(e <- xs) {
      if (ss.size < n) {
        ss = ss :+ e
      } else if(e > ss.head) {
        ss = (ss :+ e).sorted.tail
      }                    
    }
    
    ss
  }
}
package sci2s.sparkfingerprint

/**
 * @author daniel
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io.Text
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import sci2s.mrfingerprint.LSCylinderArray
import sci2s.mrfingerprint.GenericLSWrapper
import sci2s.mrfingerprint.LocalStructureCylinder
import sci2s.mrfingerprint.LocalStructure
import sci2s.mrfingerprint.PartialScoreLSS
import sci2s.mrfingerprint.PartialScoreLSSR
import sci2s.mrfingerprint.PartialScore

class RDD2Partitioner(partitions: Int) extends HashPartitioner(partitions) {

  override def getPartition(key: Any): Int = key match {
    case (k1,k2) => super.getPartition(k1)
    case _ => super.getPartition(key)
  }

}


object SparkMatcher {
  
  val usage = """
    Usage: SparkMatcher
      [--matcher m]
      [--partial-score ps]
      [--template-file path]
      [--info-file path]
      [--map-file path]
      [--output-dir path]
      [--num-partitions num]
  """
  
  type OptionMap = Map[Symbol, Any]
  
  var DEBUG = false
  
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
      case "--debug" :: tail =>
                             DEBUG = true
                             nextOption(map, tail)
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

			// Parameters
      val matcher = options.get('matcher).get.toString
      val partialScore = options.get('partialscore).get.toString
      val templateFile = options.get('templatefile).get.toString
      val outputDir = options.getOrElse('outputdir, "output_spark_" + partialScore + "_").toString + System.currentTimeMillis
      val mapFileName = options.get('mapfile).get.toString
      val infoFileName = options.get('infofile).get.toString
      val numPartitions = options.get('numpartitions).getOrElse(10).asInstanceOf[Int]

      // TODO the number of processes may be passed as a parameter
      val conf = new SparkConf().setAppName("Spark Matcher " + matcher + " " + templateFile.substring(templateFile.lastIndexOf('/')))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.hadoop.cloneConf", "true")
      
      // Register classes for serialization
      conf.registerKryoClasses(Array(
          classOf[sci2s.mrfingerprint.LSCylinderArray],
          classOf[org.apache.hadoop.io.Text],classOf[sci2s.mrfingerprint.GenericLSWrapper],
          classOf[sci2s.mrfingerprint.LocalStructureCylinder],
          classOf[sci2s.mrfingerprint.LocalStructureJiang],
          classOf[Array [scala.Tuple2[Any,Any]]],
          classOf[org.apache.hadoop.mapred.JobConf]))

      // Set SparkContext
      val sc = new SparkContext(conf)

			// Read template database
			val templateLS = sc.sequenceFile[String, LocalStructureCylinder](templateFile).partitionBy(new HashPartitioner(numPartitions))
          .mapValues(new LocalStructureCylinder(_))

			// FOR DEBUGGING
      if(DEBUG) {
		    println("Number of template LS: %s".format(templateLS.count()))
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }

			// Read input fingerprint(s)
	    val inputLSRDD = sc.sequenceFile[String, LSCylinderArray](mapFileName).mapValues(new LSCylinderArray(_))
      
      // Broadcast the input fingerprint(s)
      val inputLS = sc.broadcast(inputLSRDD.collect())

			// FOR DEBUGGING
      if(DEBUG) {
        println("Number of input LS: %s".format(inputLSRDD.count))
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }
      
      // Compute the partial scores for each template ls
      val partialscores = computeScores2(partialScore, templateLS, inputLS)
      
      println("Partial scores computed. Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))

      if(DEBUG) {
        println("Number of scores: %s".format(partialscores.count()))
        println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
        partialscores.sortBy({case (k,v) => v}).foreach(println(_))
      }
      
      // Sort by score and write output
      partialscores.sortBy({case (k,v) => v}).saveAsTextFile(outputDir)
      
      // Print time
      println("Total time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
	}

  def computeScores1[PSType <: PartialScore](partialscore : String, templateLS : RDD[(String, LocalStructureCylinder)],
      inputLS : Broadcast[Array[(String, LSCylinderArray)]]) = {
      
    val partialscores = templateLS.flatMap({ case (tid, ls) =>

        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
        
        // For each input fingerprint, compute the partial score with the template LS "ls"
        inputLS.value.map { ils => 
          val ilsarray = ils._2.get().map(elem => elem.asInstanceOf[LocalStructure])
          val newps = constructor.newInstance(ls, ilsarray).asInstanceOf[PSType]
          ((tid, ils._1), (newps, ilsarray.size))
        }
      })
        
      // Now we reduce all the partial scores of the same template fingerprint
      .reduceByKey((v1, v2) => (v1._1.aggregateSinglePS(v2._1).asInstanceOf[PSType], v1._2))
      
      // Compute the final score using the np value
      .mapValues { case (ps, inputsize) => ps.computeScore(inputsize) }
    
    partialscores
  }

  def computeScores2[PSType <: PartialScore](partialscore : String, templateLS : RDD[(String, LocalStructureCylinder)],
      inputLS : Broadcast[Array[(String, LSCylinderArray)]]) = {
      
    // First, compute the partial scores of each template LS with each input fingerprint.
    val scores = templateLS.flatMap({ case (tid, ls) =>

        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
    
        // For each input fingerprint, compute the partial score with the template LS "ls"
        inputLS.value.map { ils => 
          val ilsarray = ils._2.get().map(elem => elem.asInstanceOf[LocalStructure])
          val newps = constructor.newInstance(ls, ilsarray).asInstanceOf[PSType]
          ((tid, ils._1), (newps, ilsarray.size))
        }
      }).partitionBy(new RDD2Partitioner(templateLS.partitioner.get.numPartitions))
        
      // Now we reduce all the partial scores of the same pair of fingerprints
      .reduceByKey((v1, v2) => (v1._1.aggregateSinglePS(v2._1).asInstanceOf[PSType], v1._2))
      
      // Compute the final score using the np value
      .mapValues { case (ps, inputsize) => ps.computeScore(inputsize) }
    
    scores
  }
  
}
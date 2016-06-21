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
import org.apache.spark.util.SizeEstimator

import scala.collection.JavaConverters._
import scala.collection.Iterable

import sci2s.mrfingerprint.LSCylinderArray
import sci2s.mrfingerprint.LocalStructureCylinder
import sci2s.mrfingerprint.LocalStructure
import sci2s.mrfingerprint.PartialScoreLSSThreshold
import sci2s.mrfingerprint.PartialScore


object SparkMatcherLSSThreshold {
  
  val usage = """
    Usage: SparkMatcherJiangLocal
      [--template-file path]
      [--info-file path]
      [--map-file path]
      [--output-dir path]
      [--num-partitions num]
      [--debug]
  """
  
  type OptionMap = Map[Symbol, Any]
  
  var DEBUG = false
  
  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    
    list match {
      case Nil => map
      case "--template-file" :: value :: tail =>
                             nextOption(map ++ Map('templatefile -> value), tail)
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
  
  def printSize(st : String, elem : AnyRef) : Unit = {
    println(st + SizeEstimator.estimate(elem)/Math.pow(1024, 2) + " MB")
  }
  

	def main(args: Array[String]): Unit = {
   
      if (args.length == 0) {
        println(usage)
        System.exit(-1)
      }
      
      val options = nextOption(Map(), args.toList)
      
      println(options)

			// Parameters
      val templateFile = options.get('templatefile).get.toString
      val outputDir = options.getOrElse('outputdir, "output_spark_jiang_").toString + System.currentTimeMillis
      val mapFileName = options.get('mapfile).get.toString
      val numPartitions = options.get('numpartitions).getOrElse(10).asInstanceOf[Int]

      // TODO the number of processes may be passed as a parameter
      val conf = new SparkConf().setAppName("SparkMatcherJiang " + templateFile.substring(templateFile.lastIndexOf('/')))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.hadoop.cloneConf", "true").setMaster("local[*]")
      
      // Register classes for serialization
      conf.registerKryoClasses(Array(
          classOf[sci2s.mrfingerprint.LSCylinderArray],
          classOf[org.apache.hadoop.io.Text],
          classOf[sci2s.mrfingerprint.LocalStructureCylinder],
          classOf[org.apache.hadoop.mapred.JobConf]))

      // Set SparkContext
      val sc = new SparkContext(conf)

      val initialtime = System.currentTimeMillis

			// Read template database
			val templateLS = sc.sequenceFile[String, LocalStructureCylinder](templateFile) //.partitionBy(new HashPartitioner(numPartitions))
          .mapValues(new LocalStructureCylinder(_))

			// FOR DEBUGGING
      if(DEBUG) {
		    println("Number of template LS: %s".format(templateLS.count()))
        
        printSize("Template size: ", templateLS.collect())
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }

			// Read input fingerprint(s)
	    val inputLSRDD = sc.sequenceFile[String, LSCylinderArray](mapFileName)
        .mapValues(new LSCylinderArray(_).get().map(_.asInstanceOf[LocalStructureCylinder]))
      
      // Broadcast the input fingerprint(s)
      val inputLS = sc.broadcast(inputLSRDD.collect())

			// FOR DEBUGGING
      if(DEBUG) {
        println("Number of input LS: %s".format(inputLSRDD.count))
        printSize("LS size: ", inputLSRDD.collect())
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }
      
      // Compute the partial scores for each template ls
      val partialscores = computeScores(templateLS, inputLS)
      
      if(DEBUG) {
        println("Number of scores: %s".format(partialscores.count()))
        println("Partial scores computed. Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
        printSize("Partial score size: ", partialscores.collect())
        println("\tPartial score sample: " + partialscores.first)
        println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
//        partialscores.sortBy({case (k,v) => v}).foreach(println(_))
      }
      
      // Sort by score and write output
//      partialscores.sortBy({case (k,v) => v}).saveAsTextFile(outputDir)
      partialscores.saveAsTextFile(outputDir)
      
      // Print time
      println("Total time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
	}

  def computeScores(
      templateLS : RDD[(String, LocalStructureCylinder)],
      inputLS : Broadcast[Array[(String, Array[LocalStructureCylinder])]]) : RDD[(String, (String, Float))] = {
      
    // First, compute the partial scores of each template LS with each input fingerprint.
    val scores = templateLS.groupByKey().flatMapValues({ tlsarray =>
    
        // For each input fingerprint, compute the partial score with tid
        inputLS.value.map { ils =>

          // For each template LS, compute the partial score with the input fingerprint ilsarray
          val score = tlsarray.map ({ ls =>
            new PartialScoreLSSThreshold(ls, ils._2.asInstanceOf[Array[LocalStructure]])
            }).filter(! _.isEmpty).reduce(_.aggregateSinglePS(_).asInstanceOf[PartialScoreLSSThreshold]).computeScore()
            
          (ils._1, score)
        }
      })
      
      if(DEBUG) {
        val tmp = scores.collect()
        printSize("Partitioned partial scores size: ", tmp)
        println("\tPartitioned partial score number: " + tmp.size)
        println("\tPartitioned partial score sample: " + tmp(0))
      }
      
      scores
  }
  
}
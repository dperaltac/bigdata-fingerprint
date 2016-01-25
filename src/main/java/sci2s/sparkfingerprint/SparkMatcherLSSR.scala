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

import scala.collection.Iterable

import sci2s.mrfingerprint.LSCylinderArray
//import sci2s.mrfingerprint.GenericLSWrapper
import sci2s.mrfingerprint.LocalStructureCylinder
import sci2s.mrfingerprint.LocalStructure
import sci2s.mrfingerprint.PartialScoreLSSR
import sci2s.mrfingerprint.PartialScore


object SparkMatcherLSSR {

//  class RDD2Partitioner(partitions: Int) extends HashPartitioner(partitions) {
//  
//    override def getPartition(key: Any): Int = key match {
//      case (k1,k2) => super.getPartition(k1)
//      case _ => super.getPartition(key)
//    }
//  
//  }
  
  val usage = """
    Usage: SparkMatcherLSSR
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

			// Read template database
			val templateLS = sc.sequenceFile[String, LocalStructureCylinder](templateFile) //.partitionBy(new HashPartitioner(numPartitions))
          .mapValues(new LocalStructureCylinder(_))
          .filter({case (id, ls) => ls.isValid()})

			// Read input fingerprint(s)
	    val inputLSRDD = sc.sequenceFile[String, LSCylinderArray](mapFileName)
        .mapValues(new LSCylinderArray(_).get().map(elem => elem.asInstanceOf[LocalStructureCylinder]))
      
      // Broadcast the input fingerprint(s)
      val inputLS = sc.broadcast(inputLSRDD.collect())
      
      // Compute the partial scores for each template ls
      val partialscores = computeScores5(partialScore, templateLS, inputLS)
      
      println("Partial scores computed. Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      
      // Sort by score and write output
      partialscores.saveAsTextFile(outputDir)
      
      // Print time
      println("Total time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
	}


  def computeScores5(partialscore : String, templateLS : RDD[(String, LocalStructureCylinder)],
      inputLS : Broadcast[Array[(String, Array[LocalStructureCylinder])]]) : RDD[((String, String), Double)] = {
      
    // First, compute the partial scores of each template LS with each input fingerprint.
    templateLS.groupByKey().flatMap({ case (tid, tlsarray) =>

        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
    
        // For each input fingerprint, compute the partial score with tid
        inputLS.value.map { ils =>
          
          val minutiae = ils._2.map(_.getMinutia())

          // For each template LS, compute the partial score with the input fingerprint ilsarray
          val score = tlsarray.map ({ ls =>
            constructor.newInstance(ls, ils._2).asInstanceOf[PartialScore]
            }).reduce(_.aggregateSinglePS(_)).asInstanceOf[PartialScoreLSSR].computeScore(minutiae)
            
          ((tid, ils._1), score)
        }
      })
  }
  
}
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

import scala.collection.JavaConversions.IterableWrapper
import scala.collection.JavaConversions.SeqWrapper
import scala.collection.Iterable

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
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.hadoop.cloneConf", "true")//.setMaster("local")
      
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
			val templateLS = sc.sequenceFile[String, LocalStructureCylinder](templateFile) //.partitionBy(new HashPartitioner(numPartitions))
          .mapValues(new LocalStructureCylinder(_))
          .filter({case (id, ls) => ls.isValid()})

			// FOR DEBUGGING
      if(DEBUG) {
		    println("Number of template LS: %s".format(templateLS.count()))
        
        printSize("Template size: ", templateLS.collect())
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }

			// Read input fingerprint(s)
	    val inputLSRDD = sc.sequenceFile[String, LSCylinderArray](mapFileName)
        .mapValues(new LSCylinderArray(_).get().map(elem => elem.asInstanceOf[LocalStructureCylinder]))
      
      // Broadcast the input fingerprint(s)
      val inputLS = sc.broadcast(inputLSRDD.collect())

			// FOR DEBUGGING
      if(DEBUG) {
        println("Number of input LS: %s".format(inputLSRDD.count))
        printSize("LS size: ", inputLSRDD.collect())
		    println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
      }
      
      // Compute the partial scores for each template ls
      val partialscores = computeScores5(partialScore, templateLS, inputLS)
      
      println("Partial scores computed. Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))

      if(DEBUG) {
        println("Number of scores: %s".format(partialscores.count()))
        printSize("Partial score size: ", partialscores.collect())
        println("\tPartial score sample: " + partialscores.first)
        println("Time: %g".format((System.currentTimeMillis - initialtime)/1000.0))
//        partialscores.sortBy({case (k,v) => v}).foreach(println(_))
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
          val ilsarray = ils._2.get().map(elem => elem.asInstanceOf[LocalStructureCylinder]).filter(_.isValid())
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
      }).partitionBy(new RDD2Partitioner(templateLS.partitioner.get.numPartitions)).persist
      
      if(DEBUG) {
        val tmp = scores.collect()
        printSize("Partitioned partial scores size: ", tmp)
        println("\tPartitioned partial score number: " + tmp.size)
        println("\tPartitioned partial score sample: " + tmp(0))
      }
        
      // Now we reduce all the partial scores of the same pair of fingerprints
      scores.reduceByKey((v1, v2) => (v1._1.aggregateSinglePS(v2._1).asInstanceOf[PSType], v1._2))
      
      // Compute the final score using the np value
      .mapValues { case (ps, inputsize) => ps.computeScore(inputsize) }
    
    
  }
  
  def generatePSFromSingleLS[PSType <: PartialScore](partialscore : String, ls : LocalStructure, ilsarray : Array[LocalStructure]): PSType = {
    
        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
        
        constructor.newInstance(ls, ilsarray).asInstanceOf[PSType]
  }

//  def computeScores3[PSType <: PartialScore](partialscore : String, templateLS : RDD[(String, LocalStructureCylinder)],
//      inputLS : Broadcast[Array[(String, LSCylinderArray)]]) = {
//      
//    // First, compute the partial scores of each template LS with each input fingerprint.
//    // First, group the LS of the same template FP.
//    
//    
//    val scoresbyinput = inputLS.value.flatMap( { ils => 
//      
//      // Cast the Array of Input LS
//      val ilsarray = ils._2.get().map(elem => elem.asInstanceOf[LocalStructure])
//      
//      // Compute the score for every group of template LS from the same fingerprint
//      val pscores = templateLS.combineByKey(
//        (ls : LocalStructure) => generatePSFromSingleLS(partialscore, ls, ilsarray),
//        (ps : PartialScore, ls : LocalStructure) => ps.aggregateSinglePS(generatePSFromSingleLS(partialscore, ls, ilsarray)),
//        (ps1 : PartialScore, ps2 : PartialScore) => ps1.aggregateSinglePS(ps2)
//      )
//      
//      // Convert to the adequate format
//      .map({ case (tid,ps) => ((tid, ils._1), (ps, ilsarray.size)) })
//      
//      
//      // TODO the problem is that "inputLS.value" is not an RDD
//      pscores
//    } )
//    
//    val scores = templateLS.combineByKey(
//        
//        // Single template LS
//        (v) => generatePSFromSingleLS(v, inputLS),
//        
//        ((v1, v2) => (v1._1.aggregateSinglePS(v2._1).asInstanceOf[PSType], v1._2))
//
//        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
//        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
//        val defconstructor = PSClass.getConstructor()
//        var ps = defconstructor.newInstance().asInstanceOf[PSType]
////        val constructor = PSClass.getConstructor(classOf[java.util.List[LocalStructureCylinder]], classOf[Array[LocalStructureCylinder]])
//    
//        // For each input fingerprint, compute the partial score with the set of template LS
//        inputLS.value.map { ils => 
//          val ilsarray = ils._2.get().map(elem => elem.asInstanceOf[LocalStructureCylinder])
//          
//          tlsiterable.foreach({ls =>
//            val newps = constructor.newInstance(ls, ilsarray).asInstanceOf[PSType]
//            ps = ps.aggregateSinglePS(newps).asInstanceOf[PSType]
//          })
//        
//          ((tid, ils._1), (ps, ilsarray.size))
//        }
//      }).partitionBy(new RDD2Partitioner(templateLS.partitioner.get.numPartitions)).persist
//      
//      
//      printSize("Partitioned partial scores size: ", scores.collect())
//      println("\tPartitioned partial score sample: " + scores.first)
//        
//      // Now we reduce all the partial scores of the same pair of fingerprints
//      scores.reduceByKey((v1, v2) => (v1._1.aggregateSinglePS(v2._1).asInstanceOf[PSType], v1._2))
//      
//      // Compute the final score using the np value
//      .mapValues { case (ps, inputsize) => ps.computeScore(inputsize) }
//    
//    
//  }


  def computeScores4[PSType <: PartialScore](partialscore : String, templateLS : RDD[(String, LocalStructureCylinder)],
      inputLS : Broadcast[Array[(String, LSCylinderArray)]]) : RDD[((String, String), Double)] = {
      
    // First, compute the partial scores of each template LS with each input fingerprint.
    templateLS.groupByKey().flatMap({ case (tid, tlsarray) =>

        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
    
        tlsarray.flatMap ({ ls =>
          // For each input fingerprint, compute the partial score with the template LS "ls"
          inputLS.value.map { ils => 
            val ilsarray = ils._2.get().map(elem => elem.asInstanceOf[LocalStructure])
            val newps = constructor.newInstance(ls, ilsarray).asInstanceOf[PSType]
            ((tid, ils._1), (newps, ilsarray.size))
          }
        })
      }).partitionBy(new RDD2Partitioner(templateLS.partitioner.get.numPartitions))
      
//      if(DEBUG) {
//        val tmp = scores.collect()
//        printSize("Partitioned partial scores size: ", tmp)
//        println("\tPartitioned partial score number: " + tmp.size)
//        println("\tPartitioned partial score sample: " + tmp(0))
//      }
        
      // Now we reduce all the partial scores of the same pair of fingerprints
      .reduceByKey((v1, v2) => (v1._1.aggregateSinglePS(v2._1).asInstanceOf[PSType], v1._2))
      
      // Compute the final score using the np value
      .mapValues { case (ps, inputsize) => ps.computeScore(inputsize) }
  }
  
  




  def computeScores5[PSType <: PartialScore](partialscore : String, templateLS : RDD[(String, LocalStructureCylinder)],
      inputLS : Broadcast[Array[(String, Array[LocalStructureCylinder])]]) : RDD[((String, String), Double)] = {
      
    // First, compute the partial scores of each template LS with each input fingerprint.
    val scores = templateLS.groupByKey().flatMap({ case (tid, tlsarray) =>

        val PSClass = Class.forName("sci2s.mrfingerprint." + partialscore)
        val constructor = PSClass.getConstructor(classOf[LocalStructure], classOf[Array[LocalStructure]])
    
        // For each input fingerprint, compute the partial score with tid
        inputLS.value.map { ils =>

          // For each template LS, compute the partial score with the input fingerprint ilsarray
          val score = tlsarray.map ({ ls =>
            constructor.newInstance(ls, ils._2).asInstanceOf[PartialScore]
            }).reduce(_.aggregateSinglePS(_)).computeScore(ils._2.size)
            
          ((tid, ils._1), score)
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
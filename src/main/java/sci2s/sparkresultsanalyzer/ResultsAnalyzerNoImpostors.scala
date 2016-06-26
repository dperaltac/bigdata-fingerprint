package sci2s.sparkresultsanalyzer


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import sci2s.resultsanalyzer.FingerprintComparison

object ResultsAnalyzerNoImpostors {
  
  val usage = """
    Usage: ResultsAnalyzer
      [--results-file path]
      [--type {sfinge|nist}]
      [--origin {hadoop|spark}]
      [--debug]
  """
  
  type OptionMap = Map[Symbol, Any]
  
  var DEBUG = false
  
  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    
    list match {
      case Nil => map
      case "--results-file" :: value :: tail =>
                             nextOption(map ++ Map('resultsfile -> value), tail)
      case "--type" :: value :: tail =>
                             nextOption(map ++ Map('type -> value), tail)
      case "--origin" :: value :: tail =>
                             nextOption(map ++ Map('origin -> value), tail)
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
      
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      
      val options = nextOption(Map(), args.toList)
      
      println(options)
      
      // Set SparkContext
      val sc = new SparkContext(new SparkConf().setAppName("ResultsAnalyzer").setMaster("local[*]"))
      
      val resultsFile = options.get('resultsfile).get.toString
      val fptype = options.get('type).get.toString
      val origin = options.get('origin).get.toString
      
      val text = sc.textFile(resultsFile)
      
      var scores : RDD[(String, (String, Float))] = null;

      if(origin == "hadoop") {
    	  scores = text.map({case x =>
      	  val aux = x.split("\t")
      	  val aux2 = aux(1).split(";")
  
      	  (aux2(1).replaceAll(".xyt$", ""), (aux2(0).replaceAll(".xyt$", ""), aux(0).toFloat))
      	  }).persist
      	  
      } else if(origin == "spark") {
    	  scores = text.map({case x =>
      	  val aux = x.replaceAll("^.*;", "").split(",")
  
      	  (aux(1).replace("(", "").replaceAll(".xyt$", ""), (aux(0).replaceAll(".xyt$", ""), aux(2).replace("))", "").toFloat))
      	  }).persist
      }
      
      val bestGenuineScores = scores.reduceByKey({
        case ((t1, s1), (t2, s2)) =>
          if(s1 > s2)
            (t1, s1)
          else
            (t2, s2)
        }).mapValues( { case (template, score) => template } )

      val fc = new FingerprintComparison(fptype);
      val tp = bestGenuineScores.filter({ case (k1, k2) => fc.genuine(k1, k2) }).count()
      
      println("TP: " + tp)
	}
  
}
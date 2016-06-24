package sci2s.sparkresultsanalyzer


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import sci2s.resultsanalyzer.FingerprintComparison

object ResultsAnalyzer {
  
  val usage = """
    Usage: ResultsAnalyzer
      [--results-file path]
      [--genuine-list path]
      [--impostor-list path]
      [--type {sfinge|nist}]
      [--debug]
  """
  
  type OptionMap = Map[Symbol, Any]
  
  var DEBUG = false
  
  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    
    list match {
      case Nil => map
      case "--results-file" :: value :: tail =>
                             nextOption(map ++ Map('resultsfile -> value), tail)
      case "--genuine-list" :: value :: tail =>
                             nextOption(map ++ Map('genuinelist -> value), tail)
      case "--impostor-list" :: value :: tail =>
                             nextOption(map ++ Map('impostorlist -> value), tail)
      case "--type" :: value :: tail =>
                             nextOption(map ++ Map('type -> value), tail)
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
      val genuineList = options.get('genuinelist).get.toString
      val impostorList = options.get('impostorlist).get.toString
      val fptype = options.get('type).get.toString
      
      val scores : RDD[(String, (String, Float))] = sc.textFile(resultsFile).map({case x =>
        val aux = x.split("\t")
        val aux2 = aux(1).split(";")
        
        (aux2(1).replaceAll(".xyt$", ""), (aux2(0).replaceAll(".xyt$", ""), aux(0).toFloat))
      }).persist
      
      val genuines = sc.textFile(genuineList).map(f => f.replaceAll("^.*/", "").replaceAll(".xyt$", "")).collect.toSet
      val impostors = sc.textFile(impostorList).map(f => f.replaceAll("^.*/", "").replaceAll(".xyt$", "")).collect.toSet
      

      val genuineScores = scores.filter({case (k,v) => genuines.contains(k)})
      val impostorScores = scores.filter({case (k,v) => !genuines.contains(k)}).map({case (k1, (k2, score)) => score}).sortBy(v => v, ascending=true)
      
//      println(" " + genuineScores.count() + " " + impostorScores.count())
      
      
      //val threshold = impostorScores.reduce(Math.min(_,_))
      
      val FAR = 0.0 / 100.0
      
      val numtemplates = impostors.size * 20
      val farPosition = (FAR*numtemplates*impostors.size).toLong
      
      println("Position of the FAR: " + farPosition)
      
      val impostorNumericScores = impostorScores.zipWithIndex.map({case(k,v) => (v,k)}).lookup(farPosition)
      
      
      val threshold = if(impostorNumericScores.size == 0)
        0
      else
        impostorNumericScores(0);
      
      print("Treshold: " + threshold)
      
      val bestGenuineScores = genuineScores.filter({case (k1, (k2, score)) => (score > threshold)}).reduceByKey({
        case ((t1, s1), (t2, s2)) =>
          if(s1 > s2)
            (t1, s1)
          else
            (t2, s2)
        }).mapValues( { case (k2, score) => k2 } )

      val fc = new FingerprintComparison(fptype);
      
      val tp = bestGenuineScores.filter({ case (k1, k2) => fc.genuine(k1, k2) }).count()
      val fn = genuines.size - tp
      
      val fnr = fn.toFloat / genuines.size
      
      println("TP: " + tp + "\tFNR: " + fnr)
	}
  
}
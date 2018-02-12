package org.clmslogprocess.perf.apacheLog

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.util.control.Breaks._
import java.util.regex.Pattern

/**
 * Define the log structure.
 */
case class ApacheAccessLog(
  ipAddress: String,
  clientIdentd: String,
  userId: String,
  dateTime: String,
  method: String,
  endpoint: String,
  protocol: String,
  responseCode: Int,
  contentSize: Long) {
  
  
}


object accesslogProcess {
  
  /**
   * The main method [entry point] of the function.
   */
  def main(args: Array[String]) {
    
    val inputPath = args(1)
    
    val conf = new SparkConf()
    .setAppName("Access log processor")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf);
    
    // var brk = new Breakable;
    var counter:Integer = 1;
    for (line <- Source.fromFile(inputPath).getLines()) {
      breakable {
        if (counter > 10) break;
        
        try {
          println(line);
          parseLogLine(line);  
          
        }
        catch {
          case e:RuntimeException => println("Unable to process log file line. counter[" + counter + "]");
        }      
        counter = counter + 1;
      }
    }
    
  }
  
  /**
    * Parse log entry from a string.
    *
    * @param log A string, typically a line from a log file
    * @return An entry of Apache access log
    * @throws RuntimeException Unable to parse the string
    */
 //def parseLogLine(log: String): ApacheAccessLog = {
 def parseLogLine(log: String) = {
    
    val log = """94.102.63.11 - - [21/Jul/2009:02:48:13 -0700] "GET / HTTP/1.1" 200 18209 "http://acme.com/foo.php" "Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)"""
    val ddd = "\\d{1,3}"                      // at least 1 but not more than 3 times (possessive)
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"      // like `123.456.7.89`
    val client = "(\\S+)"                         // '\S' is 'non-whitespace character'
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"              // like `[21/Jul/2009:02:48:13 -0700]`
    val request = "\"(.*?)\""                 // any number of any character, reluctant
    val status = "(\\d{3})"
    val bytes = "(\\S+)"                      // this can be a "-"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    val ptrn = Pattern.compile(regex)
    
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)"""
    // val PATTERN = regex
    
    println("Matched ::" + log.matches(regex));
    
    
    /*log match {
      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode, contentSize)
      => ApacheAccessLog(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode.toInt,
        contentSize.toLong)
      case _ => throw new RuntimeException(s"""Cannot parse log line: $log""")
    }*/
  }
  
  
}
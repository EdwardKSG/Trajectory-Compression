import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.control._

/** The main class */
object DP extends DP {
    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("DPhull")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    /** Main function */
    def main(args: Array[String]): Unit = {

        val lines = sc.textFile("FILE_PATH") // put the file path here
        val points = parsePoints(lines)
        
        // each object and its trajectory (an array of points in chronological order)
        val grouped = points.groupBy(_.id).map(x => (x._1, x._2.toArray.sortBy(y => y.time)))
        
        // each object and its trajectory compressed by Douglas Peucker algorithm
        val compressedGrouped = grouped.map(x => (x._1, DouglasPeucker(x._2.map(y => (y.location, y.time)), 0, x._2.length-1)))
        
        val compressed = compressedGrouped.flatMap(x => x._2.map(y => (x._1, y._1, y._2)))
        
        compressed.saveAsTextFile("OUTPUT_DIRECTORY") // put the output directory here
        
  }
}


/** The parsing and kmeans methods */
class DP extends Serializable {

    /** DP parameter: Error tolerance */
    def epsilon: Double = 0.0001D

    /** Load points from the given file */
    def parsePoints (lines: RDD[String]): RDD[Point] =
        lines.map (line => {
            val arr = line.split('\t')
            new Point (
                id = arr(0).toInt,
                location = new Vector (x = arr(1).toDouble, y = arr(2).toDouble),
                time = arr(3).toLong)
        })
        
    
    /** Main Douglas Peucker algorithm */
    final def DouglasPeucker (points: Array[(Vector, Long)], startIndex: Int, lastIndex: Int) : Array[(Vector, Long)] = {
    	var dmax = 0.0D
    	var index = startIndex
     
    	for (i <- (index + 1) until lastIndex) {
     		val d = pointLineDistance(points(i)._1, points(startIndex)._1, points(lastIndex)._1)
     		if (d > dmax) {
    			index = i
    			dmax = d
    		}
    	}
     
    	if (dmax > epsilon) {
    		val left = DouglasPeucker(points, startIndex, index)
    		val right = DouglasPeucker(points, index, lastIndex)
     	    return left.slice(0, left.length-1) ++ right
    	}
    	else {
    	    return Array(points(startIndex), points(lastIndex))
    	}
    }        
        
        
    /** util function */   
    
    def pointLineDistance (point: Vector, start: Vector, end: Vector): Double = {
        if (start.equals(end)) {
    		return point.eucDistance(start)
    	}
    	
    	val n = Math.abs((end.x - start.x) * (start.y - point.y) - (start.x - point.x) * (end.y - start.y))
    	val d = start.eucDistance(end)
    	
    	return n / d
    }
    
}
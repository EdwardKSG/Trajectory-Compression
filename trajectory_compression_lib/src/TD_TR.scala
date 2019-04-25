import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.control._


/** The main class */
object TD_TR extends TD_TR {
    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("DPhull")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    /** Main function */
    def main(args: Array[String]): Unit = {

        val lines = sc.textFile("FILE_PATH") // put the file path here
        val points = parsePoints(lines)
        
        // each object and its trajectory (an array of points in chronological order)
        val grouped = points.groupBy(_.id).map(x => (x._1, x._2.toArray.sortBy(y => y.time)))
        
        // each object and its trajectory compressed by Douglas Peucker algorithm (with SED as error metric)
        val compressedGrouped = grouped.map(x => (x._1, DouglasPeucker(x._2.map(y => (y.location, y.time)), 0, x._2.length-1)))
        
        val compressed = compressedGrouped.flatMap(x => x._2.map(y => (x._1, y._1, y._2)))
        
        compressed.saveAsTextFile("OUTPUT_DIRECTORY") // put the output directory here
        
  }
}


/** The parsing and kmeans methods */
class TD_TR extends Serializable {

    /** TD-TR parameter: Error tolerance */
    def epsilon: Double = 0.01D

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
     		val d = SED(points(i), points(startIndex), points(lastIndex))
     		if (d > dmax) {
    			index = i
    			dmax = d
    		}
    	}
     
    	if (dmax > epsilon) {
    		val left = DouglasPeucker(points, startIndex, index);
    		val right = DouglasPeucker(points, index, lastIndex);
     	    return left.slice(0, left.length-1) ++ right
    	}
    	else {
    	    return Array(points(startIndex), points(lastIndex))
    	}
    }
        
        
        
    /** util function */   
    def SED (point: (Vector, Long), start: (Vector, Long), end: (Vector, Long)): Double = {
        if (start.equals(end)) {
    		return point._1.eucDistance(start._1)
    	}
    	
    	val timeRatio = (point._2 - start._2)/(end._2 - start._2)
    	val deltaX = end._1.x - start._1.x
    	val deltaY = end._1.y - start._1.y
    	val projectionVector = new Vector (x = start._1.x + deltaX * timeRatio, y = start._1.y + deltaY * timeRatio)

    	return point._1.eucDistance(projectionVector)
    }
    
}
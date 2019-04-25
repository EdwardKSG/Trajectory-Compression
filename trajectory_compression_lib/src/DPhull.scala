import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.control._
import java.util.ArrayDeque
import java.util.Stack

/** a homogeneous line */
case class Homog (x: Double, y: Double, w: Double)

/** a wrapper class to make it possible to be passed by reference) */
case class MidTag (var value: Int)




/** The main class */
object DPhull extends DPhull {
    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("DPhull")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    /** Main function */
    def main(args: Array[String]): Unit = {

        val lines = sc.textFile("FILE_PATH") // put the file path here
        val points = parsePoints(lines)
        
        // each object and its trajectory (an array of points in chronological order)
        val grouped = points.groupBy(_.id).map(x => (x._1, x._2.toArray.sortBy(y => y.time)))
        
        // each object and its trajectory compressed by Douglas Peucker algorithm (with hull simplification)
        val compressedGrouped = grouped.map(x => (x._1, DPhullManager(x._2.map(y => (y.location, y.time)), x._2.length)))
        
        val compressed = compressedGrouped.flatMap(x => x._2.map(y => (x._1, y._1, y._2)))
        
        compressed.saveAsTextFile("OUTPUT_DIRECTORY") // put the output directory here
        
  }
}


/** The parsing and kmeans methods */
class DPhull extends Serializable {

    /** DPhull parameter: Error tolerance */
    def epsilon: Double = 0.0001D
    def epSquare: Double = 0.00000001D


    /** Load points from the given file */
    def parsePoints (lines: RDD[String]): RDD[Point] =
        lines.map (line => {
            val arr = line.split('\t')
            new Point (
                id = arr(0).toInt,
                location = new Vector (x = arr(1).toDouble, y = arr(2).toDouble),
                time = arr(3).toLong)
        })
        
    
    /** Management function of Douglas Peucker algorithm with hull simplification*/
    final def DPhullManager (points: Array[(Vector, Long)], size: Int) : Array[(Vector, Long)] = {
    	var indexedPoints : Array[(Vector, Long, Int)] = new Array[(Vector, Long, Int)](points.length)
    	for (i <- 0 until points.length) {
    	    indexedPoints(i) = (points(i)._1, points(i)._2, i)
    	}
    	
        
        var midTag : MidTag = MidTag (value = 0)
    	
        val hulls = buildHull (indexedPoints, 0, size - 1, midTag)
        var leftHull = hulls._1
        var rightHull = hulls._2
        println ("the initial sizes are")
        println (leftHull.convexHull.size())
        println (rightHull.convexHull.size())
    	
    	DPhullAction(indexedPoints, 0, size - 1, midTag, leftHull, rightHull).map(x => (x._1, x._2))
    }
    
    /** Main Douglas Peucker algorithm with hull simplification*/
    final def DPhullAction (points: Array[(Vector, Long, Int)], startIndex: Int, lastIndex: Int, midTag: MidTag,
            leftHull: PathHull, rightHull: PathHull) : Array[(Vector, Long, Int)] = {
        val line : Homog = crossProductToHomog (points(startIndex)._1, points(lastIndex)._1)
        val lengthSquare = line.x * line.x + line.y * line.y
        
        if (lastIndex - startIndex <= 1) {
            Array(points(lastIndex))
            
        } else {
            println("see the sizes")
            println(leftHull.convexHull.size())
            println(rightHull.convexHull.size())
            if (leftHull.convexHull.size() == 0 || rightHull.convexHull.size() == 0) {
                
                Array(points(lastIndex))
            } else {
                val leftExtreme = leftHull.findFarthest (line)
                println("done with left extreme")
                val rightExtreme = rightHull.findFarthest (line)
                println("done with right extreme")
                val leftExtrPoint = leftExtreme._1
                val leftExtrDist = leftExtreme._2
                val rightExtrPoint = rightExtreme._1
                val rightExtrDist = rightExtreme._2
                
                if (leftExtrDist <= rightExtrDist ) {
                    if (rightExtrDist * rightExtrDist <= epSquare * lengthSquare) {
                        Array(points(lastIndex))
                        
                    } else {
                        if (midTag.value == rightExtrPoint._3) {
                            val hulls = buildHull(points, startIndex, rightExtrPoint._3, midTag)
                            leftHull.clone(hulls._1)
                            rightHull.clone(hulls._2)  
                            
                        } else {
                            rightHull.split(rightExtrPoint)                                         
                        }
                        
                        val leftResult = DPhullAction (points, startIndex, rightExtrPoint._3, midTag, leftHull, rightHull)
                        val hulls2 = buildHull(points, rightExtrPoint._3, lastIndex, midTag)
                        leftHull.clone(hulls2._1)
                        rightHull.clone(hulls2._2)
                        val rightResult = DPhullAction (points, rightExtrPoint._3, lastIndex, midTag, leftHull, rightHull)
                        
                        return leftResult ++ rightResult
                    }
                    
                } else {
                    if (leftExtrDist * leftExtrDist <= epSquare * lengthSquare) {
                        Array(points(lastIndex))
                        
                    } else {
                        leftHull.split(leftExtrPoint)
                        val rightResult = DPhullAction (points, leftExtrPoint._3, lastIndex, midTag, leftHull, rightHull)
                        val hulls3 = buildHull(points, startIndex, leftExtrPoint._3, midTag)
                        leftHull.clone(hulls3._1)
                        rightHull.clone(hulls3._2)                    
                        val leftResult = DPhullAction (points, startIndex, leftExtrPoint._3, midTag, leftHull, rightHull)
                        
                        return leftResult ++ rightResult
                    }
                }
            }
            

        }
    }
        
        
        
    /** util functions */
    
    /** build path hull for given trajectory chain */
    def buildHull (points: Array[(Vector, Long, Int)], startIndex: Int, lastIndex: Int, midTag: MidTag) : (PathHull, PathHull) = {
        val mid : Int = (startIndex + lastIndex) / 2
        midTag.value = mid;
        
        var leftHull : PathHull = new PathHull(new ArrayDeque[(Vector, Long, Int)](), new Stack[(Vector, Long, Int)](), new Stack[Int]())
        if (mid >= 1) {
            leftHull.init(points(mid), points(mid-1))
        }
        println("start and ending indexes and mid are")
        println(startIndex)
        println(lastIndex)
        println(mid)
        if (mid >= startIndex + 2) {
            for (i <- mid-2 to startIndex by -1) {
                leftHull.add(points(i))
            }
        }
        
        var rightHull : PathHull = new PathHull(new ArrayDeque[(Vector, Long, Int)](), new Stack[(Vector, Long, Int)](), new Stack[Int]())
        if (mid <= points.length - 2) {
            rightHull.init(points(mid), points(mid+1))
        }
        if (mid <= lastIndex - 2) {
            for (i <- mid+2 to lastIndex) {
                rightHull.add(points(i))
            }
        }
        
        (leftHull, rightHull)
    }
    
    /** check is the point is at the left of the line */
    def leftOf (point: Vector, start: Vector, end: Vector): Boolean = { 
        ((start.x - point.x) * (end.y - point.y)) >= ((end.x - point.x) * (start.y - point.y))
        
    }
    
    def crossProductToHomog (start: Vector, end: Vector) : Homog = {
        // get the perpendicular vector
        Homog (
                x = start.y - end.y,
                y = end.x - start.x,
                w = start.x * end.y - start.y * end.x
                )
    }
    
    def dotProductWithHomog (point: Vector, line: Homog) : Double = {
        line.w + point.x * line.x + point.y * line.y
    }
    
    def slopeSign (point1: Vector, point2: Vector, line: Homog) : Int = {
        // dot product between line (perpendicular vector of original) and edge (point2 - point1)
        val slope = line.x * (point2.x - point1.x) + line.y * (point2.y - point1.y)
        
        if (slope <= 0) { // negative cosine, positive angle between this edge and original vector
            return 1
        } else {
            return -1
        }
    }
   
    
    def pointLineDistance (point: Vector, start: Vector, end: Vector): Double = {
        if (start.equals(end)) {
    		return point.eucDistance(start)
    	}
    	
    	val n = Math.abs((end.x - start.x) * (start.y - point.y) - (start.x - point.x) * (end.y - start.y))
    	val d = start.eucDistance(end)
    	
    	return n / d
    }
  
}
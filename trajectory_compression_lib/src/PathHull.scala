import java.util.ArrayDeque
import java.util.Stack

/**
 * Convex path hull
 */
class PathHull (
        var convexHull: ArrayDeque[(Vector, Long, Int)], 
        var pointHistory: Stack[(Vector, Long, Int)], 
        var operationHistory: Stack[Int]) {
    
    /** Hull operation numbers */
    val pushOp: Int = 0;
    val popTopOp: Int = 1;
    val popBotOp: Int = 2;
    
    def clone (anotherHull : PathHull) = {
        this.convexHull = anotherHull.convexHull
        this.pointHistory = anotherHull.pointHistory
        this.operationHistory = anotherHull.operationHistory
    }
    
    def push (point: (Vector, Long, Int)) = {
        pointHistory.push(point)
        operationHistory.push(pushOp)
        convexHull.addLast(point)
        convexHull.addFirst(point)
    }
    
    def popTop () = {
        pointHistory.push(convexHull.removeLast())
        operationHistory.push(popTopOp)
    }
    
    def popBot () = {
        pointHistory.push(convexHull.removeFirst())
        operationHistory.push(popBotOp)
    }
    
    def init (point1: (Vector, Long, Int), point2: (Vector, Long, Int)) = {
        pointHistory.push(point2)
        operationHistory.push(pushOp)
        convexHull.add(point1)
        convexHull.addLast(point2)
        convexHull.addFirst(point2)
    }    
    
    def add (point: (Vector, Long, Int)) = { // implements Melkman's convex hull algorithm
        var topLast = convexHull.removeLast()
        var botLast = convexHull.removeFirst()
        
        var topFlag = DPhull.leftOf (topLast._1, convexHull.peekLast()._1, point._1)
        convexHull.addLast(topLast)
        var botFlag = DPhull.leftOf (convexHull.peekFirst()._1, botLast._1, point._1)
        convexHull.addFirst(botLast)
        
        if (topFlag || botFlag) { // if the new point is outside the hull
            while (topFlag && convexHull.size() >= 3) {
                popTop()
                
                if (convexHull.size() >= 2) {
                    topLast = convexHull.removeLast()   
                    
                    topFlag = DPhull.leftOf (topLast._1, convexHull.peekLast()._1, point._1)
                    convexHull.addLast(topLast)
                }
            }
            
            while (botFlag && convexHull.size() >= 3) {
                popBot()
                
                if (convexHull.size() >= 2) {
                    botLast = convexHull.removeFirst()
                    
                    botFlag = DPhull.leftOf (convexHull.peekFirst()._1, botLast._1, point._1)
                    convexHull.addFirst(botLast)
                }
            }
            
            push (point)
        }
        
    }
    
    def split (point: (Vector, Long, Int)) = {
        var tempPoint : (Vector, Long, Int) = pointHistory.peek()
        var tempOp : Int = operationHistory.peek()
        
        while ( !pointHistory.empty() && !operationHistory.empty()
            && (!tempPoint._1.equals(point._1) || tempPoint._2 != point._2 || tempOp != pushOp)) { // find the push operation for this given point
            pointHistory.pop();
            operationHistory.pop();
            
            tempOp match {
                case `pushOp`=> 
                    convexHull.removeLast(); convexHull.removeFirst();
                case `popTopOp` =>
                    convexHull.addLast(tempPoint)
                case `popBotOp` =>
                    convexHull.addFirst(tempPoint)                   
            }
            
            if (!pointHistory.empty() && !operationHistory.empty()) {
                tempPoint = pointHistory.peek()
                tempOp = operationHistory.peek()
            }
        }
    }
    
     
    /** find the point farthest to given line */
    def findFarthest (line: Homog) : ((Vector, Long, Int), Double) = {
        var hullArray = convexHull.toArray(new Array[(Vector, Long, Int)](0))
        
        if (convexHull.size() >= 7) { // otherwise just traverse all            
            var lower : Int = 0
            var higher : Int = convexHull.size() - 2
            var breakPoint : Int = 0
            var breakPointSign : Int = 0
            var breakPointAdgacentSign : Int = 0
            var extreme1 : Int = 0
            var extreme2 : Int = 0            
            val baseEdgeSign = DPhull.slopeSign (hullArray(lower)._1, hullArray(lower + 1)._1, line)
            val lastEdgeSign = DPhull.slopeSign (hullArray(higher)._1, hullArray(higher + 1)._1, line)
            
            if (baseEdgeSign != lastEdgeSign) {
                extreme1 = 0; // point with index 0 has a tangent parallel to the given shortcut line
                
                do { // binary search for the other extreme point (where the tangent is parallel)
                    breakPoint = ((lower + 1) + higher)/2
                    breakPointSign = DPhull.slopeSign (hullArray(breakPoint-1)._1, hullArray(breakPoint)._1, line)
                    breakPointAdgacentSign = DPhull.slopeSign (hullArray(breakPoint)._1, hullArray(breakPoint+1)._1, line)
                    
                    if (breakPointSign != breakPointAdgacentSign) {
                        extreme2 = breakPoint;
                    } else {
                        if (baseEdgeSign == breakPointSign) {
                            lower = breakPoint
                        } else {
                            higher = breakPoint
                        }

                    }
                    
                } while (breakPointSign == breakPointAdgacentSign)
            } else {
                var mid : Int = ((lower + 1) + higher)/2
                var point = mid
                var pointSign : Int = 0
                var pointAdgacentSign : Int = 0
                var newBaseEdgeSign : Int = baseEdgeSign

                var i : Int = -1;
                do {
                    i += 1 // start from 0
                    point = mid + i
                    if (point + 1 <= convexHull.size() - 1) {
                        pointSign = DPhull.slopeSign (hullArray(point-1)._1, hullArray(point)._1, line)
                        pointAdgacentSign = DPhull.slopeSign (hullArray(point)._1, hullArray(point+1)._1, line)
                    }
                    if (pointSign != pointAdgacentSign) {
                        extreme1 = point
                        if (pointSign == baseEdgeSign) {
                            lower = extreme1
                            newBaseEdgeSign = -baseEdgeSign
                        } else {
                            higher = extreme1 - 1
                        }
                    } else {
                        point = mid - i
                        if (point -1 >= 0) {
                            pointSign = DPhull.slopeSign (hullArray(point-1)._1, hullArray(point)._1, line)
                            pointAdgacentSign = DPhull.slopeSign (hullArray(point)._1, hullArray(point+1)._1, line)
                            if (pointSign != pointAdgacentSign) {
                                extreme1 = point
                                if (pointSign == baseEdgeSign) {
                                    lower = extreme1
                                    newBaseEdgeSign = -baseEdgeSign
                                } else {
                                    higher = extreme1 - 1
                                }
                            }
                        }
                    }
                    
                } while (pointSign == pointAdgacentSign)                                 
                
                do { // binary search for the other extreme point (where the tangent is parallel)
      
                    breakPoint = ((lower + 1) + higher)/2
                    breakPointSign = DPhull.slopeSign (hullArray(breakPoint-1)._1, hullArray(breakPoint)._1, line)
                    breakPointAdgacentSign = DPhull.slopeSign (hullArray(breakPoint)._1, hullArray(breakPoint+1)._1, line)
                    
                    if (breakPointSign != breakPointAdgacentSign) {
                        extreme2 = breakPoint;
                    } else {
                        if (newBaseEdgeSign == breakPointSign) {
                            lower = breakPoint
                        } else {
                            higher = breakPoint
                        }

                    }
                    
                } while (breakPointSign == breakPointAdgacentSign)
                
            }
            
            
            
            val extrDist1 = Math.abs(DPhull.dotProductWithHomog(hullArray(extreme1)._1, line))
            val extrDist2 = Math.abs(DPhull.dotProductWithHomog(hullArray(extreme2)._1, line))
            
            if (extrDist1 > extrDist2) {
                return (hullArray(extreme1), extrDist1)
            } else {
                return (hullArray(extreme2), extrDist2)
            }
            
        } else {
            var maxDist : Double = 0.0
            var index : Int = 0
            val end = convexHull.size - 1
            

            for (i <- 0 to end) {
                
                var dist = Math.abs(DPhull.dotProductWithHomog(hullArray(i)._1, line))
                if (dist > maxDist) {
                    maxDist = dist
                    index = i
                }                      
            }
            
            return (hullArray(index), maxDist)
            
        }
    }
}
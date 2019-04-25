/**
 * Coordinate of a point on 2-D plane
 */
class Vector (var x: Double, var y: Double) {
        
    def equals (location: Vector): Boolean = {
        return (this.x == location.x && this.y == location.y)        
    }
    
    /** Euclidean distance */
    def eucDistance (location: Vector): Double = {
        val deltaX = location.x - this.x
        val deltaY = location.y - this.y
        return Math.sqrt(deltaX*deltaX + deltaY*deltaY);
    }
    
    /** normalize */
    def normalize (): Vector = {
        val length = Math.sqrt(x*x + y*y); 

        if (length != 0.0) {
            val s = 1.0D / length;
            return new Vector (x = this.x * s, y = this.y * s)
        } else {
            return this
        }
    }
}
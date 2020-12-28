package edu.ucr.cs.bdlab.beast.cg

import java.awt.Rectangle

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeNDLite, IFeature}
import org.apache.hadoop.util.{IndexedSortable, QuickSort}
import org.apache.spark.util.LongAccumulator
import edu.ucr.cs.bdlab.beast.cg.CGOperationsMixin._

/**
 * A class that runs the plane-sweep join algorithm and emits records one pair at a time.
 * Used to avoid keeping all pairs in memory before producing the final result.
 * We include the duplicate avoidance testing here since it is more efficient to test when we already know the MBRs
 * and because of the way we scale the MBRs to integer coordinates which require a corresponding change to the
 * duplicate avoidance MBR
 */
class PlaneSweepSpatialJoinIterator[T1 <: IFeature, T2 <: IFeature]
  (r1: Array[T1], r2: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, numMBRTests: LongAccumulator = null)
  extends Iterator[(T1, T2)] {

  // Retrieve the bounding rectangles of features to run the plane-sweep algorithm efficiently
  val xmin1: Array[Int] = new Array[Int](r1.size)
  val xmax1: Array[Int] = new Array[Int](r1.size)
  val ymin1: Array[Int] = new Array[Int](r1.size)
  val ymax1: Array[Int] = new Array[Int](r1.size)

  val xmin2: Array[Int] = new Array[Int](r2.size)
  val xmax2: Array[Int] = new Array[Int](r2.size)
  val ymin2: Array[Int] = new Array[Int](r2.size)
  val ymax2: Array[Int] = new Array[Int](r2.size)

  // Scale all MBRs to Integer coordinates to speed up the calculations and reduce memory footprint
  val scaleMBR: EnvelopeNDLite = r1.mbr
  scaleMBR.merge(r2.mbr)
  if (dupAvoidanceMBR != null && !dupAvoidanceMBR.getSideLength(0).isInfinity)
    scaleMBR.merge(dupAvoidanceMBR)
  var scaleX: Double = (Integer.MAX_VALUE - 1) / scaleMBR.getSideLength(0)
  var scaleY: Double = (Integer.MAX_VALUE - 1) / scaleMBR.getSideLength(1)

  /**
   * Calculate the MBR of all features and store them in arrays. It also scales the coordinates to integers to speed
   * up the calculations and reduce the memory footprint.
   * @param r the list of features
   * @param xmin the array that will hold the minimum x coordinates
   * @param ymin the array that will hold the minimum y coordinates
   * @param xmax the array that will hold the maximum x coordinates
   * @param ymax the array that will hold the maximum y coordinates
   */
  private def featuresMBRs(r: Array[_ <: IFeature], xmin: Array[Int], ymin: Array[Int], xmax: Array[Int], ymax: Array[Int]) = {
    val recordMBR = new EnvelopeNDLite(2)
    for (i <- 0 until r.size) {
      recordMBR.setEmpty()
      recordMBR.merge(r(i).getGeometry)
      xmin(i) = ((recordMBR.getMinCoord(0) - scaleMBR.getMinCoord(0)) * scaleX).floor.toInt
      ymin(i) = ((recordMBR.getMinCoord(1) - scaleMBR.getMinCoord(1)) * scaleY).floor.toInt
      xmax(i) = ((recordMBR.getMaxCoord(0) - scaleMBR.getMinCoord(0)) * scaleX).ceil.toInt
      ymax(i) = ((recordMBR.getMaxCoord(1) - scaleMBR.getMinCoord(1)) * scaleY).ceil.toInt
    }
  }

  featuresMBRs(r1, xmin1, ymin1, xmax1, ymax1)
  featuresMBRs(r2, xmin2, ymin2, xmax2, ymax2)

  val integerDupAvoidanceMBR = if (dupAvoidanceMBR == null || dupAvoidanceMBR.getSideLength(0).isInfinity)
    null
    else new Rectangle(
    ((dupAvoidanceMBR.getMinCoord(0) - scaleMBR.getMinCoord(0)) * scaleX).floor.toInt,
    ((dupAvoidanceMBR.getMinCoord(1) - scaleMBR.getMinCoord(1)) * scaleY).floor.toInt,
    (dupAvoidanceMBR.getSideLength(0) * scaleX).toInt,
    (dupAvoidanceMBR.getSideLength(1) * scaleY).toInt)

  // Sort both lists on xmin along with the features
  val indexedSortable1 = new IndexedSortable {
    override def compare(i: Int, j: Int): Int = Math.signum(xmin1(i) - xmin1(j)).toInt

    override def swap(i: Int, j: Int): Unit = {
      var temp: Int = 0
      temp = xmin1(i); xmin1(i) = xmin1(j); xmin1(j) = temp;
      temp = xmax1(i); xmax1(i) = xmax1(j); xmax1(j) = temp;
      temp = ymin1(i); ymin1(i) = ymin1(j); ymin1(j) = temp;
      temp = ymax1(i); ymax1(i) = ymax1(j); ymax1(j) = temp;
      val tempF = r1(i); r1(i) = r1(j); r1(j) = tempF
    }
  }
  val indexedSortable2 = new IndexedSortable {
    override def compare(i: Int, j: Int): Int = Math.signum(xmin2(i) - xmin2(j)).toInt

    override def swap(i: Int, j: Int): Unit = {
      var temp: Int = 0
      temp = xmin2(i); xmin2(i) = xmin2(j); xmin2(j) = temp;
      temp = xmax2(i); xmax2(i) = xmax2(j); xmax2(j) = temp;
      temp = ymin2(i); ymin2(i) = ymin2(j); ymin2(j) = temp;
      temp = ymax2(i); ymax2(i) = ymax2(j); ymax2(j) = temp;
      val tempF = r2(i); r2(i) = r2(j); r2(j) = tempF
    }
  }

  new QuickSort().sort(indexedSortable1, 0, r1.size)
  new QuickSort().sort(indexedSortable2, 0, r2.size)

  // Initialize the plane-sweep algorithm and make it ready to emit records
  var i: Int = 0
  var j: Int = 0
  var ii: Int = 0
  var jj: Int = 0

  /**The list that is currently active, either 1 or 2*/
  var activeList: Int = 0

  /**Prepare the first result (if any)*/
  seekToNextOutput()

  def rectangleOverlaps(a: Int, b: Int): Boolean =
    !(xmin1(a) > xmax2(b) || xmin2(b) > xmax1(a) || ymin1(a) > ymax2(b) || ymin2(b) > ymax1(a))

  /**
   * Move to the next matching pair of records. The matching pair (if any), it should always be stored in ii and jj
   * @return whether a result was found [[true]] or an end-of-list was reached [[false]]
   */
  def seekToNextOutput(): Boolean = {
    while (i < r1.size  && j < r2.size) {
      // If not list is currently activated, activate the list with the left-most rectangle
      if (activeList == 0) {
        activeList = if (xmin1(i) < xmin2(j)) 1 else 2
        ii = i
        jj = j
      } else if (activeList == 1) {
        jj += 1
      } else if (activeList == 2) {
        ii += 1
      }
      if (activeList == 1) {
        // Fix the record in list 1, and go through list 2 until the first matching pair is found
        while (jj < r2.size && xmin2(jj) <= xmax1(ii)) {
          if (numMBRTests != null) numMBRTests.add(1)
          if (rectangleOverlaps(ii, jj)) {
            // No duplicate avoidance test needed
            if (integerDupAvoidanceMBR == null)
              return true
            val refPointX: Int = xmin1(ii) max xmin2(jj)
            val refPointY: Int = ymin1(ii) max ymin2(jj)
            if (numMBRTests != null) numMBRTests.add(1)
            if (integerDupAvoidanceMBR.contains(refPointX, refPointY)) {
              // Found a result, return it
              return true
            }
          }
          jj += 1
        }
        do {
          i += 1;
        } while (i < r1.size && xmax1(i) < xmin2(j))
        // Reset the active list
        activeList = 0
      } else if (activeList == 2) {
        // Fix the record in list 2, and go through list 1 until the first matching pair is found
        while (ii < r1.size && xmin1(ii) <= xmax2(jj)) {
          if (numMBRTests != null) numMBRTests.add(1)
          if (rectangleOverlaps(ii, jj)) {
            // No duplicate avoidance test needed
            if (integerDupAvoidanceMBR == null)
              return true;
            val refPointX: Int = xmin1(ii) max xmin2(jj)
            val refPointY: Int = ymin1(ii) max ymin2(jj)
            if (numMBRTests != null) numMBRTests.add(1)
            if (integerDupAvoidanceMBR.contains(refPointX, refPointY)) {
              // Found a result, return it
              return true
            }
          }
          ii += 1
        }
        // Skip until the first record that might produce a result from list 2
        do {
          j += 1;
        } while (j < r2.size && xmax2(j) < xmin1(i))
        // Reset the active list
        activeList = 0
      }
    }
    // Finished the lists without finding any results
    false
  }

  override def hasNext: Boolean = i < r1.size && j < r2.size

  override def next(): (T1, T2) = {
    val matchedPair = (r1(ii), r2(jj))
    seekToNextOutput()
    matchedPair
  }
}

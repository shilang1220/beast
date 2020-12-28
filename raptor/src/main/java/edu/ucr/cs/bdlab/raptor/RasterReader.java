/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.raptor;

import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.geom.Point2D;
import java.io.Closeable;
import java.io.IOException;

public interface RasterReader extends Closeable {

  /**Configuration entry for the raster layer ID*/
  String RasterLayerID = "RasterReader.RasterLayerID";

  /**
   * Returns the unique ID of the tile that contains the given pixel coordinate.
   * @param iPixel the index of the column of the pixel, i.e., x-coordinate in grid space
   * @param jPixel the index of the row of the pixel, i.e., y-coordinate
   * @return the ID of the tile that contains the given pixel
   */
  int getTileID(int iPixel, int jPixel);

  /**
   * The total number of tiles in this raster layer.
   * @return total number of tiles
   */
  int getNumTiles();

  /**
   * Number of tiles along the x-coordinate, i.e. number of tiles per row.
   * @return number of tiles per row
   */
  int getNumTilesX();

  /**
   * Number of tiles along the y-coordinate, i.e., number of tiles per column.
   * @return number of tiles per column
   */
  int getNumTilesY();

  /**
   * The index of the lowest row (scanline)
   * @return the index of the lowest row (inclusive)
   */
  int getY1();

  /**
   * The index of the highest row (scanline)
   * @return the index of the highest row (exclusive)
   */
  int getY2();

  /**
   * The index of the lowest column
   * @return the index of the lowest column
   */
  int getX1();

  /**
   * The index of the highest column
   * @return the highest index of the column (exclusive)
   */
  int getX2();

  /**
   * Returns the coordinate of the left column of the given tile
   * @param tileID the ID of the tile
   * @return the first column of this given tile (inclusive)
   */
  int getTileX1(int tileID);

  /**
   * Returns the coordinate of the last column of the given tile (inclusive)
   * @param tileID the ID of the tile
   * @return the last column of the given tile (inclusive)
   */
  int getTileX2(int tileID);

  int getRasterWidth();

  int getRasterHeight();

  /**
   * Converts a point in the vector space (double coordinates, model coordinates) to a point in
   * the raster space (grid coordinates). Even though the output point is in grid coordinates, it is still
   * represented as double to provide information on where the point is within the pixel.
   * If the returned value is floored, then it gives the pixel that contains the point.
   * If it is rounded, then it gives the pixel with the nearest lower corner.
   * @param mx the x-coordinate of the point in the model space (e.g., longitude)
   * @param my the y-coordinate of the point in the model space (e.g., latitude)
   * @param outPoint (output) the grid coordinates will be written to this object.
   */
  void modelToGrid(double mx, double my, Point2D.Double outPoint);

  /**
   * Converts a point from the raster space (integer coordinates) to the vector
   * space (double coordinates). This function returns the coordinates of the
   * center of the corresponding pixel. In other words, if a pixel maps to a
   * rectangle, this function returns the center of that rectangle. Notice that
   * the geographic coordinate system (projection) is not taken into account.
   * The returned value is in whatever that coordinate system is.
   * @param gx the x-coordinate of the pixel in grid space (column number)
   * @param gy the y-coordinate of the pixel in grid space (row number)
   * @param outPoint (output) the model coordinates will be written to this point
   */
  void gridToModel(int gx, int gy, Point2D.Double outPoint);

  /**
   * Returns the value of a given pixel. This function assumes that the raster has been already loaded and contains
   * this pixel. While this function can automatically check and load the appropriate part of the raster, it could be
   * too much overhead when reading big portions of the raster file. For performance consideration, the developer must
   * first load the appropriate part of the raster in one call before reading all pixels.
   *
   * @param iPixel the index of the column of the pixel in the grid space
   * @param jPixel the index of the row of the pixel in the grid space
   * @param value (output) the value will be written to this array
   * @throws IOException if an error happens while reading the given pixel
   */
  void getPixelValueAsInt(int iPixel, int jPixel, int[] value) throws IOException;

  /**
   * Similar to {@link #getPixelValueAsInt(int, int, int[])} but returns the values as float. This method is
   * useful if the underlying GeoTIFF file contains float values and the conversion to int will lose the details.
   * @param iPixel the index of the column of the pixel in the grid space
   * @param jPixel the index of the row of the pixel in the grid space
   * @param value (output) the value will be written to this array
   * @throws IOException if an error happens while reading the given pixel
   */
  void getPixelValueAsFloat(int iPixel, int jPixel, float[] value) throws IOException;

  /**
   * Returns the special value that marks undefined values.
   * @return the special value that is used in this file to mark a pixel as unavailable
   */
  int getFillValue();

  /**
   * Checks if the given pixel has fill value
   * @param iPixel the x-coordinate of the pixel
   * @param jPixel the y-coordinate of the pixel
   * @return {@code true} if the value of that pixel is the same as getFillValue
   * @throws IOException if an error happens while reading the file
   */
  boolean isFillValue(int iPixel, int jPixel) throws IOException;

  /**
   * Number of components for each pixel in this raster file.
   * @return the number of components in each pixel, e.g., 3 for RGB or 1 for grayscale
   */
  int getNumComponents();

  /**
   * Returns the value of all the components of the pixel that contains the given point in world coordinate
   * @param x the x-coordinate of the point in the model space
   * @param y the y-coordinate of the point in the model space
   * @param value (output) the value will be written to this array
   * @throws IOException if an error happens while reading the given pixel
   */
  default void getPointValueAsInt(double x, double y, int[] value) throws IOException {
    Point2D.Double outPoint = new Point2D.Double();
    modelToGrid(x, y, outPoint);
    getPixelValueAsInt((int) outPoint.getX(), (int) outPoint.getY(), value);
  }

  /**
   * Returns the value of all the components of the pixel that contains the given point in world coordinate
   * @param x the x-coordinate of the point in the model space
   * @param y the y-coordinate of the point in the model space
   * @param value (output) the value will be written to this array
   * @throws IOException if an error happens while reading the given pixel
   */
  default void getPointValueAsFloat(double x, double y, float[] value) throws IOException {
    Point2D.Double outPoint = new Point2D.Double();
    modelToGrid(x, y, outPoint);
    getPixelValueAsFloat((int) outPoint.getX(), (int) outPoint.getY(), value);
  }

  CoordinateReferenceSystem getCRS();

}

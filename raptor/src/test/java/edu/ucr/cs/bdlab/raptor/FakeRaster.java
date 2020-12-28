package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.raptor.RasterReader;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.geom.Point2D;
import java.io.IOException;

/**
 * Creates a fake raster layer that specific resolution and tiles
 */
class FakeRaster implements RasterReader {

  private final int numTilesX;
  private final int numTilesY;
  private final int tileWidth;
  private final int tileHeight;

  public FakeRaster(int numTilesX, int numTilesY, int tileWidth, int tileHeight) {
    this.numTilesX = numTilesX;
    this.numTilesY = numTilesY;
    this.tileWidth = tileWidth;
    this.tileHeight = tileHeight;
  }

  @Override
  public int getTileID(int iPixel, int jPixel) {
    int tileX = iPixel / tileWidth;
    int tileY = jPixel / tileHeight;
    return tileY * numTilesX + tileX;
  }

  @Override
  public int getNumTiles() {
    return numTilesX * numTilesY;
  }

  @Override
  public int getNumTilesX() {
    return numTilesX;
  }

  @Override
  public int getNumTilesY() {
    return numTilesY;
  }

  @Override
  public int getY1() {
    return 0;
  }

  @Override
  public int getY2() {
    return numTilesY * tileHeight;
  }

  @Override
  public int getX1() {
    return 0;
  }

  @Override
  public int getX2() {
    return numTilesX * tileWidth;
  }

  @Override
  public int getTileX1(int tileID) {
    int tileColumn = tileID % numTilesX;
    return tileColumn * tileWidth;
  }

  @Override
  public int getTileX2(int tileID) {
    int tileColumn = tileID % numTilesX;
    return tileColumn * tileWidth + tileWidth - 1;
  }

  @Override
  public int getRasterWidth() {
    return numTilesX * tileWidth;
  }

  @Override
  public int getRasterHeight() {
    return numTilesY * tileHeight;
  }

  @Override
  public void modelToGrid(double wx, double wy, Point2D.Double grid) {
    grid.setLocation(wx, wy);
  }

  @Override
  public void gridToModel(int gx, int gy, Point2D.Double world) {
    world.setLocation(gx + 0.5, gy + 0.5);
  }

  @Override
  public void getPixelValueAsInt(int iPixel, int jPixel, int[] value) {
  }

  @Override
  public void getPixelValueAsFloat(int iPixel, int jPixel, float[] value) {
  }

  @Override
  public int getFillValue() {
    return -1;
  }

  @Override
  public boolean isFillValue(int iPixel, int jPixel) {
    return false;
  }

  @Override
  public int getNumComponents() {
    return 0;
  }

  @Override
  public CoordinateReferenceSystem getCRS() {
    try {
      return CRS.decode("EPSG:4326", true);
    } catch (FactoryException e) {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
  }
}

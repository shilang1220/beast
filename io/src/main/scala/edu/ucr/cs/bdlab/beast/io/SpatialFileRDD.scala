/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.cg.SparkSpatialPartitioner
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, EnvelopeNDLite, IFeature}
import edu.ucr.cs.bdlab.beast.indexing.CellPartitioner
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD.SpatialFilePartition
import edu.ucr.cs.bdlab.beast.util.{IConfigurable, OperationParam}
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.LineReader
import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

import java.io.File
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A SpatialRDD that is backed by a file. Each partition points to part of a file and contains spatial information
 * about that file if a master file exists.
 */
class SpatialFileRDD(@transient sc: SparkContext, path: String, opts: BeastOptions = new BeastOptions())
  extends SpatialRDD(sc, Seq()) with Logging {

  /**The feature reader class that will be used to read this file*/
  lazy val featureReaderClass: Class[_ <: FeatureReader] = SpatialFileRDD.getFeatureReaderClass(path, opts)

  /**Metadata of the feature reader*/
  lazy val readerMetadata: FeatureReader.Metadata =
    featureReaderClass.getAnnotation[FeatureReader.Metadata](classOf[FeatureReader.Metadata])

  override def getPartitions: Array[Partition] =
    SpatialFileRDD.createPartitions(path, opts, sc.hadoopConfiguration).asInstanceOf[Array[Partition]]

  /**
   * Define the partitioner of this RDD. If this RDD is defined based on a master file that defines the spatial extents
   * of each partition, this partitioner will be defined accordingly. Otherwise, no partitioner is defined.
   */
  @transient override val partitioner: Option[Partitioner] = {
    val allPartitioned = partitions.forall(p => p.asInstanceOf[SpatialFilePartition].mbr != null)
    if (allPartitioned && partitions.nonEmpty)
      Some(new SparkSpatialPartitioner(new CellPartitioner(partitions.map(_.asInstanceOf[SpatialFilePartition].mbr):_*)))
    else None
  }

  override def compute(split: Partition, context: TaskContext): Iterator[IFeature] =
    SpatialFileRDD.readPartition(split.asInstanceOf[SpatialFilePartition], featureReaderClass, opts)

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[SpatialFilePartition].locations
}

// SpatialRDD的同名对象
object SpatialFileRDD extends IConfigurable with Logging {
  /**
   * A spatial partition points to part of a file and might contain spatial information about it
   * @param index the index of the partition within its parent RDD
   * @param path the path to the file
   * @param offset the start offset within the file
   * @param length number of bytes in the file
   * @param locations the machines that contain this part of the file
   * @param mbr the minimum bounding rectangle or an infinite rectangle that encloses the data in this partition
   * @param numRecords the number of records in this partition or -1 if unknown
   */
  case class SpatialFilePartition(index: Int, path: String, offset: Long, length: Long, locations: Array[String],
                                  mbr: EnvelopeNDLite, numRecords: Long) extends Partition {
    override def equals(other: Any): Boolean = {
      other match {
        case p: SpatialFilePartition =>
          this.path.equals(p.path) && this.offset == p.offset && this.length == p.length
        case _ => false
      }
    }
  }

  /** A user-friendly input format string */
  @OperationParam(description =
  """The format of the input file {point(xcol,ycol),envelope(x1col,y1col,x2col,y2col),wkt(gcol)}
    |	point(xcol,ycol) indicates a CSV input where xcol and ycol indicate the indexes of the columns that contain the x and y coordinates
    |	envelope(x1col,y1col,x2col,y2col) indicate an input that contains rectangles stored in (x1,y1,x2,y2) format
    |	wkt(gcol) indicate a CSV file with the field (gcol) containing a WKT-encoded geometry.
    |	shapefile: Esri shapefile. Accepts both .shp+.shx+.dbf files or a compressed .zip file with these three files
    |	rtree: An optimized R-tree index
    |	geojson: GeoJSON file containing features with geometries and properties (attributes)"""
    , required = false) val InputFormat = "iformat"

  /** Early filter the input based on this MBR */
  @OperationParam(description = "An optional MBR to filter the input. Format: x1,y1,x2,y2") val FilterMBR = "filtermbr"

  /** Process the input recursively */
  @OperationParam(description = "Process the input recursively", defaultValue = "false") val Recursive = "recursive"

  /** Whether to split input files or not */
  val SplitFiles = "SpatialInputFormat.SplitFiles"

  /** Minimum split size */
  @OperationParam(description = "Minimum split size", showInUsage = false) val MinSplitSize: String =
  FileInputFormat.SPLIT_MINSIZE

  /** Maximum split size*/
  @OperationParam(description = "Maximum split size", showInUsage = false) val MaxSplitSize: String =
  FileInputFormat.SPLIT_MAXSIZE

  /** A filter that prunes hidden files (files that start with _ or .) */
  val HiddenFileFilter: PathFilter = (p: Path) => p.getName.charAt(0) != '_' && p.getName.charAt(0) != '.'

  /** A filter that finds master files only */
  val MasterFileFilter: PathFilter = (p: Path) => p.getName.startsWith("_master")

  /** Applied duplicate avoidance upon reading the input. Set to true by default */
  val DuplicateAvoidance = "SpatialInputFormat.DuplicateAvoidance"

  /**
   * Add the feature reader class that might have additional parameters
   * @param opts the user-provided options for this class.
   * @param parameterClasses
   */
  override def addDependentClasses(opts: BeastOptions, parameterClasses: util.Stack[Class[_]]): Unit = {
    super.addDependentClasses(opts, parameterClasses)
    if (opts == null) return
    val featureReaderClass = getFeatureReaderClass(null, opts)
    if (featureReaderClass != null)
      parameterClasses.push(featureReaderClass)
  }

  /**
   * Tries to auto-detect the input format from the given path. If an input could be detected, the corresponding
   * parameters for that input are set in BeastOptions and returned. If the input format could not be detected,
   * a `null` is returned. The given set of options are assumed to be correct (given by user) and this function
   * does not try to override them. Any options that is not present in the given options can be overriden by the
   * auto-detect function.
   * @param path the path to either a file or a directory
   * @param opts the options set by user.
   * @return the detected feature reader class and the options that can be used to set that reader or `null` if
   *         the input could not be detected
   */
  def autodetectInputFormat(path: String, opts: BeastOptions): (Class[_ <: FeatureReader], BeastOptions) = {
    // Iterate over all input formats and run the auto-detect function for each of them
    val t1: Long = System.nanoTime()
    val allReaders: Iterator[Class[_ <: FeatureReader]] = FeatureReader.featureReaders.values().iterator().asScala
    var detectedOpts: BeastOptions = null
    var readerClass: Class[_ <: FeatureReader] = null
    while (detectedOpts == null && allReaders.hasNext) {
      readerClass = allReaders.next()
      val reader: FeatureReader = readerClass.newInstance()
      detectedOpts = reader.autoDetect(opts.loadIntoHadoopConf(new Configuration(false)), path)
    }
    val t2: Long = System.nanoTime()
    logWarning(s"Detection took ${(t2-t1)*1E-9} seconds")
    if (detectedOpts != null) {
      // Print out the added parameters
      logWarning("Input format detected. In the future, please add these parameters to your command line to avoid " +
        "running this costly step. " + detectedOpts.toString)
    }
    if (detectedOpts == null)
      null
    else
      (readerClass, detectedOpts)
  }

  /**
   * Returns the path of the master file within the given index if exists.
   * If more than one master file exists, the most recent one is returned.
   *
   * @param fileSystem the file system that contains the index
   * @param indexPath  the path to the index
   * @return the path to the master file if exists. {@code null} otherwise.
   * @throws IOException if an error happens while reading the master file
   */
  def getMasterFilePath(fileSystem: FileSystem, indexPath: Path): Path = {
    val masterFiles = fileSystem.listStatus(indexPath, MasterFileFilter)
    if (masterFiles.length == 0) return null
    var iMostRecent = 0
    for ($i <- 1 until masterFiles.length) {
      if (masterFiles($i).getModificationTime > masterFiles(iMostRecent).getModificationTime)
        iMostRecent = $i
    }
    masterFiles(iMostRecent).getPath
  }


  /**
   * Adds a path to the list of splits by creating the appropriate file splits. If the file is splittable
   * ({@link #isSplitable ( JobContext, Path)}), multiple splits might be added.
   *
   * @param fileSystem  the file system that contains the input
   * @param fileStatus  the status of the input file
   * @param start       the start offset in the file
   * @param length      the length of the part of the file to add
   * @param noSplit     a boolean flag that is set to avoid splitting files
   * @param mbr         the MBR of that partition as obtained from the master file (if exists)
   * @param numRecords  the total number of records as appear in the master file or -1 if unknown
   * @param _partitions (output) the created partitions are added to this list
   */
  private def addPartitions(opts: BeastOptions, fileSystem: FileSystem, fileStatus: FileStatus,
                            start: Long, length: Long, noSplit: Boolean,
                            mbr: EnvelopeNDLite, numRecords: Long,
                            _partitions: mutable.ArrayBuffer[SpatialFilePartition]): Unit = {
    val splitSize = if (noSplit) length else {
      val minSize = opts.getLong(MinSplitSize, 1)
      val maxSize = opts.getLong(MaxSplitSize, Long.MaxValue)
      Math.max(minSize, Math.min(maxSize, fileStatus.getBlockSize))
    }
    val blkLocations = fileSystem.getFileBlockLocations(fileStatus, start, length)
    var partitionStart = start
    val fileEnd = start + length
    val SPLIT_SLOP = 1.1 // 10% slop
    while (partitionStart < fileEnd) {
      val blkIndex = blkLocations.find(bl => partitionStart >= bl.getOffset && partitionStart < bl.getOffset + bl.getLength)
      assert(blkIndex.isDefined, s"No locations found for block at offset $partitionStart")
      val partitionEnd = if ((fileEnd - partitionStart).toDouble / splitSize > SPLIT_SLOP)
        partitionStart + splitSize
      else
        fileEnd
      _partitions.append(SpatialFilePartition(_partitions.length, fileStatus.getPath.toString, partitionStart,
        partitionEnd - partitionStart, blkIndex.get.getHosts, mbr, numRecords))
      partitionStart = partitionEnd
    }
  }

  /**
   * Create all partitions in this RDD for the given input file
   * @return
   */
  def createPartitions(path: String, opts: BeastOptions, conf: Configuration): Array[SpatialFilePartition] = {
    val pathsToInspect = mutable.ArrayBuffer[String](path)
    val recursive: Boolean = opts.getBoolean(Recursive, false)
    val hadoopConf: Configuration = opts.loadIntoHadoopConf(new Configuration(conf))
    val featureReaderClass: Class[_ <: FeatureReader] = getFeatureReaderClass(path, opts)
    val readerMetadata: FeatureReader.Metadata = featureReaderClass.getAnnotation(classOf[FeatureReader.Metadata])
    val filterMBR: EnvelopeNDLite =
      if (opts.contains(SpatialFileRDD.FilterMBR))
        EnvelopeNDLite.decodeString(opts.getString(SpatialFileRDD.FilterMBR), new EnvelopeNDLite())
      else
        null
    val splitFiles: Boolean = opts.getBoolean(SpatialFileRDD.SplitFiles, true) &&
      !readerMetadata.noSplit()
    var fileFilters: Seq[PathFilter] = Seq(SpatialFileRDD.HiddenFileFilter)
    val wildcard = readerMetadata.filter()
    if (wildcard != null && wildcard.nonEmpty) {
      val wildcardFilter = new WildcardFileFilter(wildcard.split("\n"))
      val wildcardPathFilter: PathFilter = p => wildcardFilter.accept(new File(p.getName))
      fileFilters = fileFilters :+ wildcardPathFilter
    }
    // Merge all filters together
    val fileFilter: PathFilter = if (fileFilters.length == 1)
      fileFilters.head
    else // More than one filter, combine them with &&
      p => fileFilters.forall(filter => filter.accept(p))

    val _partitions = mutable.ArrayBuffer[SpatialFilePartition]()
    val fs: FileSystem = new Path(path).getFileSystem(hadoopConf)
    while (pathsToInspect.nonEmpty) {
      val pathToInspect: Path = new Path(pathsToInspect.remove(pathsToInspect.length - 1))
      val fileStatus: FileStatus = fs.getFileStatus(pathToInspect)
      // Check if the file matches the filter. Special case, the given input path should be added without testing
      if (fileStatus.isFile && (fileFilter.accept(fileStatus.getPath) || pathToInspect.toString.equals(path))) {
        // A file that matches the filter, process it
        SpatialFileRDD.addPartitions(opts, fs, fileStatus, 0, fileStatus.getLen, !splitFiles, null, -1, _partitions)
      } else if (fileStatus.isDirectory && (recursive ||  pathToInspect.toString.equals(path))) {
        // A directory and the recursive option is enabled, add all its contents

        val masterFilePath = getMasterFilePath(fs, fileStatus.getPath)
        if (masterFilePath != null) {
          // If a master file exists, use the master file to list the files
          // 1 - To determine the number of dimensions, we read the first line
          val lineReader = new LineReader(fs.open(masterFilePath))
          val headerLine = new Text()
          lineReader.readLine(headerLine)
          lineReader.close()
          val headerParts: Array[String] = headerLine.toString.split("\t")
          val numDimensions: Int = (headerParts.length - headerParts.indexOf("xmin")) / 2
          // Reader the master file
          val masterFileReader = new CSVFeatureReader
          val masterFileOpts = new BeastOptions(opts)
            .setBoolean(CSVFeatureReader.SkipHeader, true)
            .set(SpatialFileRDD.InputFormat, s"envelopek($numDimensions,xmin)")
            .set(CSVFeatureReader.FieldSeparator, "\t")
            .set(CSVFeatureReader.QuoteCharacters, "\'\'\"\"")
          masterFileReader.initialize(masterFilePath, masterFileOpts.loadIntoHadoopConf(new Configuration()))
          try {
            for (spatialPartition <- masterFileReader.iterator().asScala) {
              val partitionPath = new Path(fileStatus.getPath, spatialPartition.getAs("File Name").toString)
              if (fileFilter.accept(partitionPath)) {
                val start: Long = 0
                val length: Long = spatialPartition.getAs("Data Size").toString.toLong
                val numRecords: Long = spatialPartition.getAs("Record Count").toString.toLong
                val mbr: EnvelopeNDLite = new EnvelopeNDLite(new EnvelopeND(FeatureReader.DefaultGeometryFactory).merge(spatialPartition.getGeometry))
                SpatialFileRDD.addPartitions(opts, fs, fs.getFileStatus(partitionPath), start, length, !splitFiles, mbr, numRecords, _partitions)
              }
            }
          } finally {
            masterFileReader.close()
          }
        } else {
          // No master files, list all directory contents
          for (subfile <- fs.listStatus(fileStatus.getPath))
            pathsToInspect.append(subfile.getPath.toString)
        }
      }
    }
    _partitions.toArray
  }


  /**
   * The class of the feature reader to use with this RDD. All partitions use the same feature reader.
   */
  def getFeatureReaderClass(path: String, opts: BeastOptions): Class[_ <: FeatureReader] = {
    val iformat: String = opts.getString(SpatialFileRDD.InputFormat)
    var ifClass: Class[_ <: FeatureReader] = null
    if (iformat == null || iformat.equals("*auto*")) {
      if (path != null) {
        // Auto detect the input
        val detected = SpatialFileRDD.autodetectInputFormat(path, opts)
        require(detected != null, s"Input format could not be detected for input '${path}'")
        ifClass = detected._1
        opts.mergeWith(detected._2)
      }
    } else {
      // Iterate over all input formats and test if the given user-friendly format is supported
      var allReaders: Iterator[Class[_ <: FeatureReader]] = FeatureReader.featureReaders.values().iterator().asScala
      while (ifClass == null && allReaders.hasNext) {
        val readerClass = allReaders.next()
        if (readerClass.newInstance().isRecognized(iformat))
          ifClass = readerClass
      }
      if (ifClass == null) {
        // Input format not recognized. Try to suggest some corrections
        allReaders = FeatureReader.featureReaders.values().iterator().asScala
        var corrections: Array[String] = Array()
        for (readerClass <- allReaders) {
          val c: Array[String] = readerClass.newInstance().iformatCorrections(iformat)
          if (c != null)
            corrections = corrections ++ c
        }
        throw new RuntimeException(s"Input format '$iformat' cannot be recognized. " +
          s"Perhaps you mean one of {${corrections.mkString(",")}}")
      }
    }
    ifClass
  }

  /**
   * Reads the given partition
   * @param partition the partition to read
   * @param featureReaderClass the class of the feature reader
   * @param opts the user options
   * @return an iterator to the features
   */
  def readPartition(partition: SpatialFilePartition, featureReaderClass: Class[_ <: FeatureReader],
                    opts: BeastOptions): Iterator[IFeature] = {
    val featureReader = featureReaderClass.newInstance()
    featureReader.initialize(partition, opts)
    logInfo(s"Processing partition ${partition.path} [${partition.offset},${partition.offset+partition.length})")
    val features = featureReader.iterator().asScala
    if (partition.mbr == null || !opts.getBoolean(DuplicateAvoidance, true))
      features
    else
      features.filter(f => {
        val recordMBR: EnvelopeND = new EnvelopeND(f.getGeometry.getFactory).merge(f.getGeometry)
        var refPointInMBR = true
        var d = 0
        while (d < recordMBR.getCoordinateDimension && refPointInMBR) {
          refPointInMBR = recordMBR.getMinCoord(d) >= partition.mbr.getMinCoord(d) &&
            recordMBR.getMinCoord(d) < partition.mbr.getMaxCoord(d)
          d += 1
        }
        refPointInMBR
      })
  }

  /**
   * Java shortcut to read the features as a Java iterator
   * @param partition
   * @param featureReaderClass
   * @param opts
   * @return
   */
  def readPartitionJ(partition: SpatialFilePartition, featureReaderClass: Class[_ <: FeatureReader],
                     opts: BeastOptions): java.util.Iterator[IFeature] =
    readPartition(partition, featureReaderClass, opts).asJava

  /**
   * Reads the given path locally without creating any RDDs. Useful for reading a small file
   * when SparkContext is not accessible, e.g., inside a mapPartition function.
   * @param path path to a single file or a directory
   * @param iformat the format of the data
   * @param opts additional options for reading the file
   * @return an iterator to features in the given path
   */
  def readLocal(path: String, iformat: String, opts: BeastOptions, conf: Configuration): Iterator[IFeature] = {
    val beastOpts =
      if (opts != null) new BeastOptions(opts).set(SpatialFileRDD.InputFormat, iformat)
      else new BeastOptions(SpatialFileRDD.InputFormat -> iformat)
    val readerClass = getFeatureReaderClass(path, beastOpts)
    val partitions = createPartitions(path, beastOpts, conf)
    new Iterator[IFeature] {
      var currentPartition: Int = 0

      var currentIterator: Iterator[IFeature] = SpatialFileRDD.readPartition(partitions(currentPartition), readerClass, beastOpts)

      override def hasNext: Boolean = currentIterator.hasNext || currentPartition < partitions.length - 1

      override def next(): IFeature = {
        if (!currentIterator.hasNext) {
          if (currentPartition < partitions.length - 1) {
            currentPartition += 1
            currentIterator = SpatialFileRDD.readPartition(partitions(currentPartition), readerClass, beastOpts)
          }
        }
        assert(currentIterator.hasNext)
        currentIterator.next()
      }
    }
  }

  /**
   * (Java version) Reads the given path locally without creating any RDDs. Useful for reading a small file
   * when SparkContext is not accessible, e.g., inside a mapPartition function.
   * @param path path to a single file or a directory
   * @param format the format of the data
   * @param opts additional options for reading the file
   * @param conf configuration used to create the file system to read the input
   * @return an iterator to features in the given path
   */
  def readLocalJ(path: String, format: String, opts: BeastOptions, conf: Configuration): java.util.Iterator[IFeature] =
    readLocal(path, format, opts, conf).asJava
}
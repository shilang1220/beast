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
package edu.ucr.cs.bdlab.beast.indexing;

import edu.ucr.cs.bdlab.beast.cg.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.io.FeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Vector;

/**
 * A class that writes an index as a set of files, each representing one partition. The key-value pairs represent
 * a partition ID and a feature. All features belonging to one partition are stored in one file. The format of the file
 * can be configured through the parameter {@link SpatialOutputFormat#OutputFormat}.
 */
public class IndexRecordWriter extends RecordWriter<Integer, IFeature> {
  /**The CSV head line of the master file*/
  public static final String CSVHeader = String.join("\t", "ID", "File Name",
      "Record Count", "Data Size", "Geometry");

  private static final Log LOG = LogFactory.getLog(IndexRecordWriter.class);

  /**Maximum number of active closing threads*/
  private static final int MaxClosingThreads = Runtime.getRuntime().availableProcessors() * 2;

  /**The class for a writer that writes the contents of each partition file*/
  protected Class<? extends FeatureWriter> writerClass;

  /**The metadata of the writer class*/
  private final FeatureWriter.Metadata writerClassMetadata;

  /**To continuously report the progress and avoid killing the job*/
  private Progressable progress;

  /**Job configuration*/
  private Configuration conf;

  /**The spatial partitioner used by the current job*/
  private SpatialPartitioner partitioner;

  /**The output file system*/
  private FileSystem outFS;

  /**The path to the directory where output files are written*/
  private Path outPath;

  /**A list of all threads that are closing partitions in the background*/
  private Vector<Thread> closingThreads = new Vector<Thread>();

  /**The master file contains information about all written partitions*/
  private OutputStream masterFile;

  /**List of errors that happened by background threads that close the partitions*/
  private Vector<Throwable> listOfErrors = new Vector<Throwable>();

  /**Whether records are replicated in the index to keep the partitions disjoint*/
  private boolean disjoint;
  
  /**The current partition ID which is being written. null means there is no partition being written*/
  private Integer currentPartitionID = null;
  
  /**Parameters for writing features to a partition*/
  private Path partitionPath = null;
  private FeatureWriter writer = null;
  private Summary partition;

  public IndexRecordWriter(TaskAttemptContext task, Path outPath)
      throws IOException {
    this(task, Integer.toString(task.getTaskAttemptID().getTaskID().getId()), outPath);
  }

  public IndexRecordWriter(TaskAttemptContext task, String name, Path outPath)
      throws IOException {
    // 从hadoop临时文件中，读取分区信息，
    this(IndexHelper.readPartitionerFromHadoopConfiguration(task.getConfiguration()), name, outPath, task.getConfiguration());
    this.progress = task;
  }

  /**
   * Create a record writer for index files
   * 为索引文件创建一个记录输出器
   * @param partitioner the partitioner used to partition records into files
   * @param name A unique name added to the global index file which is used
   *             to prevent multiple reducers from writing separate files with
   *             the same name.
   * @param outPath the path of the output
   * @param conf the configuration of the enviornment
   * @throws IOException if an error happens while creating the master file.
   */
  public IndexRecordWriter(SpatialPartitioner partitioner, String name, Path outPath, Configuration conf)
          throws IOException {
    this.conf = conf;
    this.disjoint = partitioner.isDisjoint();
    this.outFS = outPath.getFileSystem(conf);
    this.outPath = outPath;
    this.partitioner = partitioner;

    String globalIndexExtension = partitioner.getClass().getAnnotation(SpatialPartitioner.Metadata.class).extension();

    Path masterFilePath = name == null ?
        new Path(outPath, String.format("_master.%s", globalIndexExtension)) :
        new Path(outPath, String.format("_master_%s.%s", name, globalIndexExtension));

    this.masterFile = outFS.create(masterFilePath);
    writerClass = SpatialOutputFormat.getConfiguredFeatureWriterClass(conf);

    // Get writer class metadata if it is defined
    this.writerClassMetadata = writerClass.getAnnotation(FeatureWriter.Metadata.class);
  }

  /**
   * Initialize parameters for writing features to a partition
   * @param partitionID
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private void initializePartition(Integer partitionID) throws IOException, InstantiationException, IllegalAccessException {
	  partitionPath = getPartitionPath(partitionID);
	  writer = writerClass.newInstance();
	  writer.initialize(partitionPath, conf);
	  partition = new Summary();
	  partition.setCoordinateDimension(partitioner.getCoordinateDimension());
  }
  
  /**
   * Write a feature to current opening partition
   * @param f
   */
  private void writeFeature(IFeature f) {
    partition.incrementNumFeatures(1);
    partition.merge(f.getGeometry());
    try {
      writer.write(null, f);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error writing the feature '%s'", f.toString()), e);
    }
  }

  /**
   * Writes the given feature in the given partitionID. If the given partition ID is already open, the given feature
   * is appended to it. Otherwise, the current open partition is closed and a new file is created to the given
   * partitionID.
   * @param partitionID the ID of the partition to write to
   * @param f the featre to write into the given partition
   * @throws IOException if an error happens while writing the feature.
   */
  @Override
  public void write(Integer partitionID, IFeature f) throws IOException {
	  try {
		  if (currentPartitionID == null) {
			  currentPartitionID = partitionID;
			  initializePartition(currentPartitionID);
			  writeFeature(f);
		  } else if (partitionID.equals(currentPartitionID)) {
        writeFeature(f);
      } else {
        // Close current partition
        this.closePartition(partitionPath, currentPartitionID, partition, writer);

        // Initialize new partition to write
        currentPartitionID = partitionID;
        initializePartition(currentPartitionID);
        writeFeature(f);
      }
	  } catch (IllegalAccessException e) {
	    throw new IOException("Error writing to the output", e);
    } catch (InstantiationException e) {
      throw new IOException("Error writing to the output", e);
    }
  }

  /**
   * Closes a file that is currently open for a specific partition. Returns a background thread that will continue
   * all close-related logic.
   * @param id the partition ID to close
   *
   */
  private void closePartition(final Path partitionPath, final int id, final Summary partition, final FeatureWriter writer) {
    Thread closeThread = new Thread(() -> {
      try {
        writer.close(null);
        partition.setSize(partitionPath.getFileSystem(conf).getFileStatus(partitionPath).getLen());
        if (disjoint) {
          // If data is replicated, we need to shrink down the size of the partition to keep partitions disjoint
          EnvelopeNDLite partitionMBR = new EnvelopeNDLite();
          partitioner.getPartitionMBR(id, partitionMBR);
          partition.shrink(partitionMBR);
        }
        StringBuilder partitionText = getPartitionAsText(id, partitionPath.getName(), partition);
        synchronized (masterFile) {
          // Write partition information to the master file
          masterFile.write(partitionText.toString().getBytes());
          masterFile.write('\n');
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Error closing partition: "+id, e);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        closingThreads.remove(Thread.currentThread());
        // Start more background threads if needed
        int numRunningThreads = 0;
        try {
          for (int i_thread = 0; i_thread < closingThreads.size() &&
              numRunningThreads < MaxClosingThreads; i_thread++) {
            Thread thread = closingThreads.elementAt(i_thread);
            synchronized(thread) {
              switch (thread.getState()) {
                case NEW:
                  // Start the thread and fall through to increment the counter
                  thread.start();
                case RUNNABLE:
                case BLOCKED:
                case WAITING:
                case TIMED_WAITING:
                  // No need to start. Just increment number of threads
                  numRunningThreads++;
                  break;
                case TERMINATED: // Do nothing.
                  // Should never happen as each thread removes itself from
                  // the list before completion
              }
            }
          }
        } catch (ArrayIndexOutOfBoundsException e) {
          // No problem. The array of threads might have gone empty
        }
      }
    });

    closeThread.setUncaughtExceptionHandler((t, e) -> listOfErrors.add(e));

    if (closingThreads.size() < MaxClosingThreads) {
      // Start the thread in the background and make sure it started before
      // adding it to the list of threads to avoid an exception when other
      // thread tries to start it after it is in the queue
      closeThread.start();
      try {
        while (closeThread.getState() == Thread.State.NEW) {
          Thread.sleep(1000);
          LOG.info("Waiting for thread #"+closeThread.getId()+" to start");
        }
      } catch (InterruptedException e) {}
    }
    closingThreads.add(closeThread);
  }

  /**
   * Convert a partition to text in a format that will appear in the master file
   * @param id the ID of the partition
   * @param filename the name of the file
   * @param partition other partition information
   * @return the created text
   */
  public static StringBuilder getPartitionAsText(int id, String filename, Summary partition) {
    StringBuilder partitionText = new StringBuilder();
    partitionText.append(id);
    partitionText.append('\t');
    partitionText.append(filename);
    partitionText.append('\t');
    partitionText.append(partition.numFeatures());
    partitionText.append('\t');
    partitionText.append(partition.size());
    partitionText.append('\t');
    if (partition.getCoordinateDimension() == 2)
      partition.toWKT(partitionText);
    partitionText.append('\t');
    for (int d = 0; d < partition.getCoordinateDimension(); d++) {
      partitionText.append(partition.getMinCoord(d));
      partitionText.append('\t');
    }
    for (int d = 0; d < partition.getCoordinateDimension(); d++) {
      // Avoid appending a tab separator after the last coordinate
      if (d != 0)
        partitionText.append('\t');
      partitionText.append(partition.getMaxCoord(d));
    }
    return partitionText;
  }

  /**
   * Returns a unique name for a file to write the given partition
   * @param id
   * @return
   * @throws IOException
   */
  private Path getPartitionPath(int id) throws IOException {
    String format = "part-%05d";
    if (writerClassMetadata != null)
      format += writerClassMetadata.extension();
    Path partitionPath = new Path(outPath, String.format(format, id));
    if (outFS.exists(partitionPath)) {
      format = "part-%05d-%03d";
      if (writerClassMetadata != null)
        format += writerClassMetadata.extension();
      int i = 0;
      do {
        partitionPath = new Path(outPath, String.format(format, id, ++i));
      } while (outFS.exists(partitionPath));
    }
    return partitionPath;
  }

  @Override
  public void close(TaskAttemptContext task) throws IOException {
	  if (currentPartitionID != null)
	  this.closePartition(partitionPath, currentPartitionID, partition, writer);
    try {
      if (task != null)
        task.setStatus("Closing! "+closingThreads.size()+" remaining");
      // Wait until all background threads are closed
      try {
        while (!closingThreads.isEmpty()) {
          Thread thread;
          synchronized (closingThreads) {
            thread = closingThreads.isEmpty()? null : closingThreads.firstElement();
          }
          while (thread != null && thread.isAlive()) {
            try {
              thread.join(10000);
              if (task != null)
                task.progress();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          if (task != null)
            task.setStatus("Closing! "+closingThreads.size()+" remaining");
          if (thread != null && !thread.isAlive()) {
            synchronized (closingThreads) {
              closingThreads.remove(thread);
            }
          }
        }
      } catch (ArrayIndexOutOfBoundsException NoSuchElementException) {
        // The array of threads has gone empty. Nothing to do
      }
      if (task != null)
        task.setStatus("All closed");
      // All threads are now closed. Check if errors happened
      if (!listOfErrors.isEmpty()) {
        for (Throwable t : listOfErrors)
          LOG.error("Error in thread", t);
        throw new RuntimeException("Encountered "+listOfErrors.size()+" errors in background thread", listOfErrors.firstElement());
      }
    } finally {
      // Close the master file to ensure there are no open files
      masterFile.close();
    }
  }
}
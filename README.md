# BEAST
BEAST is a system for Big Exploratory Analytics on Spatio-Temporal data. It supports both vector and raster data with
multidimensional data types and index structures. Here is a short list of the most important components of Beast.


- **Input/Output formats**: Beast supports many standard formats including Esri Shapefile, CSV, GeoJSON, KML, KMZ, and GPX.
- **Multidimensional data**: Beast inherently supports multidimensional data, so it can index and process 3D or higher dimensions.
- **Efficient indexes**: Beast adopts new indexes including R*-Grove, the most advanced index for big spatial data.
- **Query processor**: An efficient query processor that supports several queries including spatial join.
- **Easy to use**: You can run Beast without any installation on-top of your existing Spark installation.
- **Scalable visualization**: Visualize terabytes of data on an interactive map. Beast powers [UCR-Star](https://star.cs.ucr.edu) the home of terabytes of public geospatial data.
- **Raptor = Raster + Vector**: Beast hosts a novel component for processing raster and vector data concurrently. It is orders of magnitude faster than competetive systems.

## Interface
Beast provides two main interfaces a Scala/Java-based API for developers, and a command-line-interface for quick access.

### Scala/Java API

If you are starting a new Maven project, use the following command to create a new sample project that includes
Java and Scala examples.
```shell
mvn archetype:generate -DgroupId=com.example.beastExample -DartifactId=beast-project \
    -DarchetypeGroupId=edu.ucr.cs.bdlab -DarchetypeArtifactId=beast-spark -DarchetypeVersion=0.8.3
```

If you already have a Maven project, add the following dependency to it.
```xml
<dependency>
  <groupId>edu.ucr.cs.bdlab</groupId>
  <artifactId>beast-spark</artifactId>
  <version>0.8.3</version>
</dependency>
```

In your Scala program, add the following import to your code.
```scala
import edu.ucr.cs.bdlab.beast._
```

After that, assuming that your created SparkContext object is named `sparkContext` you can access the following functions:
* Load a Shapefile, `states = sparkContext.shapefile("states.shp")` or `sc.shapefile("states.zip")`
* Load a GeoJSON file, `counties = sparkContext.geojson("counties.geojson")`
* Load a CSV file with WKT geometry, `parks = sparkContext.readWKTFile(filename = "parks.wkt", wktColumn = 1, delimiter = '\t', skipHeader = true)`
* Perform a simple range query, `ca_parks = parks.rangeQuery(states.filter(_ => _.getAttributeValue("NAME") == "California")`
* Save geometries to a Shapefile, `ca_parks.saveAsShapefile("ca_parks.shp")`
* Perform a spatial join between two datasets, `state_parks = states.spatialJoin(parks)`
* Add a new attribute to features and save the results.
```scala
state_parks.map(sp => {
  val state = sp._1
  val park = sp._2
  val result = new Feature(park)
  result.appendAttribute("state_name", state.getAttributeValue("NAME"))
}).saveAsKML("state_parks.kml")

```

### Command Line Interface (CLI)

The easiest way to use Beast on Spark is through the command line interface (CLI).
No setup is needed to access CLI. As a shortcut, you can setup a new command `beast` as a shorthand
to the CLI main function. Just add the following line to your `~/.bashrc` or `~/.zshrc` and open a new shell window.

```shell
alias beast="spark-submit --packages edu.ucr.cs.bdlab:beast-spark:0.8.3 --class edu.ucr.cs.bdlab.beast.sparkOperations.Main ."
```

From now on, you can run access Beast by just typing `beast` in your command line.
You can check some
[alternative methods](https://bitbucket.org/eldawy/beast-examples/src/master/doc/beast-cmd.md)
to access Beast CLI.

### CLI Examples

#### Summary

The `summary` command processes an input file and produces some statistics that summarize the input.
The summary includes the minimum bounding rectangle (MBR), number of features (records) in the file,
number of points in these features, and the average side length for the features (zero for points).
For example, the following command produces a summary for the `STATE` dataset by the US Census Bureau.
You can download the file at the following link [ftp://ftp2.census.gov/geo/tiger/TIGER2019/STATE/]. 

```shell
beast summary tl_2019_us_state.zip iformat:shapefile
```

The output will contain the following lines with the summary in JSON format.
```json
{
  "extent" : [ -179.231086, -14.601813, 179.859681, 71.439786 ],
  "size" : 14872296,
  "num_features" : 56,
  "num_points" : 928607,
  "avg_sidelength" : [ 12.188805160714292, 4.276105553571429 ],
  "geometry_type" : "MULTIPOLYGON",
  "attributes" : [ {
    "name" : "REGION",
    "type" : "string"
  },
  ...
  {
    "name" : "INTPTLON",
    "type" : "string"
  } ]
```

#### Index

The `index` operation spatially partitions a file while keeping spatial locality in these partitions.
For example, the following command indexes the `STATE` file using an R*-Grove index. 

```shell
beast index tl_2019_us_state.zip iformat:shapefile states_index gindex:rsgrove oformat:rtree
```

The output will be stored in a directory named `states_index`
The available indexes include `zcurve`, `grid`, `rsgrove`, `rstreebb`, and `kdtree`.
The command `beast index` will display the usage of the `index` command including the list
of available indexes.

##### Additional parameters for indexing

- *pcriterion:size(128m)*: The criterion that will be used to determine the total number of partitions. There are three
types of supported criteria, `fixed`, `size`, and `count`. `Fixed(n)` means that a fixed number of partitions will be created.
`Size(s)` means that each partition will be adjusted to have the given size in bytes.
`Count(c)` means that each partition will be adjusted to have the given number of records.
Notice that the three criteria are meant to be only suggestions and the command might choose
to create fewer or more partitions depending on the logic.
- *-balanced*: When this flag is added, some partitioning techniques are able to provide more balanced partitions by using a histogram of the input.

#### Visualization

The following command visualizes a file into a single-level image.

```shell
beast plot tl_2019_us_state.zip iformat:shapefile states.png plotter:gplot
```

##### Additional parameters for single-level plot.

- *width:1000*: The width of the generated image in pixels.
- *height:1000*: The height of the generated image in pixels.
- *-keepratio*: Makes the aspect ratio of the image similar to the aspect ratio of the input.
- *-mercator*: If the input data is in world coordinates (i.e., longitude and latitude) this option will convert it on
the fly to the web-mercator projection.
- *-vflip*: Flips the image vertically.
- *-merge|-no-merge*: By default, this option is set to true which merges all the partial images into one final image.
If set to false, i.e., `-no-merge`, the output will contain several partial images. An associated `index.kml` file will
be produced to display the images

The following command visualizes a file into a multilevel image that contains only images.

```shell
beast mplot tl_2019_us_state.zip iformat:shapefile states_plot plotter:gplot threshold:0
```

The generated visualization can be displayed by opening the file `states_plot/index.html` in your browser.

The next command generates an adaptive multilevel image that contains images and data.

```shell
beast mplot tl_2019_us_state.zip iformat:shapefile states_plot plotter:gplot threshold:1000
```

To visualize the adaptive multilevel image, you first need to start the visualization server using the following command

```shell
beast server port:10000
```

After this, you should navigate to the following address in your browser `http://localhost:10000/dynamic/visualize.cgi/states_plot/`.

Finally, to build an optimized multilevel plot, you need to first create an index for the input, and then visualize
the indexed data using `mplot` with the parameter `-no-data-tiles` added. See the following example which contains three
commands.

```shell
beast index tl_2019_us_state.zip iformat:shapefile states_rtree gindex:rsgrove oformat:rtree
beast mplot states_rtree iformat:rtree states_plot plotter:gplot -no-data-tiles
beast vizserver port:10000
```

After that, you can display the visualization by navigating to the address `http://localhost:10000/dynamic/visualize.cgi/point_plot/`
in your web browser while the server is running.

##### Additional parameters for multilevel plot

- *levels:7*: Number of zoom levels to generate in the image. The zoom levels start at zero. So, setting number of levels
to seven, for example, will generate zoom levels zero through six.
- *threshold:1000*: The threshold for classifying tiles as image tiles or data tiles for adaptive indexes. In particular,
the threshold is the largest number of records in a data tile.
- *-data-tiles*: If set to false, i.e., `-no-data-tiles`, no data tiles will be written to the output. Instead, a link
to the original input will be added so that images for non-generated tiles can be produced on the fly.

##### Additional parameters for the geometric plotter

- *stroke:black*: The color for the stroke used to draw the point, lines, and the outer boundary of envelopes and polygons.
- *fill:black*: The color of the fill for envelopes and polygons. This parameter is not used when plotting points and lines.

#### File Conversion

You can use the command `cat` to convert a file between two formats.
The following example converts a shapefile into a GeoJSON file.

```shell
beast cat tl_2018_us_state.zip iformat:shapefile states.geojson oformat:geojson
```

#### Histogram

This command can compute a simple uniform histogram for an input file. The histogram lays a uniform grid and counts
the total number of records that have centers in each grid cell.

```shell
beast histogram tl_2018_us_state.zip iformat:shapefile histogram.bin
```

#### Help with CLI

To get additional help with the command-line interface, you can use the following commands.

This command lists all the available operations that you can run.

```shell
beast
```

This command prints more details on the usage of a specific operation, e.g., index.

```shell
beast index
```

The more information you provide in the command line, the more details you might get. For example, the following command
will provide information on how to set the stroke and fill colors which are specific to the geometric plotter.

```shell
beast plot plotter:gplot
```

#### Common Parameters

The following are some common parameters that are supported for most operations.

*-overwrite* - Overwrites the output file or directory if it already exists. Without it, the command will fail if the
output directory already exists.

*-recursive* - Processes all subdirectories and files recursively in each input path.

*iformat:\*auto\** - Runs a slightly time consuming step that tries to automatically detect the input format.
This step should be used only for testing if you do not want to bother adding all the input details. As it runs,
the log will display the details fo the auto-detected input format so that you can manually set it in the future.

*oformat:xxx* - For operations that produces geometric output, the parameter `oformat` can be used to specify a different
format than the input. If not specified, the input format is also used as an output format.

*filtermbr:x1,y1,x2,y2* - This parameter early prunes the records that do not overlap the given MBR. If the input is
indexed, the filtering can be quite efficient as it can prune entire blocks that do not match the input. Otherwise,
it will only be able to prune the records after they are retrieved from disk.

### Additional Tutorials

You can find additional tutorials and examples on the [Beast-examples](https://bitbucket.org/eldawy/beast-examples) 
repository and its [Documentation page](https://bitbucket.org/eldawy/beast-examples/src/master/doc/).

### Compilation

If you would like to compile the source code, you need JDK and Maven installed on your machine.
You might also need git to clone the repository.

To compile and build a Spark-runnable JAR run the following command.

```shell
mvn package -DskipTests -Puberjar
```

This will produce a JAR file under `target/beast-uber-spark-<version>.jar`.

To run the JAR file, run `spark-submit target/beast-uber-spark-*.jar`

## Questions

If you have any questions about any of the packages or modules of beast,
please ask your question on [StackOverflow](https://stackoverflow.com/)
and tag your questions with `#ucrbeast` and `#beast`. Beast team members
follow these tags and will respond to those questions as soon as possible.

Issues
------

If you found any bugs or have any recommended features to add, please
[open a ticket](https://bitbucket.org/eldawy/beast/issues/new)
on the project homepage.
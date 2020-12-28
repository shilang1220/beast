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
package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.beast.util.FileUtil;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

public class FileUtilTest extends TestCase {

  public void testRelativize() {
    Path path = new Path("path/to/directory");
    Path refPath = new Path("path/to/another");

    Path relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("../directory"), relativePath);

    // Try with a common prefix in the name
    path = new Path("long_directory_name");
    refPath = new Path("longer_directory_name/");

    relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("../long_directory_name"), relativePath);

    // Try with a subdirectory
    refPath = new Path("basepath/");
    path = new Path(refPath, "subdir");

    relativePath = FileUtil.relativize(path, refPath);
    assertEquals(new Path("subdir"), relativePath);

    // Test equal paths
    relativePath = FileUtil.relativize(new Path("basepath/"), new Path("basepath/"));
    assertEquals(new Path("."), relativePath);
  }
}
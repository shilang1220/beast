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

import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.{Calendar, GregorianCalendar}


/**
 * A helper for CSV features
 */
object CSVHelper {

  def encodeValue(value: Any, str: java.lang.StringBuilder): java.lang.StringBuilder = value match {
    case null => str // Do nothing
    case timestamp: Calendar => str.append(timestamp.toInstant)
    case other => str.append(other.toString)
  }
}

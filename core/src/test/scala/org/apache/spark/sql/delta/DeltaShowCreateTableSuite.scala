/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

import scala.collection.JavaConverters._

class DeltaShowCreateTableSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test(testName="assert simple managed table show create table") {
    withTempDir { foo =>
      val fooPath = foo.getCanonicalPath()
      sql(s"CREATE TABLE `assert_managed_table` (id LONG) USING delta LOCATION '$fooPath'")
      val df = sql(s"SHOW CREATE TABLE assert_managed_table")
      df.show(false)
      assert(io.delta.tables.DeltaTable.isDeltaTable(fooPath))
    }
  }
}

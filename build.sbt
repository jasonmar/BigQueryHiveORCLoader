/*
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

name := "bigquery-hive-orc-loader"

version := "0.1.0"

scalaVersion := "2.11.11"

val exHadoop = ExclusionRule("org.apache.hadoop")
val exGuava = ExclusionRule("com.google.guava")

libraryDependencies ++= Seq(
	"org.apache.hive" % "hive-metastore" % "1.2.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.7",
	"com.google.cloud" % "google-cloud-bigquery" % "1.69.0" excludeAll exGuava,
	"com.google.apis" % "google-api-services-bigquery" % "v2-rev431-1.25.0" excludeAll exGuava,
	"com.google.code.gson" % "gson" % "2.8.5",
  "com.google.protobuf" % "protobuf-java" % "3.6.1",
  "com.google.guava" % "guava" % "27.1-jre",
  "com.github.scopt" %% "scopt" % "3.7.1"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

mainClass in assembly := Some("com.google.cloud.example.BQHiveLoader")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "s.guava.@1").inAll,
  ShadeRule.rename("com.google.protobuf.*" -> "s.proto.@1").inAll
)

/*
 * Copyright 2019 Google LLC
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

package com.google.example

import com.google.example.MetaStore.Partition

object PartitionFilters {
  sealed trait FilterExpression {
    def apply(column: String, value: String): Boolean
    def reject(column: String, value: String): Boolean = !apply(column, value)
  }

  case class Equals(l: String, r: String) extends FilterExpression {
    override def apply(column: String, value: String): Boolean = {
      l == column && (r == "*" || r == value)
    }
  }

  case class GreaterThan(l: String, r: String) extends FilterExpression {
    override def apply(column: String, value: String): Boolean = {
      l == column && r > value
    }
  }

  case class LessThan(l: String, r: String) extends FilterExpression {
    override def apply(column: String, value: String): Boolean = {
      l == column && r < value
    }
  }

  case class GreaterThanOrEq(l: String, r: String) extends FilterExpression {
    override def apply(column: String, value: String): Boolean = {
      if (isDate(r))
        l == column && r <= value
      else
        l == column && r >= value
    }
  }

  def isDate(s: String): Boolean = {
    if ((s.length <= 10 && s.length >= 8) && s.count(_ == '-') == 2) true
    else false
  }

  case class LessThanOrEq(l: String, r: String) extends FilterExpression {
    override def apply(column: String, value: String): Boolean = {
      if (isDate(r))
        l == column && r >= value
      else
        l == column && r <= value
    }
  }

  case class In(l: String, r: Set[String]) extends FilterExpression {
    override def apply(column: String, value: String): Boolean = {
      l == column && r.contains(value)
    }
  }

  def parse(expr: String): Map[String,Seq[FilterExpression]] = {
    if (expr.nonEmpty){
      expr.replaceAllLiterally(" and ", " AND ")
        .replaceAllLiterally(" And ", " AND ")
        .replaceAllLiterally(" in ", " IN ")
        .replaceAllLiterally(" In ", " IN ")
        .split(" AND ")
        .flatMap(parseExpression)
        .groupBy(_._1)
        .mapValues{_.map{_._2}.toSeq}
    } else Map.empty
  }

  def parseExpression(expr: String): Option[(String,FilterExpression)] = {
    if (expr.contains('=')) {
      expr.split('=').map(_.trim) match {
        case Array(l,r) => Option((l, Equals(l, r)))
        case _ => None
      }
    } else if (expr.contains("<=")) {
      expr.split("<=").map(_.trim) match {
        case Array(l,r) => Option((l, LessThanOrEq(l, r)))
        case _ => None
      }
    } else if (expr.contains(">=")) {
      expr.split(">=").map(_.trim) match {
        case Array(l,r) => Option((l, GreaterThanOrEq(l, r)))
        case _ => None
      }
    } else if (expr.contains('<')) {
      expr.split('<').map(_.trim) match {
        case Array(l,r) => Option((l, LessThan(l, r)))
        case _ => None
      }
    } else if (expr.contains('>')) {
      expr.split('>').map(_.trim) match {
        case Array(l,r) => Option((l, GreaterThan(l, r)))
        case _ => None
      }
    } else if (expr.contains(" IN ")) {
      expr.split(" IN ").map(_.trim) match {
        case Array(l,r) =>
          val set = r.stripPrefix("(").stripSuffix(")")
            .split(',').map(_.trim).toSet
          Option((l, In(l, set)))
        case _ => None
      }
    } else None
  }

  def filter(partition: Partition, filters: Map[String,Seq[FilterExpression]]): Boolean = {
    for ((col, value) <- partition.values) {
      filters.get(col) match {
          case Some(filter) if filter.exists(_.reject(col, value)) =>
            return false
          case _ =>
        }
    }
    true
  }
}

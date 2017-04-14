/**
 * Copyright 2011-2017 Green Energy Corp.
 *
 * Licensed to Green Energy Corp (www.greenenergycorp.com) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. Green Energy
 * Corp licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.greenbus.edge.data.mapping

trait ReaderContext {
  def context: String
  def field(name: String): ReaderContext
  def structField(tag: String, name: String): ReaderContext
}

sealed trait ContextEntry
case class RootCtx(tag: String) extends ContextEntry {
  override def toString: String = {
    "object " + tag
  }
}
case class FieldCtx(name: String) extends ContextEntry {
  override def toString: String = {
    "." + name + " / "
  }
}
case class StructFieldCtx(tag: String, name: String) extends ContextEntry {
  override def toString: String = {
    s".($tag).$name"
  }
}

case class SimpleReaderContext(stack: Vector[ContextEntry]) extends ReaderContext {
  def context: String = {
    stack.mkString("")
  }

  def field(name: String): ReaderContext = {
    SimpleReaderContext(stack :+ FieldCtx(name))
  }

  def structField(tag: String, name: String): ReaderContext = {
    SimpleReaderContext(stack :+ StructFieldCtx(tag, name))
  }
}
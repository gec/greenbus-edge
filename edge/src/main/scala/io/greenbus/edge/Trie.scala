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
package io.greenbus.edge

import io.greenbus.edge.Trie.TrieIterator

import scala.collection.immutable.{ Map, MapLike }

object Trie {

  def empty[Part, B]: Trie[Part, B] = new Trie[Part, B](None, Map())

  case class Node[Part, +B](valueOpt: Option[B], subSegments: Map[Part, Trie.Segment[Part, B]])
  case class Segment[Part, +B](segment: Iterable[Part], node: Node[Part, B])

  def segmentDiff[P](a: Iterable[P], b: Iterable[P]): (Iterable[P], Iterable[P], Iterable[P]) = {
    var i = 0
    val aItr = a.toIterator
    val bItr = b.toIterator
    var continue = true
    while (continue && aItr.hasNext && bItr.hasNext) {
      if (aItr.next() != bItr.next()) {
        continue = false
      } else {
        i += 1
      }
    }

    val (matched, aRemain) = a.splitAt(i)
    val (_, bRemain) = b.splitAt(i)

    (matched, aRemain, bRemain)
  }

  def recursiveGet[Part, B](postfix: Iterable[Part], tree: Map[Part, Trie.Segment[Part, B]]): Option[B] = {
    tree.get(postfix.head).flatMap { segment =>
      val (matched, searchRemains, treeSegRemains) = Trie.segmentDiff(postfix, segment.segment)

      if (searchRemains.isEmpty && treeSegRemains.isEmpty) {
        segment.node.valueOpt
      } else if (treeSegRemains.isEmpty) {
        recursiveGet(searchRemains, segment.node.subSegments)
      } else {
        None
      }
    }
  }

  def subTreeSearch[Part, B](stack: Iterable[Part], postfix: Iterable[Part], tree: Map[Part, Trie.Segment[Part, B]]): Seq[(Iterable[Part], B)] = {
    tree.get(postfix.head) match {
      case None => Seq()
      case Some(segment) =>
        val (matched, searchRemains, treeSegRemains) = Trie.segmentDiff(postfix, segment.segment)

        if (searchRemains.isEmpty && treeSegRemains.isEmpty) {
          iterateFromNode(stack ++ segment.segment, segment.node)
        } else if (treeSegRemains.isEmpty) {
          subTreeSearch(stack ++ matched, searchRemains, segment.node.subSegments)
        } else if (searchRemains.isEmpty) {
          iterateFromNode(stack ++ segment.segment, segment.node)
        } else {
          Seq()
        }
    }
  }

  def iterateFromNode[Part, B](stack: Iterable[Part], node: Node[Part, B]): Seq[(Iterable[Part], B)] = {
    node.valueOpt.map(v => Seq((stack, v))).getOrElse(Seq()) ++
      node.subSegments.flatMap {
        case (_, seg) => iterateFromNode(stack ++ seg.segment, seg.node)
      }
  }

  private def recurseAdd[Part, B, B1 >: B](v: B1, postfix: Iterable[Part], tree: Map[Part, Trie.Segment[Part, B]]): Map[Part, Trie.Segment[Part, B1]] = {
    tree.get(postfix.head) match {
      case None =>
        val added = Trie.Segment[Part, B1](postfix, Trie.Node(Some(v), Map()))
        tree + (postfix.head -> added)

      case Some(segment) => {

        val (matched, searchRemains, treeSegRemains) = Trie.segmentDiff(postfix, segment.segment)

        if (searchRemains.isEmpty && treeSegRemains.isEmpty) {

          val updated = segment.copy(node = segment.node.copy(valueOpt = Some(v)))
          tree + (postfix.head -> updated)

        } else if (treeSegRemains.isEmpty) {

          val updatedNodeTree = recurseAdd(v, searchRemains, segment.node.subSegments)
          val updatedNode = segment.node.copy(subSegments = updatedNodeTree)
          tree.updated(postfix.head, segment.copy(node = updatedNode))

        } else if (searchRemains.isEmpty) {

          val existSeg = Trie.Segment[Part, B1](treeSegRemains, segment.node)
          val matchedNode = Trie.Node(Some(v), Map(treeSegRemains.head -> existSeg))
          val matchedSeg = Trie.Segment(matched, matchedNode)
          tree.updated(postfix.head, matchedSeg)

        } else {

          val existSeg = Trie.Segment[Part, B1](treeSegRemains, segment.node)
          val addedSeg = Trie.Segment[Part, B1](searchRemains, Trie.Node(Some(v), Map()))
          val emptySplitNode = Trie.Node(None, Map(treeSegRemains.head -> existSeg, searchRemains.head -> addedSeg))
          val segToSplit = Trie.Segment(matched, emptySplitNode)
          tree.updated(postfix.head, segToSplit)
        }
      }
    }
  }

  private def recurseRemove[Part, B](postfix: Iterable[Part], tree: Map[Part, Trie.Segment[Part, B]]): Map[Part, Trie.Segment[Part, B]] = {
    tree.get(postfix.head) match {
      case None => tree
      case Some(segment) => {

        val (_, searchRemains, treeSegRemains) = Trie.segmentDiff(postfix, segment.segment)

        if (searchRemains.isEmpty && treeSegRemains.isEmpty) {
          if (segment.node.subSegments.isEmpty) {
            tree - postfix.head
          } else {
            tree.updated(postfix.head, segment.copy(node = segment.node.copy(valueOpt = None)))
          }
        } else if (treeSegRemains.isEmpty) {

          val updatedSegSubtree = recurseRemove(searchRemains, segment.node.subSegments)
          if (segment.node.valueOpt.isEmpty && updatedSegSubtree.isEmpty) {
            tree - postfix.head
          } else {
            tree.updated(postfix.head, segment.copy(node = segment.node.copy(subSegments = updatedSegSubtree)))
          }
        } else {
          tree
        }
      }
    }
  }

  class TrieIterator[Part, B](stack: Iterable[Part], valueOpt: Option[B], subSegments: Map[Part, Trie.Segment[Part, B]]) extends Iterator[(Iterable[Part], B)] {
    private var producedSelf = false
    private lazy val subIters: Iterator[(Iterable[Part], B)] = {
      subSegments.valuesIterator.flatMap { segment =>
        new TrieIterator(stack ++ segment.segment, segment.node.valueOpt, segment.node.subSegments)
      }
    }

    def hasNext: Boolean = {
      if (!producedSelf && valueOpt.nonEmpty) {
        true
      } else {
        subIters.hasNext
      }
    }

    def next(): (Iterable[Part], B) = {
      (producedSelf, valueOpt) match {
        case (false, Some(v)) =>
          producedSelf = true
          (stack, v)
        case (false, None) =>
          producedSelf = true
          subIters.next()
        case (true, _) =>
          subIters.next()
      }
    }
  }
}

class Trie[Part, +B] private (emptyPathOpt: Option[B], subSegments: Map[Part, Trie.Segment[Part, B]])
    extends Map[Iterable[Part], B]
    with MapLike[Iterable[Part], B, Trie[Part, B]] {

  def get(key: Iterable[Part]): Option[B] = {
    if (key.isEmpty) {
      emptyPathOpt
    } else {
      Trie.recursiveGet(key, subSegments)
    }
  }

  def matchingPrefix(key: Iterable[Part]): Seq[(Iterable[Part], B)] = {
    if (key.isEmpty) {
      Trie.iterateFromNode(Iterable.empty[Part], Trie.Node(emptyPathOpt, subSegments))
    } else {
      Trie.subTreeSearch(Iterable.empty[Part], key, subSegments)
    }
  }

  def +[B1 >: B](kv: (Iterable[Part], B1)): Trie[Part, B1] = {
    val (key, v) = kv
    if (key.isEmpty) {
      new Trie[Part, B1](Some(v), subSegments)
    } else {
      new Trie(emptyPathOpt, Trie.recurseAdd(v, key, subSegments))
    }
  }

  def -(key: Iterable[Part]): Trie[Part, B] = {
    if (key.isEmpty) {
      new Trie[Part, B](None, subSegments)
    } else {
      new Trie(emptyPathOpt, Trie.recurseRemove(key, subSegments))
    }
  }

  override def empty: Trie[Part, B] = new Trie[Part, B](None, Map())

  def iterator: Iterator[(Iterable[Part], B)] = new TrieIterator(Iterable.empty[Part], emptyPathOpt, subSegments)
}

package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.collection.{BiMultiMap, MapToUniqueValues}

class SynthesizerTable extends LazyLogging {
  private var routed = Map.empty[TypeValue, Map[TableRow, DbMgr]]
  private var unrouted = Map.empty[TableRow, DbMgr]
  private var sourceToRow = BiMultiMap.empty[PeerSourceLink, RowId] //Map.empty[PeerSourceLink, RowId]

  def handleBatch(sourceLink: PeerSourceLink, events: Seq[StreamEvent]): Seq[StreamEvent] = {
    val results = Vector.newBuilder[StreamEvent]

    events.foreach {
      case ev: RowAppendEvent => {
        val tableRow = ev.rowId.tableRow
        ev.rowId.routingKeyOpt match {
          case None => {
            // TODO: What does this even mean???
            /*unrouted.get(tableRow) match {
              case None => {
                ev.appendEvent match {
                  case resync: ResyncSession =>
                    val (db, events) = DbMgr.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot)
                    unrouted += (tableRow -> db)
                    results ++= events.map(v => RowAppendEvent(ev.rowId, v))
                  case _ =>
                    logger.warn(s"Initial row event was not resync session: $ev")
                }
              }
              case Some(db) => db.append(sourceLink, ev.appendEvent)
            }*/
          }
          case Some(routingKey) =>
            routed.get(routingKey) match {
              case None => {
                results ++= addRouted(sourceLink, ev, routingKey, Map())
              }
              case Some(routingKeyRows) =>
                routingKeyRows.get(tableRow) match {
                  case None => {
                    results ++= addRouted(sourceLink, ev, routingKey, routingKeyRows)
                  }
                  case Some(db) =>
                    db.append(sourceLink, ev.appendEvent)
                }
            }
        }
      }
      case in: RouteDesynced => {
        routed.get(in.routingKey) match {
          case None => Seq()
          case Some(rows) => {
            // TODO: stable sort?
            results ++= rows.toVector.flatMap {
              case (row, db) =>
                val rowId = RowId(Some(in.routingKey), row.table, row.rowKey)
                db.sourceRemoved(sourceLink)
                  .map(append => RowAppendEvent(rowId, append))
            }

            val rowIds = rows.keys.map(tr => RowId(Some(in.routingKey), tr.table, tr.rowKey)).toSet
            sourceToRow = sourceToRow.removeMappings(sourceLink, rowIds)
          }
        }
      }
    }

    results.result()
  }

  def sourceRemoved(sourceLink: PeerSourceLink): Seq[StreamEvent] = {
    val results = sourceToRow.keyToVal.get(sourceLink) match {
      case None => Seq()
      case Some(rowSet) =>
        // TODO: stable sort?
        rowSet.toVector.flatMap { rowId =>
          lookup(rowId)
            .map(db => db.sourceRemoved(sourceLink).map(apEv => RowAppendEvent(rowId, apEv)))
            .getOrElse(Seq())
        }
    }
    sourceToRow = sourceToRow.removeKey(sourceLink)
    results
  }

  private def lookup(rowId: RowId): Option[DbMgr] = {
    rowId.routingKeyOpt match {
      case None => unrouted.get(rowId.tableRow)
      case Some(routingKey) =>
        routed.get(routingKey).flatMap { map =>
          map.get(rowId.tableRow)
        }
    }
  }

  private def addRouted(sourceLink: PeerSourceLink, ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, DbMgr]): Seq[StreamEvent] = {
    ev.appendEvent match {
      case resync: ResyncSession =>
        val (db, events) = DbMgr.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot)
        routed += (routingKey -> (existingRows + (ev.rowId.tableRow -> db)))
        sourceToRow += (sourceLink -> ev.rowId)
        events.map(v => RowAppendEvent(ev.rowId, v))
      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
        Seq()
    }
  }
}


object DbMgr {
  def build(rowId: RowId, initSource: PeerSourceLink, initSess: PeerSessionId, snapshot: SetSnapshot): (DbMgr, Seq[AppendEvent]) = {
    val log = SessionRowLog.build(rowId, initSess, snapshot)
    val (events, db) = log.activate()
    val synthesizer = new RowSynthesizerDb(rowId, initSource, initSess, db)
    (synthesizer, events)
  }
}
trait DbMgr {
  def append(source: PeerSourceLink, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: PeerSourceLink): Seq[AppendEvent]
}

/*
three modes a session can be in:
- active
- standby (not yet active)
- TODO backfill (succeeded, allowing backfill events until a timeout happens or all sources are removed)

"standby" is really "unresolved"? in other words it's not got a clear ordering from the active session

 */
class RowSynthesizerDb(rowId: RowId, initSource: PeerSourceLink, initSess: PeerSessionId, initMgr: SessionRowDb) extends DbMgr with LazyLogging {

  private var activeSession: PeerSessionId = initSess
  private var activeDb: SessionRowDb = initMgr
  //private var activeSources: Set[PeerSourceLink] = Set(initSource)

  //private var standbySessions = Map.empty[PeerSessionId, (SessionModifyRowLog, Set[PeerSourceLink])]
  private var standbySessions = Map.empty[PeerSessionId, SessionRowLog]
  //private var sourceToSession: Map[PeerSourceLink, PeerSessionId] = Map(initSource, initSess)
  //private var sourceMapping: ValueTrackedSetMap[PeerSessionId, PeerSourceLink] = ValueTrackedSetMap((initSess, initSource))
  private var sessionToSources: MapToUniqueValues[PeerSessionId, PeerSourceLink] = MapToUniqueValues((initSess, initSource))
  private def sourceToSession: Map[PeerSourceLink, PeerSessionId] = sessionToSources.valToKey

  def append(source: PeerSourceLink, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case delta: StreamDelta => {
        sourceToSession.get(source) match {
          case None => logger.warn(s"$rowId got delta for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {
              activeDb.delta(delta.update).map(StreamDelta)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.delta(delta.update)
              }
              Seq()
            }
          }
        }
      }
      case resync: ResyncSnapshot => {
        sourceToSession.get(source) match {
          case None => logger.warn(s"$rowId got snapshot for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {
              activeDb.resync(resync.snapshot).map(ResyncSnapshot)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.resync(resync.snapshot)
              }
              Seq()
            }
          }
        }
      }
      case sessionResync: ResyncSession => {

        // TODO: session succession logic by actually reading peer session id
        val prevSessOpt = sourceToSession.get(source)
        sessionToSources = sessionToSources + (sessionResync.sessionId -> source)

        if (sessionResync.sessionId == activeSession) {

          // gc sessions with no sources
          prevSessOpt.foreach { prevSess =>
            if (!sessionToSources.keyToVal.contains(prevSess)) {
              standbySessions -= prevSess
            }
          }

          activeDb.resync(sessionResync.snapshot).map(ResyncSnapshot)
            .map(v => Seq(v)).getOrElse(Seq())

        } else {

          standbySessions.get(sessionResync.sessionId) match {
            case None => {
              val log = SessionRowLog.build(rowId, sessionResync.sessionId, sessionResync.snapshot)
              standbySessions += (sessionResync.sessionId -> log)
            }
            case Some(log) => log.resync(sessionResync.snapshot)
          }

          activeSessionChangeCheck()
        }
      }
    }
  }

  // TODO: need to mark succeeded sessions to prevent immediately reverting to a prev session propped up by a source who is just delayed
  private def activeSessionChangeCheck(): Seq[AppendEvent] = {
    if (!sessionToSources.keyToVal.contains(activeSession)) {

      if (standbySessions.keySet.size == 1) {
        val (nowActiveSession, log) = standbySessions.head
        standbySessions -= nowActiveSession
        activeSession = nowActiveSession
        val (events, db) = log.activate()
        activeDb = db
        events

      } else {
        Seq()
      }

    } else {
      Seq()
    }
  }

  def sourceRemoved(source: PeerSourceLink): Seq[AppendEvent] = {

    val prevSessOpt = sourceToSession.get(source)
    sessionToSources = sessionToSources.remove(source)

    prevSessOpt match {
      case None => Seq()
      case Some(prevSess) =>
        if (prevSess == activeSession) {
          activeSessionChangeCheck()
        } else {
          if (!sessionToSources.keyToVal.contains(prevSess)) {
            standbySessions -= prevSess
          }
          Seq()
        }
    }
  }

}

object SessionRowLog {
  def build(rowId: RowId, sessionId: PeerSessionId, init: SetSnapshot): SessionRowLog = {
    init match {
      case snap: ModifiedSetSnapshot => new SessionModifyRowLog(rowId, sessionId, snap)
    }
  }
}
trait SessionRowLog {
  def activate(): (Seq[AppendEvent], SessionRowDb)
  def delta(delta: SetDelta): Unit
  def resync(snapshot: SetSnapshot): Unit
}

trait SessionRowDb {
  def delta(delta: SetDelta): Option[SetDelta]
  def resync(snapshot: SetSnapshot): Option[SetSnapshot]
}

class SessionModifyRowLog(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot) extends SessionRowLog with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  private var current: Set[TypeValue] = init.snapshot

  def activate(): (Seq[AppendEvent], SessionModifyRowDbMgr) = {
    val snap = ModifiedSetSnapshot(sequence, current)
    (Seq(ResyncSession(sessionId, snap)),
      new SessionModifyRowDbMgr(rowId, sessionId, snap))
  }

  def delta(delta: SetDelta): Unit = {
    delta match {
      case d: ModifiedSetDelta =>
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          current = (current -- d.removes) ++ d.adds
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def resync(snapshot: SetSnapshot): Unit = {
    snapshot match {
      case d: ModifiedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          current = d.snapshot
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}

class SessionModifyRowDbMgr(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot) extends SessionRowDb with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  private var current: Set[TypeValue] = init.snapshot

  def delta(delta: SetDelta): Option[SetDelta] = {
    delta match {
      case d: ModifiedSetDelta => {
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          current = (current -- d.removes) ++ d.adds
          Some(delta)
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def resync(snapshot: SetSnapshot): Option[SetSnapshot] = {
    snapshot match {
      case d: ModifiedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          current = d.snapshot
          Some(snapshot)
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}
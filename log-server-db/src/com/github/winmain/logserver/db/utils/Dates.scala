package com.github.winmain.logserver.db.utils

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, Temporal}
import java.util.Date

object Dates {dates=>
  import scala.language.implicitConversions

  /**
   * Number of milliseconds in a standard second.
   */
  val SECOND: Long = 1000
  /**
   * Number of milliseconds in a standard minute.
   */
  val MINUTE: Long = 60 * SECOND
  /**
   * Number of milliseconds in a standard hour.
   */
  val HOUR: Long = 60 * MINUTE
  /**
   * Number of milliseconds in a standard day.
   */
  val DAY: Long = 24 * HOUR
  val DAY_DOUBLE: Double = DAY.toDouble

  /**
   * Сдвиг Москвы GMT+3 в миллисекундах
   */
  val GMT_MOSCOW: Long = 3 * HOUR

  /**
   * Сдвиг Москвы GMT+3 в секундах
   */
  val GMT_MOSCOW_SECONDS: Long = GMT_MOSCOW / 1000

  val ZONE_ID: ZoneId = ZoneId.systemDefault()

  val httpDateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss O")
  val httpDateFormatGmt: DateTimeFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
  def httpDateFormatMillis(millis: Long): String = httpDateFormatGmt.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
  def httpDateFormatLDT(v: LocalDateTime): String = httpDateFormatGmt.format(v.atOffset(ZoneOffset.UTC))

  def daysBetween(t0: Temporal, t1: Temporal): Int = ChronoUnit.DAYS.between(t0, t1).toInt
  def monthsBetween(t0: Temporal, t1: Temporal): Int = ChronoUnit.MONTHS.between(t0, t1).toInt
  def yearsBetween(t0: Temporal, t1: Temporal): Int = ChronoUnit.YEARS.between(t0, t1).toInt

  def getMskDay(millis: Long): Long = (millis + GMT_MOSCOW) / DAY
  def getMskDayDouble(millis: Long): Double = (millis + GMT_MOSCOW) / DAY_DOUBLE
  def mskDayMidnight(millis: Long): Long = (millis + GMT_MOSCOW) / DAY

  def nowTimestamp: Timestamp = new Timestamp(System.currentTimeMillis())

  def toInstant(v: LocalDateTime): Instant = v.atZone(ZONE_ID).toInstant
  def toInstant(v: LocalDate): Instant = v.atStartOfDay(ZONE_ID).toInstant
  def toInstant(millis: Long): Instant = Instant.ofEpochMilli(millis)

  def toTimestamp(v: LocalDateTime): Timestamp = Timestamp.from(toInstant(v))
  def toTimestamp(v: LocalDate): Timestamp = Timestamp.from(toInstant(v))

  def toDate(v: LocalDateTime): Date = Date.from(toInstant(v))
  def toDate(v: LocalDate): Date = Date.from(toInstant(v))

  def toMillis(v: LocalDateTime): Long = toInstant(v).toEpochMilli
  def toMillis(v: LocalDate): Long = toInstant(v).toEpochMilli

  def toLocalDateTime(v: Date): LocalDateTime = toLocalDateTime(v.getTime)
  def toLocalDateTime(millis: Long): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZONE_ID)

  def toLocalDate(v: Date): LocalDate = toLocalDate(v.getTime)
  def toLocalDate(millis: Long): LocalDate = Instant.ofEpochMilli(millis).atZone(ZONE_ID).toLocalDate

  def toOffsetDateTime(millis: Long): OffsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZONE_ID)

  def toZonedDateTime(v: LocalDateTime): ZonedDateTime = ZonedDateTime.of(v, ZONE_ID)
  def toZonedDateTime(v: LocalDate): ZonedDateTime = ZonedDateTime.of(v, LocalTime.MIDNIGHT, ZONE_ID)

  implicit def _toLocalDateTimeWrapper(v: LocalDateTime): LocalDateTimeWrapper = new LocalDateTimeWrapper(v)
  implicit def _toLocalDateWrapper(v: LocalDate): LocalDateWrapper = new LocalDateWrapper(v)

  implicit def dateWrapper(v: LocalDate): LocalDateWrapper = new LocalDateWrapper(v)
  implicit def dateWrapper(v: LocalDateTime): LocalDateTimeWrapper = new LocalDateTimeWrapper(v)
  implicit def dateWrapper(v: DateTimeFormatter): DateTimeFormatterWrapper = new DateTimeFormatterWrapper(v)

  //
  // =================================== LocalDateWrapper Wrapper ===================================
  //
  class LocalDateWrapper(v: LocalDate) {
    def toInstant: Instant = v.atStartOfDay(ZONE_ID).toInstant
    def toTimestamp: Timestamp = Timestamp.from(toInstant)
    def toDate: Date = Date.from(toInstant)
    def toMillis: Long = toInstant.toEpochMilli
    def toLocalDateTime: LocalDateTime = LocalDateTime.of(v, LocalTime.MIDNIGHT)
    def toZonedDateTime: ZonedDateTime = ZonedDateTime.of(v, LocalTime.MIDNIGHT, ZONE_ID)
  }

  //
  // =================================== LocalDateTimeWrapper Wrapper ===================================
  //
  class LocalDateTimeWrapper(v: LocalDateTime) {
    def toInstant: Instant = v.atZone(ZONE_ID).toInstant
    def toTimestamp: Timestamp = Timestamp.from(toInstant)
    def toDate: Date = Date.from(toInstant)
    def toMillis: Long = toInstant.toEpochMilli
    def toZonedDateTime: ZonedDateTime = ZonedDateTime.of(v, ZONE_ID)
  }

  //
  // =================================== LocalDateTimeWrapper Wrapper ===================================
  //
  class DateTimeFormatterWrapper(v: DateTimeFormatter) {
    def parseLocalDate(text: CharSequence): LocalDate = LocalDate.parse(text, v)
    def parseLocalDateTime(text: CharSequence): LocalDateTime = LocalDateTime.parse(text, v)
  }
}

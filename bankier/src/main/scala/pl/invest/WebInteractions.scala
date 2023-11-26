package pl.invest

import io.circe.*
import io.circe.parser.*

import java.net.URL
import java.sql.DriverManager
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.io.Source
import scala.util.Using

object WebInteractions {
  def main(args: Array[String]): Unit = {

    Array("PKOBP", "PZU", "PKNORLEN", "KGHM", "PEKAO", "PGE").foreach { ticker =>
      connectToHttpServerAndReadResponse(s"https://www.bankier.pl/new-charts/get-data?symbol=${ticker}&intraday=true&today=true&type=area")
    }

    initializeDatabaseFromFile(args(0))

  }

  def connectToHttpServerAndReadResponse(address: String): Unit = {
    val url = new URL(address)
    val connection = url.openConnection()
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setConnectTimeout(5000) // 5 seconds
    connection.setReadTimeout(5000) // 5 seconds
    connection.connect()
    val content = Source.fromInputStream(connection.getInputStream).mkString
    // Map content to case class
    val json = parse(content)
  }

  private case class Trade(number: String, ticker: String, side: Char, price: BigDecimal, amount: Int, currency: String,
                           chamber: String, realizationTime: LocalDateTime, settledDate: LocalDate,
                           value: BigDecimal, fees: BigDecimal, valueWithFees: BigDecimal)

  def initializeDatabaseFromFile(filePath: String): Unit = {
    Using(Source.fromResource(filePath)) { source =>
      val content = source.mkString
      //2302846579	S	MRB	PL	200	7.92 PLN	24-07-2023 09:11:22	25-07-2023	1 584.00	6.18	1 577.82

      val DATABASE_URL: String = s"jdbc:h2:mem:invest;DB_CLOSE_DELAY=-1"
      val connection = DriverManager.getConnection(DATABASE_URL, "sa", "")
      val statement = connection.createStatement()

      statement.executeUpdate("CREATE TABLE trades (number VARCHAR(255), ticker VARCHAR(255), side CHAR(1), price DECIMAL(10,2), amount INT, currency VARCHAR(255), chamber VARCHAR(255), realizationTime TIMESTAMP, settledDate DATE, value DECIMAL(10,2), fees DECIMAL(10,2), valueWithFees DECIMAL(10,2))")

      content.split("\n").map(_.split("\t")).map {
        case Array("Numer zlecenia", _, _, _, _, _, _, _, _, _, _) => println("Skipping header"); None
        case Array(number: String, side: String, ticker: String, chamber: String, v1: String,
        priceAndCurrency: String, dateTime: String, date: String, v2: String, v3: String, v4: String) =>
          val price = priceAndCurrency.substring(0, priceAndCurrency.indexOf(" "))
          val currency = priceAndCurrency.substring(priceAndCurrency.indexOf(" ") + 1)
          val amount = v1.replace(" ", "")
          val value = v2.replace(" ", "")
          val fees = v3.replace(" ", "")
          val valueWithFees = v4.replace(" ", "")

          println(s"number: $number, side: $side, ticker: $ticker, chamber: $chamber, amount: $amount, " +
            s"price: $price, currency: $currency, dateTime: $dateTime, date: $date, value: $value, fees: $fees, " +
            s"valueWithFees: $valueWithFees")

          Some(Trade(number, ticker, side.charAt(0), BigDecimal(price), amount.toInt, currency, chamber,
            LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
            LocalDate.parse(date, DateTimeFormatter.ofPattern("dd-MM-yyyy")),
            BigDecimal(value), BigDecimal(fees), BigDecimal(valueWithFees)))
      }.filter(_.isDefined).map(_.get).foreach { trade =>
        println(trade)
        statement.executeUpdate(s"INSERT INTO trades VALUES (" +
          s"'${trade.number}', '${trade.ticker}', '${trade.side}', ${trade.price}, ${trade.amount}, '${trade.currency}', " +
          s"'${trade.chamber}', '${trade.realizationTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}', " +
          s"'${trade.settledDate.format(DateTimeFormatter.ISO_DATE)}', ${trade.value}, ${trade.fees}, " +
          s"${trade.valueWithFees})")
      }
    }.toEither match {
      case Left(exception) => println(exception); exception.printStackTrace()
      case Right(_) => println("Database initialized")
    }
  }
}

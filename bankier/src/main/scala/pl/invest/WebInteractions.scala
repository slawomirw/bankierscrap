package pl.invest

import io.circe.*
import io.circe.Json.JArray
import io.circe.parser.*
import scalaj.http.{Http, HttpOptions}

import java.net.http.HttpClient.{Redirect, Version}
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.net.{Authenticator, URI, URL}
import java.sql.{Connection, DriverManager}
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate, LocalDateTime, Period}
import java.util.function.Consumer
import scala.io.{BufferedSource, Source}
import scala.math.Fractional.Implicits.infixFractionalOps
import scala.util.Using

private case class Trade(number: String, ticker: String, side: Char, price: BigDecimal, amount: Int, currency: String,
                         chamber: String, realizationTime: LocalDateTime, settledDate: LocalDate,
                         sharesValue: BigDecimal, fees: BigDecimal, sharesValueWithFees: BigDecimal)

class WebInteractions {

}

object WebInteractions {

  private val currencyExchangeMap = Map(
    "WSE" -> "PLN",
    "NSQ" -> "USD"
  )

  def main(args: Array[String]): Unit = {

    //Array("PKOBP", "PZU", "PKNORLEN", "KGHM", "PEKAO", "PGE").foreach { ticker =>
    //  connectToHttpServerAndReadResponse(s"https://www.bankier.pl/new-charts/get-data?symbol=${ticker}&intraday=true&today=true&type=area")
    //}

    initializeDbTrades(args(0))
    initializeDbPrices()

    inDb("Selecting from database")(connection => {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT SUM(amount) amount, side FROM trades WHERE chamber='PL' AND ticker='IZS' GROUP BY side")
      while resultSet.next() do {
        println(s"${resultSet.getString("amount")}, ${resultSet.getString("side")}")
        //        println(s"${resultSet.getString("number")}, ${resultSet.getString("ticker")}, ${resultSet.getString("side")}, " +
        //          s"${resultSet.getBigDecimal("price")}, ${resultSet.getInt("amount")}, ${resultSet.getString("currency")}, " +
        //          s"${resultSet.getString("chamber")}, ${resultSet.getTimestamp("realizationTime")}, " +
        //          s"${resultSet.getDate("settledDate")}, ${resultSet.getBigDecimal("value")}, ${resultSet.getBigDecimal("fees")}, " +
        //          s"${resultSet.getBigDecimal("valueWithFees")}")
      }
    })
  }

  def getUrlContent(url: URL) = {
    val source = Source.fromURL(url)
    val content = source.mkString
    source.close()
    content
  }

  private def initializeDbTrades(filePath: String): Unit = {
    Using(Source.fromResource(filePath)) { source =>
      val content = source.mkString

      inDb("Database initialized")(connection => {
        val statement = connection.createStatement()

        statement.executeUpdate("CREATE TABLE trades (" +
          "number VARCHAR(255), ticker VARCHAR(255), side CHAR(1), price DECIMAL(10,2), amount INT, currency VARCHAR(255), " +
          "chamber VARCHAR(255), realizationTime TIMESTAMP, settledDate DATE, sharesValue DECIMAL(10,2), fees DECIMAL(10,2), " +
          "sharesValueWithFees DECIMAL(10,2))")

        content.split("\n").map(_.split("\t")).map {
          case Array("Numer zlecenia", _, _, _, _, _, _, _, _, _, _) => println("Skipping header"); None
          case Array(number: String, side: String, ticker: String, chamber: String, v1: String,
          priceAndCurrency: String, dateTime: String, date: String, v2: String, v3: String, v4: String) =>
            val price = priceAndCurrency.substring(0, priceAndCurrency.indexOf(" "))
            val currency = priceAndCurrency.substring(priceAndCurrency.indexOf(" ") + 1)
            val amount = v1.replace(" ", "")
            val sharesValue = v2.replace(" ", "")
            val fees = v3.replace(" ", "")
            val sharesValueWithFees = v4.replace(" ", "")

            println(s"number: $number, side: $side, ticker: $ticker, chamber: $chamber, amount: $amount, " +
              s"price: $price, currency: $currency, dateTime: $dateTime, date: $date, sharesValue: $sharesValue, fees: $fees, " +
              s"sharesValueWithFees: $sharesValueWithFees")

            Some(Trade(number, ticker, side.charAt(0), BigDecimal(price), amount.toInt, currency, chamber,
              LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")),
              LocalDate.parse(date, DateTimeFormatter.ofPattern("dd-MM-yyyy")),
              BigDecimal(sharesValue), BigDecimal(fees), BigDecimal(sharesValueWithFees)))
        }.filter(_.isDefined).map(_.get).foreach { trade =>
          statement.executeUpdate(s"INSERT INTO trades VALUES (" +
            s"'${trade.number}', '${trade.ticker}', '${trade.side}', ${trade.price}, ${trade.amount}, '${trade.currency}', " +
            s"'${trade.chamber}', '${trade.realizationTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}', " +
            s"'${trade.settledDate.format(DateTimeFormatter.ISO_DATE)}', ${trade.sharesValue}, ${trade.fees}, " +
            s"${trade.sharesValueWithFees})")
        }
      })
    }.toEither match {
      case Left(exception) => println(exception); exception.printStackTrace()
      case Right(_) => println("Transactions file processed.")
    }
  }

  private def initializeDbPrices(): Unit = {

    inDb("Dates and shares read")(connection => {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT DISTINCT ticker, settledDate, chamber FROM trades ORDER BY settledDate")
      val tickersAndDates = new Iterator[(String, LocalDate, String)] {
        def hasNext: Boolean = resultSet.next()

        def next(): (String, LocalDate, String) = (resultSet.getString("ticker"), resultSet.getDate("settledDate").toLocalDate, resultSet.getString("chamber"))
      }.to(LazyList)

      inDb("Store shares prices")(connection => {
        val statement = connection.createStatement()
        val (tradingDates, prices) = tickersAndDates.map {
          case (ticker, date, "PL") => (s"$ticker:WSE", date)
          case (ticker, date, "US") => (s"$ticker:NSQ", date)
        }.foldLeft(Map.empty[String, LocalDate]) {
          case (acc, (ticker, date)) if !acc.contains(ticker) => acc.updated(ticker, date)
          case (acc, _) => acc
        }.foldLeft((Seq[LocalDate](), Map.empty[String, Seq[(LocalDate, BigDecimal)]])) {
          case ((Seq(), prices), (ticker, date)) =>
            val tradingDates = getTradingDates(ticker, date)
            (tradingDates, prices ++ getPricesFTOf(ticker, tradingDates))
          case ((tradingDates, prices), (ticker, date)) => (tradingDates, prices ++ getPricesFTOf(ticker, tradingDates))
        }

        statement.executeUpdate("CREATE TABLE prices (ticker VARCHAR(255), currency CHAR(3), date DATE, close DECIMAL(10,2), volume INT)")

        prices.foreach {
          case Array(ticker: String, details: Seq[(LocalDate, BigDecimal)]) =>
            val equity = ticker.substring(0, ticker.indexOf(":"))
            val currency = currencyExchangeMap(ticker.substring(ticker.indexOf(":") + 1))
            details.foreach {
              case (date, close) =>
                statement.executeUpdate(s"INSERT INTO prices VALUES (" +
                  s"'$equity', '$currency', '${date.format(DateTimeFormatter.ISO_DATE)}', $close, -1)")
            }
        }
      })
    })
  }

  def getFTIdentifier(ticker: String): String = {
    val url = new URL(s"https://markets.ft.com/data/equities/tearsheet/summary?s=$ticker")
    val content = getUrlContent(url)
    val section = content.substring(content.indexOf("mod-tearsheet-add-alert") + 23, content.indexOf("</section>"))
      .replaceAll("\\&quot;", "'")

    section.substring(
      section.indexOf("'issueId'\\:") + 10,
      section.indexOf(",", section.indexOf("'issueId'\\:") + 10)
    ).replaceAll("[,:']", "")
  }

  def getPricesFTOf(symbol: String, tradingDates: Seq[LocalDate]): Map[String, Seq[(LocalDate, BigDecimal)]] = {
    val days = Period.between(tradingDates.head, LocalDate.now()).getDays

    val request = HttpRequest.newBuilder
      .uri(URI.create(s"https://markets.ft.com/data/chartapi/series"))
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .POST(HttpRequest.BodyPublishers.ofString(
        s"""{
           |    "days": $days,
           |    "dataNormalized": false,
           |    "dataPeriod": "Day",
           |    "dataInterval": 1,
           |    "realtime": false,
           |    "yFormat": "0.###",
           |    "timeServiceFormat": "JSON",
           |    "rulerIntradayStart": 26,
           |    "rulerIntradayStop": 3,
           |    "rulerInterdayStart": 10957,
           |    "rulerInterdayStop": 365,
           |    "returnDateType": "ISO8601",
           |    "elements": [
           |        {
           |            "Label": "fca4745c",
           |            "Type": "price",
           |            "Symbol": "$symbol",
           |            "OverlayIndicators": [],
           |            "Params": {}
           |        }
           |    ]
           |}""".stripMargin))
      .build

    val response: String = connectPostToHttpServerAndReadResponse(request)

    parse(response) match {
      case Left(dates) => Map.empty[String, Seq[(LocalDate, BigDecimal)]]
      case Right(ftSharePriceJson) =>
        val cursor: HCursor = ftSharePriceJson.hcursor
        val dates = cursor.downField("Dates").as[Seq[String]].getOrElse(Seq.empty[String])
        val prices = cursor.downField("Elements").downArray.downField("DataSeries").downArray.downField("values").as[Seq[BigDecimal]].getOrElse(Seq.empty[BigDecimal])
        Map(symbol -> dates.zip(prices).map {
          case (date, price) => (LocalDate.parse(date, DateTimeFormatter.ISO_DATE), price)
        })
    }
  }

  def getTradingDates(ticker: String, start: LocalDate) = {
    // Map content to case class
    val body = connectToHttpServerAndReadResponse(s"https://markets.ft.com/data/equities/tearsheet/summary?s=$ticker")
    val json = parse(body)
    Seq[LocalDate]()
  }

  protected def connectPostToHttpServerAndReadResponse(request: HttpRequest) = {
    val client = HttpClient.newBuilder
      .version(Version.HTTP_2)
      .followRedirects(Redirect.NORMAL)
      .connectTimeout(Duration.ofSeconds(10))
      .authenticator(Authenticator.getDefault)
      .build
    val response = client.send(request, BodyHandlers.ofString).body()
    response
  }

  protected def connectToHttpServerAndReadResponse(address: String): String = {
    val url = new URL(address)
    val connection = url.openConnection()
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setConnectTimeout(5000) // 5 seconds
    connection.setReadTimeout(5000) // 5 seconds
    connection.connect()
    Source.fromInputStream(connection.getInputStream).mkString
  }


  private def inDb(operationName: String)(operation: Consumer[Connection]): Unit = {
    val DATABASE_URL: String = s"jdbc:h2:mem:invest;DB_CLOSE_DELAY=-1"
    Using(DriverManager.getConnection(DATABASE_URL, "sa", "")) { connection =>
      operation.accept(connection);
    }.toEither match
      case Left(exception) => println(exception); exception.printStackTrace()
      case Right(_) => println(s"$operationName finished")
  }
}
package pl.invest

import io.circe.*
import io.circe.parser.*
import org.h2.util.StringUtils

import java.net.http.HttpClient.{Redirect, Version}
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.net.{URI, URL}
import java.sql.{Connection, DriverManager}
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate, LocalDateTime, Period}
import java.util.function.Consumer
import scala.collection.immutable.LazyList
import scala.io.Source
import scala.util.Using

private case class Trade(number: String, ticker: String, side: Char, price: BigDecimal, amount: Int, currency: String,
                         chamber: String, realizationTime: LocalDateTime, settledDate: LocalDate,
                         sharesValue: BigDecimal, fees: BigDecimal, sharesValueWithFees: BigDecimal)

class WebInteractions {

}

object WebInteractions {

  private val currencyExchangeMap = Map(
    "WSE" -> "PLN",
    "NSQ" -> "USD",
    "NYQ" -> "USD"
  )

  private val exchanges = Map("US" -> Map(
    "WMT" -> "NYQ",
    "UBER" -> "NYQ",
    "DIS" -> "NYQ"
  ).withDefault { * => "NSQ" },
    "PL" -> Map().withDefault { * => "WSE" }
  )

  private var inactiveShares = Set.empty[String]

  def main(args: Array[String]): Unit = {
    initializeInactiveShares(args(1))
    initializeDbTrades(args(0))
    initializeDbPrices()
    initializeFxRates("USD", "PLN")
  }

  private def initializeFxRates(from: String, to: String): Unit = {
    getFTFxIdentifier(s"$from$to").foreach { symbol =>

      val tickersAndDates: List[(String, LocalDate, String, BigDecimal)] = tickerTradeDates
      val startDate = tickersAndDates.head._2

      inDb("Fx rates initialized")(connection => {
        val statement = connection.createStatement()
        statement.executeUpdate("CREATE TABLE fx_rates (fromCcy CHAR(3), toCcy CHAR(3), date DATE, rate DECIMAL(10,2))")
        getFxRatesFTOf(symbol, startDate).map { content =>
          println(s"currency pair: $from$to, date: ${content._1}, rate: ${content._2}")
          val sqlInsert = s"INSERT INTO fx_rates VALUES (" +
            s"'$from', '$to', '${content._1.format(DateTimeFormatter.ISO_DATE)}', ${content._2})"
          statement.executeUpdate(sqlInsert)
        }
      })
    }
  }

  private def initializeInactiveShares(filePath: String): Unit = {
    Using(Source.fromResource(filePath)) { source =>
      val content = source.mkString
      inactiveShares = content.split("\n").toSet
    }.toEither match {
      case Left(exception) => println(exception); exception.printStackTrace()
      case Right(_) => println("Inactive shares file processed")
    }
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
      case Right(_) => println("Transactions file processed")
    }
  }

  private def initializeDbPrices(): Unit = {

    val tickersAndDates: List[(String, LocalDate, String, BigDecimal)] = tickerTradeDates

    val startDate = tickersAndDates.head._2
    inDb("Store shares prices")(connection => {
      val statement = connection.createStatement()
      val prices = tickersAndDates.map {
        case (ticker, _, market, startPrice) => (s"$ticker:${exchanges(market)(ticker)}", startPrice)
      }.foldLeft(Map.empty[String, BigDecimal]) {
        case (acc, (ticker, startPrice)) if !acc.contains(ticker) => acc.updated(ticker, startPrice)
        case (acc, _) => acc
      }.foldLeft(Map.empty[String, Seq[(LocalDate, BigDecimal)]]) {
        case (prices, (ticker, _)) if !inactive(ticker) =>
          getFTIdentifier(ticker) match {
            case Some(ftIdentifier) => prices ++ getPricesFTOf(ftIdentifier, ticker, startDate)
            case None => throw new RuntimeException(s"Ticker's $ticker FT id not found")
          }
        case (prices, (ticker, startPrice)) =>
          prices.find(_._2.nonEmpty).map {
            case (_, details) =>
              prices ++ Map(ticker -> details.map(d => (d._1, startPrice)))
          }.getOrElse(prices)
      }

      statement.executeUpdate("CREATE TABLE prices (ticker VARCHAR(255), currency CHAR(3), date DATE, close DECIMAL(10,2), volume INT)")

      prices.foreachEntry {
        case (ticker, details) =>
          val equity = ticker.substring(0, ticker.indexOf(":"))
          val currency = currencyExchangeMap(ticker.substring(ticker.indexOf(":") + 1))
          details.foreach {
            case (date, close) =>
              val sqlInsert = s"INSERT INTO prices VALUES (" +
                s"'$equity', '$currency', '${date.format(DateTimeFormatter.ISO_DATE)}', $close, -1)"

              println(s"Inserting price: $sqlInsert")

              statement.executeUpdate(sqlInsert)
          }
      }
    })
  }

  private def tickerTradeDates: List[(String, LocalDate, String, BigDecimal)] = {
    var tickersAndTradeDates: List[(String, LocalDate, String, BigDecimal)] = List.empty
    inDb("Dates and shares read")(connection => {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT DISTINCT ticker, realizationTime, chamber, price FROM trades ORDER BY realizationTime ASC")
      tickersAndTradeDates = new Iterator[(String, LocalDate, String, BigDecimal)] {
        def hasNext: Boolean = resultSet.next()

        def next(): (String, LocalDate, String, BigDecimal) = (
          resultSet.getString("ticker"),
          resultSet.getTimestamp("realizationTime").toLocalDateTime.toLocalDate,
          resultSet.getString("chamber"),
          resultSet.getBigDecimal("price")
        )
      }.to(LazyList).toList
    })
    tickersAndTradeDates
  }

  private def inactive(ticker: String): Boolean = inactiveShares.contains(ticker)

  private def getFTIdentifier(ticker: String): Option[String] = {
    print(s"Ticker: $ticker ...")
    val url = new URL(s"https://markets.ft.com/data/equities/tearsheet/summary?s=$ticker")
    val content = getUrlContent(url)
    val section = content.substring(content.indexOf("mod-tearsheet-add-alert") + 23,
        content.indexOf("</section>", content.indexOf("mod-tearsheet-add-alert") + 23))
      .replaceAll("&quot;", "'")

    Option(section.substring(
      section.indexOf("'issueId':") + "'issueId':".length,
      section.indexOf(",", section.indexOf("'issueId':") + "'issueId':".length - 1)
    )).filter(_.nonEmpty).map(_.replaceAll("'", "")).filter(v => StringUtils.isNumber(v)) match
      case Some(ftIdentifier) => Some(ftIdentifier)
      case None =>
        println(s"FT identifier not found for ticker $ticker: \n$content")
        None
  }

  private def getFTFxIdentifier(ticker: String): Option[String] = {
    print(s"Fx pair: $ticker ...")
    val url = new URL(s"https://markets.ft.com/data/currencies/tearsheet/summary?s=$ticker")
    val content = getUrlContent(url)
    val divIndex = content.indexOf("<div data-module-name=\"SymbolChartApp\"") + "data-module-name=\"SymbolChartApp\"".length
    val section = content.substring(divIndex, content.indexOf(">", divIndex + 1)).replaceAll("&quot;", "'")
    val issueTagValue = section.indexOf("'issueID':") + "'issueID':".length

    Option(section.substring(issueTagValue, section.indexOf(",", issueTagValue - 1)))
      .filter(_.nonEmpty).map(_.replaceAll("'", "")).filter(v => StringUtils.isNumber(v)) match
      case Some(ftIdentifier) => Some(ftIdentifier)
      case None =>
        println(s"FT Fx identifier not found for ticker $ticker: \n$content")
        None

  }

  private def getPricesFTOf(symbol: String, ticker: String, startDate: LocalDate): Map[String, Seq[(LocalDate, BigDecimal)]] = {
    print(s" $symbol ...")
    val period = Period.between(startDate, LocalDate.now())
    val days = period.getDays + period.getMonths * 30 + period.getYears * 365

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
      case Left(_) => Map.empty[String, Seq[(LocalDate, BigDecimal)]]
      case Right(ftSharePriceJson) =>
        val cursor: HCursor = ftSharePriceJson.hcursor
        val dates = cursor.downField("Dates").as[Seq[String]].getOrElse(Seq.empty[String])
        val prices = cursor.downField("Elements").downN(0).downField("ComponentSeries").downN(3).downField("Values").as[Seq[BigDecimal]].getOrElse(Seq.empty[BigDecimal])
        println(s" $ticker: ${dates.length} dates, ${prices.length} prices")
        Map(ticker -> dates.zip(prices).map {
          case (date, price) => (LocalDate.parse(date.substring(0, 10), DateTimeFormatter.ISO_DATE), price)
        })
    }
  }

  private def getFxRatesFTOf(symbol: String, startDate: LocalDate): Seq[(LocalDate, BigDecimal)] = {
    print(s" $symbol ...")
    val period = Period.between(startDate, LocalDate.now())
    val days = period.getDays + period.getMonths * 30 + period.getYears * 365

    val request = HttpRequest.newBuilder
      .uri(URI.create(s"https://markets.ft.com/data/chartapi/series"))
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .POST(HttpRequest.BodyPublishers.ofString(
        s"""{
           |  "days": $days,
           |  "dataNormalized": false,
           |  "dataPeriod": "Day",
           |  "dataInterval": 1,
           |  "realtime": false,
           |  "yFormat": "0.###",
           |  "timeServiceFormat": "JSON",
           |  "rulerIntradayStart": 26,
           |  "rulerIntradayStop": 3,
           |  "rulerInterdayStart": 10957,
           |  "rulerInterdayStop": 365,
           |  "returnDateType": "ISO8601",
           |  "elements": [
           |    {
           |      "Label": "62d3d51b",
           |      "Type": "price",
           |      "Symbol": "$symbol",
           |      "OverlayIndicators": [],
           |      "Params": {}
           |    }
           |  ]
           |}""".stripMargin))
      .build

    val response: String = connectPostToHttpServerAndReadResponse(request)

    parse(response) match {
      case Left(_) => Seq[(LocalDate, BigDecimal)]()
      case Right(ftSharePriceJson) =>
        val cursor: HCursor = ftSharePriceJson.hcursor
        val dates = cursor.downField("Dates").as[Seq[String]].getOrElse(Seq.empty[String])
        val prices = cursor.downField("Elements").downN(0).downField("ComponentSeries").downN(3).downField("Values").as[Seq[BigDecimal]].getOrElse(Seq.empty[BigDecimal])
        val ccy = s"${cursor.downField("Elements").downN(0).downField("Currency")}"
        println(s" $ccy: ${dates.length} dates, ${prices.length} prices")
        dates.zip(prices).map {
          case (date, price) => (LocalDate.parse(date.substring(0, 10), DateTimeFormatter.ISO_DATE), price)
        }
    }
  }

  private def connectPostToHttpServerAndReadResponse(request: HttpRequest): String = {
    val client = HttpClient.newBuilder
      .version(Version.HTTP_2)
      .followRedirects(Redirect.NORMAL)
      .connectTimeout(Duration.ofSeconds(10))
      .build
    val response = client.send(request, BodyHandlers.ofString).body()
    response
  }

  private def inDb(operationName: String)(operation: Consumer[Connection]): Unit = {
    val DATABASE_URL: String = s"jdbc:h2:mem:invest;DB_CLOSE_DELAY=-1"
    Using(DriverManager.getConnection(DATABASE_URL, "sa", "")) { connection =>
      operation.accept(connection);
    }.toEither match
      case Left(exception) => println(exception); exception.printStackTrace()
      case Right(_) => println(s"$operationName finished")
  }

  private def getUrlContent(url: URL): String = {
    val source = Source.fromURL(url)
    val content = source.mkString
    source.close()
    content
  }
}

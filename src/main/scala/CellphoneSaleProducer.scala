import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.LogManager

import java.sql.Timestamp
import java.util.concurrent.ThreadLocalRandom
import java.util.{Date, Properties}

object CellphoneSaleProducer extends AppConfigs {

  private val log = LogManager.getLogger

  def main(args: Array[String]): Unit = {

    val properties = new Properties
    properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, ApplicationID)
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootStrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](properties)
    log.info("delivering data to toCassandra topic")
    for (_ <- 0 until 100) {
      try {
        producer.send(RandomMobiles("Oneplus"), new AsyncCallback)
        Thread.sleep(300)
        producer.send(RandomMobiles("iphone"), new AsyncCallback)
        Thread.sleep(300)
        producer.send(RandomMobiles("Galaxy note S6"), new AsyncCallback)
        Thread.sleep(300)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def RandomMobiles(model: String): ProducerRecord[String, String] = {
      val Mobiles = JsonNodeFactory.instance.objectNode
      val amount = ThreadLocalRandom.current.nextInt(20000, 70000)
      val date = new Date
      Mobiles.put("model", model)
      Mobiles.put("amount", amount)
      Mobiles.put("boughtAt", String.valueOf(new Timestamp(date.getTime)))
      new ProducerRecord[String, String](Topic, Mobiles.toString)
  }

  class AsyncCallback extends Callback {
    override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) println(s"${e.getMessage} in ${metadata.toString}")
    }
  }
}

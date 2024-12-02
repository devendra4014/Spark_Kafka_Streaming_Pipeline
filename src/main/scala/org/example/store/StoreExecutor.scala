package org.example.store

import org.example.config.Output

class StoreExecutor {

  def executor(stores: Array[Output]) : Unit = {
    stores.foreach(store => {
      try {
        if (store.getOutputType.equalsIgnoreCase("kafka")) {
          val kafkaStore = new KafkaStore()
          kafkaStore.execute(store)
        }
        else {

        }
      }
      catch {
        case exception: Exception =>
          println(
            "[Store] [ERROR] Error occurred in Store Name: " + store
              .getDataframeName
              .concat(" And Store Type: ")
              .concat(store.getOutputType)
          )
          exception.printStackTrace()
          throw new RuntimeException(exception)

      }
    } )
  }

}

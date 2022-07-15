package sample;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class Sample implements AutoCloseable {

  private final DistributedTransactionManager manager;

  public Sample() throws IOException {
    // Create a transaction manager object
    TransactionFactory factory =
        new TransactionFactory(new DatabaseConfig(new File("database.properties")));
    manager = factory.getTransactionManager();
  }

  public void loadInitialData() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      setConf();
      loadShopInitialData();
      loadCustomerInitialData();
      loadItemInitialData();
    } catch (TransactionException e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void setConf() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      transaction = manager.start();
      setConfIfNotExists(transaction, 0, 0, 0);
      transaction.commit();
    } catch (TransactionException e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  private void setConfIfNotExists(
      DistributedTransaction transaction,
      int shopnum,
      int itemnum,
      int customernum
      )
      throws TransactionException {
        int confId = 0;
        Optional<Result> conf =
        transaction.get(
            new Get(new Key("conf_id", confId))
                .forNamespace("conf")
                .forTable("conf"));

      if (!conf.isPresent()) {
        transaction.put(
            new Put(new Key("conf_id", confId))
                .withValue("shopnum", shopnum)
                .withValue("itemnum", itemnum)
                .withValue("customernum", customernum)
                .forNamespace("conf")
                .forTable("conf"));
      }
  }

  public void loadShopInitialData() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      transaction = manager.start();
      loadShopIfNotExists(transaction, 1, "Amazones", 0, 0, "amazones");
      loadShopIfNotExists(transaction, 2, "SuperGoogle", 0, 0, "supergoogle");
      loadShopIfNotExists(transaction, 3, "Zony", 0, 0, "zony");
      transaction.commit();
    } catch (TransactionException e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void loadCustomerInitialData() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      transaction = manager.start();
      loadCustomerIfNotExists(transaction, 1, "Yamada Taro", 90000, 0);
      loadCustomerIfNotExists(transaction, 2, "Yamada Hanako", 10000, 0);
      loadCustomerIfNotExists(transaction, 3, "Suzuki Ichiro", 10000, 0);
      transaction.commit();
    } catch (TransactionException e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void loadItemInitialData() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      transaction = manager.start();
      loadItemIfNotExists(transaction, 1, "Apple", 1000, 3, 1);
      loadItemIfNotExists(transaction, 2, "Apple", 2000, 0, 2);
      loadItemIfNotExists(transaction, 3, "Grape", 2500, 4, 1);
      loadItemIfNotExists(transaction, 4, "Mango", 5000, 10, 1);
      loadItemIfNotExists(transaction, 5, "Melon", 3000, 10, 2);
      loadItemIfNotExists(transaction, 6, "Mango", 1500, 4, 3);
      loadItemIfNotExists(transaction, 7, "PlayAirPort", 1500, 10, 3);
      loadItemIfNotExists(transaction, 8, "ScalarTV", 8000, 10, 3);
      loadItemIfNotExists(transaction, 9, "HeadPhone", 1500, 10, 3);
      transaction.commit();
    } catch (TransactionException e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getShopDBNameSpace(int shopId) throws TransactionException {
    DistributedTransaction transaction = null;
    //System.out.println("getShopNamefromID");
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified customer ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      Optional<Result> shop;
      
      shop = transaction.get(
                new Get(new Key("shop_id", shopId))
                            .forNamespace("shops")
                            .forTable("shops"));
      if (!shop.isPresent()) {
        // If the customer info the specified customer ID doesn’t exist, throw an exception
        throw new RuntimeException("Shop not found");
      }

      result += String.format("%s", shop.get().getValue("db").get().getAsString().get());
      //System.out.println(result);
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  private void loadCustomerIfNotExists(
      DistributedTransaction transaction,
      int customerId,
      String name,
      int creditLimit,
      int creditTotal)
      throws TransactionException {
    Optional<Result> customer =
        transaction.get(
            new Get(new Key("customer_id", customerId))
                .forNamespace("customers")
                .forTable("customers"));
    if (!customer.isPresent()) {
      transaction.put(
          new Put(new Key("customer_id", customerId))
              .withValue("name", name)
              .withValue("credit_limit", creditLimit)
              .withValue("credit_total", creditTotal)
              .forNamespace("customers")
              .forTable("customers"));
      incrementCustomerNum();
    }
  }

  private void loadShopIfNotExists(
      DistributedTransaction transaction,
      int shopId,
      String name,
      int itemcnt,
      int earnings,
      String db)
      throws TransactionException {
    Optional<Result> shop =
        transaction.get(
            new Get(new Key("shop_id", shopId))
                .forNamespace("shops")
                .forTable("shops"));
    if (!shop.isPresent()) {
      transaction.put(
          new Put(new Key("shop_id", shopId))
              .withValue("name", name)
              .withValue("itemcnt", itemcnt)
              .withValue("earnings", earnings)
              .withValue("db", db)
              .forNamespace("shops")
              .forTable("shops"));
      incrementShopNum();
    }
  }

  private void loadItemIfNotExists(
      DistributedTransaction transaction, 
      int itemId, 
      String name, 
      int price,
      int stock, 
      int shopId)
      throws TransactionException {
              

        Optional<Result> item =
            transaction.get(
                new Get(new Key("item_id", itemId))
                            .forNamespace(getShopDBNameSpace(shopId))
                            .forTable("items"));
        if (!item.isPresent()) {
          transaction.put(
              new Put(new Key("item_id", itemId))
                  .withValue("name", name)
                  .withValue("price", price)
                  .withValue("stock", stock)
                  .withValue("shop_id", shopId)
                  .forNamespace(getShopDBNameSpace(shopId))
                  .forTable("items"));
          incrementShopItemCnt(shopId);
          incrementItemNum();
        }
  }

  public void incrementShopNum() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> conf =
          transaction.get(
              new Get(new Key("conf_id", 0))
                  .forNamespace("conf")
                  .forTable("conf"));

      if (!conf.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Conf not Settings");
      }
      int shopnum = conf.get().getValue("shopnum").get().getAsInt();
      shopnum += 1;
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.put(
        new Put(new Key("conf_id", 0))
                    .withValue("shopnum", shopnum)
                    .forNamespace("conf")
                  .forTable("conf"));
                    
      result += String.format(
            "{\"Shop\": \"%d\", \"Item\": %d, \"Customer\": %d}\n",
            conf.get().getValue("shopnum").get().getAsInt(),
            conf.get().getValue("itemnum").get().getAsInt(),
            conf.get().getValue("customernum").get().getAsInt());
      transaction.commit();
      // Return the customer info as a JSON format
      
      return; //result;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void incrementItemNum() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> conf =
          transaction.get(
              new Get(new Key("conf_id", 0))
                  .forNamespace("conf")
                  .forTable("conf"));

      if (!conf.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Conf not Settings");
      }
      int itemnum = conf.get().getValue("itemnum").get().getAsInt();
      itemnum += 1;
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.put(
        new Put(new Key("conf_id", 0))
                    .withValue("itemnum", itemnum)
                    .forNamespace("conf")
                  .forTable("conf"));
                    
      result += String.format(
            "{\"Shop\": \"%d\", \"Item\": %d, \"Customer\": %d}\n",
            conf.get().getValue("shopnum").get().getAsInt(),
            conf.get().getValue("itemnum").get().getAsInt(),
            conf.get().getValue("customernum").get().getAsInt());
      transaction.commit();
      // Return the customer info as a JSON format
      
      return; //result;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void incrementCustomerNum() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> conf =
          transaction.get(
              new Get(new Key("conf_id", 0))
                  .forNamespace("conf")
                  .forTable("conf"));

      if (!conf.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Conf not Settings");
      }
      int customernum = conf.get().getValue("customernum").get().getAsInt();
      customernum += 1;
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.put(
        new Put(new Key("conf_id", 0))
                    .withValue("customernum", customernum)
                    .forNamespace("conf")
                  .forTable("conf"));
                    
      result += String.format(
            "{\"Shop\": \"%d\", \"Item\": %d, \"Customer\": %d}\n",
            conf.get().getValue("shopnum").get().getAsInt(),
            conf.get().getValue("itemnum").get().getAsInt(),
            conf.get().getValue("customernum").get().getAsInt());
      transaction.commit();
      // Return the customer info as a JSON format
      
      return; //result;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void incrementShopItemCnt(int shopId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> shop =
          transaction.get(
              new Get(new Key("shop_id", shopId))
                  .forNamespace("shops")
                  .forTable("shops"));

      if (!shop.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Shop not found");
      }
      int itemcnt = shop.get().getValue("itemcnt").get().getAsInt();
      itemcnt += 1;
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.put(
        new Put(new Key("shop_id", shopId))
                    .withValue("itemcnt", itemcnt)
                    .forNamespace("shops")
                  .forTable("shops"));
                    
      result += String.format(
            "{\"id\": %d, \"name\": \"%s\", \"itemcnt\": %d, \"earnings\": %d, \"DB\": \"%s\"}\n",
            shopId,
            shop.get().getValue("name").get().getAsString().get(),
            shop.get().getValue("itemcnt").get().getAsInt(),
            shop.get().getValue("earnings").get().getAsInt(),
            shop.get().getValue("db").get().getAsString().get());
      transaction.commit();
      // Return the customer info as a JSON format
      
      
      return; //result;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void decrementShopItemCnt(int shopId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> shop =
          transaction.get(
              new Get(new Key("shop_id", shopId))
                  .forNamespace("shops")
                  .forTable("shops"));

      if (!shop.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Shop not found");
      }
      int itemcnt = shop.get().getValue("itemcnt").get().getAsInt();
      itemcnt -= 1;
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.put(
        new Put(new Key("shop_id", shopId))
                    .withValue("itemcnt", itemcnt)
                    .forNamespace("shops")
                  .forTable("shops"));
                    
      result += String.format(
            "{\"id\": %d, \"name\": \"%s\", \"itemcnt\": %d, \"earnings\": %d, \"DB\": \"%s\"}\n",
            shopId,
            shop.get().getValue("name").get().getAsString().get(),
            shop.get().getValue("itemcnt").get().getAsInt(),
            shop.get().getValue("earnings").get().getAsInt(),
            shop.get().getValue("db").get().getAsString().get());
      transaction.commit();
      // Return the customer info as a JSON format
      
      return ;//result;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public int getShopNum() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> conf =
          transaction.get(
              new Get(new Key("conf_id", 0))
                  .forNamespace("conf")
                  .forTable("conf"));

      if (!conf.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Conf not Settings");
      }
      int shopnum = conf.get().getValue("shopnum").get().getAsInt();
      
      transaction.commit();
      // Return the customer info as a JSON format
      
      return shopnum;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public int getItemNum() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> conf =
          transaction.get(
              new Get(new Key("conf_id", 0))
                  .forNamespace("conf")
                  .forTable("conf"));

      if (!conf.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Conf not Settings");
      }
      int itemnum = conf.get().getValue("itemnum").get().getAsInt();
      
      transaction.commit();
      // Return the customer info as a JSON format
      
      return itemnum;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public int getCustomerNum() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> conf =
          transaction.get(
              new Get(new Key("conf_id", 0))
                  .forNamespace("conf")
                  .forTable("conf"));

      if (!conf.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Conf not Settings");
      }
      int customernum = conf.get().getValue("customernum").get().getAsInt();
      
      transaction.commit();
      // Return the customer info as a JSON format
      
      return customernum;

    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getCustomerInfo(int customerId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> customer =
          transaction.get(
              new Get(new Key("customer_id", customerId))
                  .forNamespace("customers")
                  .forTable("customers"));

      if (!customer.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Customer not found");
      }

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      // Return the customer info as a JSON format
      return String.format(
          "{\"id\": %d, \"name\": \"%s\", \"credit_limit\": %d, \"credit_total\": %d}",
          customerId,
          customer.get().getValue("name").get().getAsString().get(),
          customer.get().getValue("credit_limit").get().getAsInt(),
          customer.get().getValue("credit_total").get().getAsInt());
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getCustomerList() throws TransactionException {
    DistributedTransaction transaction = null;
    
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified item ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      int customernum = getCustomerNum();
      int customercnt = 0;
      Optional<Result> customer;
      for(int i=1; customercnt < customernum; i++){
          customer = transaction.get(
                          new Get(new Key("customer_id", i))
                            .forNamespace("customers")
                            .forTable("customers"));
          
          if(customer.isPresent()){
            customercnt++;
            result += String.format(
            "{\"id\": %d, \"name\": \"%s\", \"credit_limit\": %d, \"credit_total\": %d}\n",
            i,
            customer.get().getValue("name").get().getAsString().get(),
            customer.get().getValue("credit_limit").get().getAsInt(),
            customer.get().getValue("credit_total").get().getAsInt());
          }
          
      }
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  private String getShopNamefromID(int shopId) throws TransactionException {
    DistributedTransaction transaction = null;
    //System.out.println("getShopNamefromID");
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified customer ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      Optional<Result> shop;
      
      shop = transaction.get(
                new Get(new Key("shop_id", shopId))
                            .forNamespace("shops")
                            .forTable("shops"));
      if (!shop.isPresent()) {
        // If the customer info the specified customer ID doesn’t exist, throw an exception
        throw new RuntimeException("Shop not found");
      }

      result += String.format("%s", shop.get().getValue("name").get().getAsString().get());
      
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }
  /* 
  public String getItemList() throws TransactionException {
    DistributedTransaction transaction = null;
    
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified item ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      int itemnum = getItemNum();
      int itemcnt = 0;
      Optional<Result> item;
      for(int i=1; itemcnt < itemnum; i++){
          item = transaction.get(
                          new Get(new Key("item_id", i))
                            .forNamespace("items")
                            .forTable("items"));
          
          if(item.isPresent()){
            itemcnt++;
            result += String.format(
              "{\"id\": %d, \"name\": \"%s\", \"price\": %d, \"stock\": %d, \"shop\": \"%s\"}\n",
              i,
              item.get().getValue("name").get().getAsString().get(),
              item.get().getValue("price").get().getAsInt(),
              item.get().getValue("stock").get().getAsInt(),
              getShopNamefromID(item.get().getValue("shop_id").get().getAsInt()));
          }
          
      }
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }*/

  public String getShopList() throws TransactionException {
    DistributedTransaction transaction = null;
    
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified item ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      int shopnum = getShopNum();
      int shopcnt = 0;
      Optional<Result> shop;
      for(int i=1; shopcnt < shopnum; i++){
          shop = transaction.get(
                          new Get(new Key("shop_id", i))
                            .forNamespace("shops")
                            .forTable("shops"));
          
          if(shop.isPresent()){
            shopcnt++;
            result += String.format(
            "{\"id\": %d, \"name\": \"%s\", \"itemcnt\": %d, \"earnings\": %d, \"DB\": \"%s\"}\n",
            i,
            shop.get().getValue("name").get().getAsString().get(),
            shop.get().getValue("itemcnt").get().getAsInt(),
            shop.get().getValue("earnings").get().getAsInt(),
            shop.get().getValue("db").get().getAsString().get());
          }
          
      }
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getItemList() throws TransactionException {
    DistributedTransaction transaction = null;
    
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified item ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      int shopnum = getShopNum();
      int shopcnt = 0;
      Optional<Result> shop;
      for(int i=1; shopcnt < shopnum; i++){
          shop = transaction.get(
                          new Get(new Key("shop_id", i))
                            .forNamespace("shops")
                            .forTable("shops"));
          
          if(shop.isPresent()){
            shopcnt++;
            result += getShopItemList(i);
          }
          
      }
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getShopItemList(int shopId) throws TransactionException {
    DistributedTransaction transaction = null;
    //System.out.println("getShopItemList");
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified customer ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      Optional<Result> shop;
      shop = transaction.get(
                          new Get(new Key("shop_id", shopId))
                            .forNamespace("shops")
                            .forTable("shops"));
      if(!shop.isPresent()){
        throw new RuntimeException("Shop not found");
      }
      
      result+=retShopItemList(shopId,shop.get().getValue("itemcnt").get().getAsInt());
      
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String retShopItemList(int shopId, int itemcnt) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified customer ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      
      Optional<Result> item;
      int itemId = 0;
      String itemNS = getShopDBNameSpace(shopId);
      
      for(int i=0; i<itemcnt; ){
          itemId += 1;
          item = transaction.get(
                          new Get(new Key("item_id", itemId))
                            .forNamespace(
                                itemNS
                              )
                            .forTable("items"));
          if(!item.isPresent())continue;
          
          if(item.get().getValue("shop_id").get().getAsInt()==shopId){
            
              result += String.format(
              "{\"id\": %d, \"name\": \"%s\", \"price\": %d, \"stock\": %d, \"seller\": { \"name\": \"%s\" }}",
              itemId,
              item.get().getValue("name").get().getAsString().get(),
              item.get().getValue("price").get().getAsInt(),
              item.get().getValue("stock").get().getAsInt(),
              itemNS);
              i+=1;
          }
      }
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      
      return result;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getItemInfo(int itemId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> item =
          transaction.get(
              new Get(new Key("item_id", itemId))
                  .forNamespace(getShopDBNameSpace(getShopIdfromItemId(itemId)))
                  .forTable("items"));

      if (!item.isPresent()) {
        // If the customer info the specified customer ID doesn't exist, throw an exception
        throw new RuntimeException("Customer not found");
      }

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      return String.format(
              "{\"id\": %d, \"name\": \"%s\", \"price\": %d, \"stock\": %d}\n",
              itemId,
              item.get().getValue("name").get().getAsString().get(),
              item.get().getValue("price").get().getAsInt(),
              item.get().getValue("stock").get().getAsInt());

      // Return the customer info as a JSON format
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public int getShopIdfromItemId(int itemId) throws TransactionException {
    DistributedTransaction transaction = null;
    
    try {
      int shopId = 0;
      String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified item ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      int shopnum = getShopNum();
      int shopcnt = 0;
      Optional<Result> shop;
      for(int i=1; shopcnt < shopnum; i++){
          shop = transaction.get(
                          new Get(new Key("shop_id", i))
                            .forNamespace("shops")
                            .forTable("shops"));
          
          if(shop.isPresent()){
            shopcnt++;
            if(retItemIdinShop(itemId,i,shop.get().getValue("itemcnt").get().getAsInt())){
              shopId = i;
              break;
            }
          }
      }
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      return shopId;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public boolean retItemIdinShop(int itemId, int shopId, int itemcnt) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      boolean ret = false;
      //String result = "";
      // Start a transaction
      transaction = manager.start();
      // Retrieve the customer info for the specified customer ID from the customers table
      //Optional<Result>[] customer = new Optional<Result>[3];
      
      Optional<Result> item;
      
      String shopNS = getShopDBNameSpace(shopId);
      
      item = transaction.get(
                          new Get(new Key("item_id", itemId))
                            .forNamespace(shopNS)
                            .forTable("items"));

      ret = item.isPresent();
      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();
      // Return the customer info as a JSON format
      
      return ret;
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String placeOrder(int customerId, int[] itemIds, int[] itemCounts)
      throws TransactionException {
    assert itemIds.length == itemCounts.length;

    DistributedTransaction transaction = null;
    try {
      String orderId = UUID.randomUUID().toString();

      // Start a transaction
      transaction = manager.start();

      // Put the order info into the orders table
      transaction.put(
          new Put(
                  new Key("customer_id", customerId),
                  new Key("timestamp", System.currentTimeMillis()))
              .withValue("order_id", orderId)
              .forNamespace("orders")
              .forTable("orders"));

      int amount = 0;
      for (int i = 0; i < itemIds.length; i++) {
        int itemId = itemIds[i];
        int count = itemCounts[i];

        // Put the order statement into the statements table
        transaction.put(
            new Put(new Key("order_id", orderId), new Key("item_id", itemId))
                .withValue("count", count)
                .forNamespace("statements")
                .forTable("statements"));

        // Retrieve the item info from the items table
        String shopNS = getShopDBNameSpace(getShopIdfromItemId(itemId));
        Optional<Result> item =
            transaction.get(
                new Get(new Key("item_id", itemId)).forNamespace(shopNS).forTable("items"));
        if (!item.isPresent()) {
          throw new RuntimeException("Item not found");
        }
        int stock = item.get().getValue("stock").get().getAsInt(); 
        if(stock < count){
          throw new RuntimeException("Item is out of Stock");
        }
        stock -= count;
        transaction.put(
          new Put(new Key("item_id", itemId))
              .withValue("stock", stock)
              .forNamespace(shopNS)
              .forTable("items"));
        // Calculate the total amount
        amount += item.get().getValue("price").get().getAsInt() * count;

        int shopId = item.get().getValue("shop_id").get().getAsInt();
        Optional<Result> shop =
            transaction.get(
                new Get(new Key("shop_id", shopId)).forNamespace("shops").forTable("shops"));
        int earnings = amount + shop.get().getValue("earnings").get().getAsInt();
        transaction.put(
          new Put(new Key("shop_id", shopId))
              .withValue("earnings", earnings)
              .forNamespace("shops")
              .forTable("shops"));

      }

      // Check if the credit total exceeds the credit limit after payment
      Optional<Result> customer =
          transaction.get(
              new Get(new Key("customer_id", customerId))
                  .forNamespace("customers")
                  .forTable("customers"));
      if (!customer.isPresent()) {
        throw new RuntimeException("Customer not found");
      }
      int creditLimit = customer.get().getValue("credit_limit").get().getAsInt();
      int creditTotal = customer.get().getValue("credit_total").get().getAsInt();
      if (creditTotal + amount > creditLimit) {
        throw new RuntimeException("Credit limit exceeded");
      }

      // Update credit_total for the customer
      transaction.put(
          new Put(new Key("customer_id", customerId))
              .withValue("credit_total", creditTotal + amount)
              .forNamespace("customers")
              .forTable("customers"));

      // Commit the transaction
      transaction.commit();

      // Return the order id
      return String.format("{\"order_id\": \"%s\"}", orderId);
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  private String getOrderJson(DistributedTransaction transaction, String orderId)
      throws TransactionException {
    // Retrieve the order info for the order ID from the orders table
    Optional<Result> order =
        transaction.get(
            new Get(new Key("order_id", orderId)).forNamespace("orders").forTable("orders"));
    if (!order.isPresent()) {
      throw new RuntimeException("Order not found");
    }

    int customerId = order.get().getValue("customer_id").get().getAsInt();

    // Retrieve the customer info for the specified customer ID from the customers table
    Optional<Result> customer =
        transaction.get(
            new Get(new Key("customer_id", customerId))
                .forNamespace("customers")
                .forTable("customers"));

    // Retrieve the order statements for the order ID from the statements table
    List<Result> statements =
        transaction.scan(
            new Scan(new Key("order_id", orderId)).forNamespace("statements").forTable("statements"));

    // Make the statements JSONs
    List<String> statementJsons = new ArrayList<>();
    int total = 0;
    for (Result statement : statements) {
      int itemId = statement.getValue("item_id").get().getAsInt();

      // Retrieve the item data from the items table
      String shopNS = getShopDBNameSpace(getShopIdfromItemId(itemId));
      Optional<Result> item =
          transaction.get(
              new Get(new Key("item_id", itemId)).forNamespace(shopNS).forTable("items"));
      if (!item.isPresent()) {
        throw new RuntimeException("Item not found");
      }

      int price = item.get().getValue("price").get().getAsInt();
      int count = statement.getValue("count").get().getAsInt();

      statementJsons.add(
          String.format(
              "{\"item_id\": %d,\"item_name\": \"%s\",\"price\": %d,\"count\": %d,\"total\": %d}",
              itemId,
              item.get().getValue("name").get().getAsString().get(),
              price,
              count,
              price * count));

      total += price * count;
    }

    // Return the order info as a JSON format
    return String.format(
        "{\"order_id\": \"%s\",\"timestamp\": %d,\"customer_id\": %d,\"customer_name\": \"%s\",\"statement\": [%s],\"total\": %d}",
        orderId,
        order.get().getValue("timestamp").get().getAsLong(),
        customerId,
        customer.get().getValue("name").get().getAsString().get(),
        String.join(",", statementJsons),
        total);
  }

  public String getOrderByOrderId(String orderId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Get an order JSON for the specified order ID
      String orderJson = getOrderJson(transaction, orderId);

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      // Return the order info as a JSON format
      return String.format("{\"order\": %s}", orderJson);
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getOrdersByCustomerId(int customerId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the order info for the customer ID from the orders table
      List<Result> orders =
          transaction.scan(
              new Scan(new Key("customer_id", customerId))
                  .forNamespace("orders")
                  .forTable("orders"));

      // Make order JSONs for the orders of the customer
      List<String> orderJsons = new ArrayList<>();
      for (Result order : orders) {
        orderJsons.add(
            getOrderJson(transaction, order.getValue("order_id").get().getAsString().get()));
      }

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      // Return the order info as a JSON format
      return String.format("{\"order\": [%s]}", String.join(",", orderJsons));
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void repayment(int customerId, int amount) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> customer =
          transaction.get(
              new Get(new Key("customer_id", customerId))
                  .forNamespace("customers")
                  .forTable("customers"));
      if (!customer.isPresent()) {
        throw new RuntimeException("Customer not found");
      }

      int updatedCreditLimit = customer.get().getValue("credit_total").get().getAsInt() - amount;

      // Check if over repayment or not
      if (updatedCreditLimit < 0) {
        throw new RuntimeException("Over repayment");
      }

      // Reduce credit_total in the customer
      transaction.put(
          new Put(new Key("customer_id", customerId))
              .withValue("credit_total", updatedCreditLimit)
              .forNamespace("customers")
              .forTable("customers"));

      // Commit the transaction
      transaction.commit();
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  @Override
  public void close() {
    manager.close();
  }
  
}
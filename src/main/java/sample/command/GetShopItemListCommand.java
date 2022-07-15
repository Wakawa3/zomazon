package sample.command;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import sample.Sample;

@Command(name = "GetShopItemList", description = "Get Shop Product List information")
public class GetShopItemListCommand implements Callable<Integer> {

  @Parameters(index = "0", paramLabel = "ShopId", description = "Shop ID")
  private int shopId;

  @Override
  public Integer call() throws Exception {
    try (Sample sample = new Sample()) {
      System.out.println(sample.getShopItemList(shopId));
    }
    return 0;
  }
}

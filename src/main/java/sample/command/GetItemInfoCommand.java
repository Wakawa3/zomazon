package sample.command;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import sample.Sample;

@Command(name = "GetItemInfo", description = "Get customer information")
public class GetItemInfoCommand implements Callable<Integer> {

  @Parameters(index = "0", paramLabel = "ITEM_ID", description = "item ID")
  private int itemId;

  @Override
  public Integer call() throws Exception {
    try (Sample sample = new Sample()) {
      System.out.println(sample.getItemInfo(itemId));
    }
    return 0;
  }
}

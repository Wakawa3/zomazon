package sample.command;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import sample.Sample;

@Command(name = "GetItemList", description = "Get Product List information")
public class GetItemListCommand implements Callable<Integer> {

  @Override
  public Integer call() throws Exception {
    try (Sample sample = new Sample()) {
      System.out.println(sample.getItemList());
    }
    return 0;
  }
}

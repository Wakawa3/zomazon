package sample.command;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import sample.Sample;

@Command(name = "GetShopList", description = "Get Shop List information")
public class GetShopListCommand implements Callable<Integer> {

  @Override
  public Integer call() throws Exception {
    try (Sample sample = new Sample()) {
      System.out.println(sample.getShopList());
    }
    return 0;
  }
}

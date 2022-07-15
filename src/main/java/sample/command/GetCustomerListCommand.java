package sample.command;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import sample.Sample;

@Command(name = "GetCustomerList", description = "Get Product List information")
public class GetCustomerListCommand implements Callable<Integer> {

  @Override
  public Integer call() throws Exception {
    try (Sample sample = new Sample()) {
      System.out.println(sample.getCustomerList());
    }
    return 0;
  }
}

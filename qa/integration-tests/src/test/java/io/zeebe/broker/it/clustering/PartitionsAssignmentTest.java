package io.zeebe.broker.it.clustering;

import io.zeebe.broker.it.GrpcClientRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class PartitionsAssignmentTest {

  public static final String NULL_PAYLOAD = null;
  public static final String JOB_TYPE = "testTask";

  public Timeout testTimeout = Timeout.seconds(120);
  public ClusteringRule clusteringRule = new ClusteringRule(8,3,4);
  public GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
    RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  @Test
  public void testPartitions(){
    clusteringRule.getLeaderForPartition(0);
  }

}

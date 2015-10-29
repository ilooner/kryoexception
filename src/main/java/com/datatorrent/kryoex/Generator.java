/**
 * Put your copyright and license info here.
 */
package com.datatorrent.kryoex;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.kryoex.MyClass.MyClassA;
import com.datatorrent.kryoex.MyClass.MyClassB;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Map;

/**
 * This is a simple operator that emits random number.
 */
public class Generator extends BaseOperator implements InputOperator, Partitioner<Generator>
{
  public boolean sendA = false;

  public final transient DefaultOutputPort<MyClass> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    if(sendA) {
      out.emit(new MyClassA());
    }
    else {
      out.emit(new MyClassB());
    }
  }

  @Override
  public void emitTuples()
  {
  }

  @Override
  public Collection<Partition<Generator>> definePartitions(Collection<Partition<Generator>> clctn, PartitioningContext pc)
  {
    Collection<Partition<Generator>> partitions = Lists.newArrayList();
    Generator genA = new Generator();
    genA.sendA = true;
    Generator genB = new Generator();
    genB.sendA = false;

    partitions.add(new DefaultPartition(genA));
    partitions.add(new DefaultPartition(genB));

    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<Generator>> map)
  {
  }
}

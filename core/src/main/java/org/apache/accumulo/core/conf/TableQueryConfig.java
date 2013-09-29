package org.apache.accumulo.core.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TableQueryConfig implements Writable {
  
  private String tableName;
  private List<IteratorSetting> iterators;
  private List<Range> ranges;
  private Set<Pair<Text,Text>> columns;
  
  private boolean autoAdjustRanges = true;
  private boolean useLocalIterators = false;
  private boolean useIsolatedScanners = false;
  
  public TableQueryConfig(String tableName) {
    checkNotNull(tableName);
    this.tableName = tableName;
  }

  public TableQueryConfig(DataInput input) throws IOException{
    readFields(input);
  }

  public TableQueryConfig setRanges(List<Range> ranges) {
    this.ranges = ranges;
    return this;
  }

  public TableQueryConfig setColumns(Set<Pair<Text,Text>> columns) {
    this.columns = columns;
    return this;
  }

  public TableQueryConfig setIterators(List<IteratorSetting> iterators) {
    this.iterators = iterators;
    return this;
  }

  public TableQueryConfig setAutoAdjustRanges(boolean autoAdjustRanges){
    this.autoAdjustRanges=autoAdjustRanges;
    return this;
  }

  public TableQueryConfig setUseLocalIterators(boolean useLocalIterators){
    this.useLocalIterators=useLocalIterators;
    return this;
  }

  public TableQueryConfig setUseIsolatedScanners(boolean useIsolatedScanners){
    this.useIsolatedScanners=useIsolatedScanners;
    return this;
  }

  public String getTableName(){
    return tableName;
  }

  public List<IteratorSetting> getIterators(){
    return iterators;
  }

  public List<Range> getRanges(){
    return ranges;
  }

  public Set<Pair<Text,Text>> getColumns(){
    return columns;
  }

  public boolean shouldAutoAdjustRanges(){
    return autoAdjustRanges;
  }

  public boolean shouldUseLocalIterators(){
    return useLocalIterators;
  }

  public boolean shouldUseIsolatedScanners(){
    return useIsolatedScanners;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(tableName);
    if (iterators != null) {
      dataOutput.writeInt(iterators.size());
      for (IteratorSetting setting : iterators)
        setting.write(dataOutput);
    } else {
      dataOutput.writeInt(0);
    }
    if (ranges != null) {
      dataOutput.writeInt(ranges.size());
      for (Range range : ranges)
        range.write(dataOutput);
    } else {
      dataOutput.writeInt(0);
    }
    if (columns != null) {
      dataOutput.writeInt(columns.size());
      for (Pair<Text,Text> column : columns) {
        if (column.getSecond() == null) {
          dataOutput.writeInt(1);
          column.getFirst().write(dataOutput);
        } else {
          dataOutput.writeInt(2);
          column.getFirst().write(dataOutput);
          column.getSecond().write(dataOutput);
        }
      }
    } else {
      dataOutput.writeInt(0);
    }
    dataOutput.writeBoolean(autoAdjustRanges);
    dataOutput.writeBoolean(useLocalIterators);
    dataOutput.writeBoolean(useIsolatedScanners);
  }
  
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.tableName = dataInput.readUTF();
    // load iterators
    long iterSize = dataInput.readInt();
    if (iterSize > 0)
      iterators = new ArrayList<IteratorSetting>();
    for (int i = 0; i < iterSize; i++)
      iterators.add(new IteratorSetting(dataInput));
    // load ranges
    long rangeSize = dataInput.readInt();
    if (rangeSize > 0)
      ranges = new ArrayList<Range>();
    for (int i = 0; i < rangeSize; i++) {
      Range range = new Range();
      range.readFields(dataInput);
      ranges.add(range);
    }
    // load columns
    long columnSize = dataInput.readInt();
    if (columnSize > 0)
      columns = new HashSet<Pair<Text,Text>>();
    for (int i = 0; i < columnSize; i++) {
      long numPairs = dataInput.readInt();
      Text colFam = new Text();
      colFam.readFields(dataInput);
      if (numPairs == 1) {
        columns.add(new Pair<Text,Text>(colFam, null));
      } else if (numPairs == 2) {
        Text colQual = new Text();
        colQual.readFields(dataInput);
        columns.add(new Pair<Text,Text>(colFam, colQual));
      }
    }
    autoAdjustRanges = dataInput.readBoolean();
    useLocalIterators = dataInput.readBoolean();
    useIsolatedScanners = dataInput.readBoolean();
  }

  @Override
  public boolean equals(Object o){
    if(this==o) return true;
    if(o==null||getClass()!=o.getClass()) return false;

    TableQueryConfig that=(TableQueryConfig)o;

    if(autoAdjustRanges!=that.autoAdjustRanges) return false;
    if(columns!=null?!columns.equals(that.columns):that.columns!=null) return false;
    if(iterators!=null?!iterators.equals(that.iterators):that.iterators!=null) return false;
    if(ranges!=null?!ranges.equals(that.ranges):that.ranges!=null) return false;
    if(tableName!=null?!tableName.equals(that.tableName):that.tableName!=null) return false;

    return true;
  }

  @Override
  public int hashCode(){
    int result=tableName!=null?tableName.hashCode():0;
    result=31*result+(iterators!=null?iterators.hashCode():0);
    result=31*result+(ranges!=null?ranges.hashCode():0);
    result=31*result+(columns!=null?columns.hashCode():0);
    result=31*result+(autoAdjustRanges?1:0);
    return result;
  }
}

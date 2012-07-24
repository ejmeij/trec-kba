/*******************************************************************************
 * Copyright 2012 Edgar Meij
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package ilps.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Non-splitable FileInputFormat.
 * 
 * @author emeij
 *
 */
public class ThriftFileInputFormat extends
    FileInputFormat<Text, StreamItemWritable> {

  // private static final Log LOG =
  // LogFactory.getLog(ThriftFileInputFormat.class);

  @Override
  public RecordReader<Text, StreamItemWritable> createRecordReader(
      InputSplit split, TaskAttemptContext tac) throws IOException,
      InterruptedException {
    return new ThriftRecordReader((FileSplit) split, tac.getConfiguration());
  }

  /**
   * Split the inputfiles on the object boundary.
   * 
   * @param fs
   * @param filename
   * @return
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
  /*
    // @Override
    public List<InputSplit> getSplitsNew(JobContext job) throws IOException {

      long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));

      // generate splits
      List<InputSplit> splits = new ArrayList<InputSplit>();

      for (FileStatus file : listStatus(job)) {

        int count = 0;

        Path path = file.getPath();
        FileSystem fs = path.getFileSystem(job.getConfiguration());

        FSDataInputStream in = fs.open(path);
        TProtocol tp = new TBinaryProtocol.Factory()
            .getProtocol(new TIOStreamTransport(in));

        long length = file.getLen();
        long position = 0;
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

        if ((length != 0)) {

          // long blockSize = file.getBlockSize();
          // long splitSize = computeSplitSize(blockSize, minSize, length);
          long splitSize = 0;
          long bytesRemaining = length;
          long curLen = 0;
          long prevpos = 0;
          long newpos = 0;

          StreamItem si = new StreamItem();

          while (bytesRemaining > 0) {

            try {

              prevpos = length - in.available();
              si.read(tp);
              newpos = length - in.available();

              if (++count % 1000 == 0) {

                int blkIndex = getBlockIndex(blkLocations, length
                    - bytesRemaining);

                splits.add(new FileSplit(path, length - bytesRemaining,
                    splitSize, blkLocations[blkIndex].getHosts()));

                position = bytesRemaining;
                splitSize = 0;

              } else {
                splitSize += newpos - prevpos;
              }

              bytesRemaining = in.available();

            } catch (Exception e) {
              e.printStackTrace();
              throw new IOException(e);
            }
          }

          if (splitSize != 0) {
            splits.add(new FileSplit(path, length - bytesRemaining, splitSize,
                blkLocations[blkLocations.length - 1].getHosts()));
          }

          // get any remaining blocks
          if (length - position != 0) {
            int blkIndex = getBlockIndex(blkLocations, length - position);
            splits.add(new FileSplit(path, length - bytesRemaining, splitSize,
                blkLocations[blkIndex].getHosts()));
          }

          LOG.info("a Total # of splits: " + splits.size());


        } else if (length != 0) {
          splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
        } else {
          // Create empty hosts array for zero length files
          splits.add(new FileSplit(path, 0, length, new String[0]));
        }
      }

      LOG.info("b Total # of splits: " + splits.size());
      // System.exit(-1);

      return splits;
    }
    */
}

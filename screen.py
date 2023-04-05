(base) [root@cm1 pycharm_project_842]#  su - hdfs -c "/home/hdfs/anaconda3/bin/python /tmp/pycharm_project_842/yn69.py -r hadoop --hadoop-streaming-jar /opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/hadoop-mapreduce/hadoop-streaming.jar hdfs:///tmp/input/* hdfs:///tmp/output/"
No configs found; falling back on auto-configuration
No configs specified for hadoop runner
Looking for hadoop binary in $PATH...
Found hadoop binary: /bin/hadoop
Using Hadoop version 3.0.0
Creating temp directory /tmp/yn69.hdfs.20211228.103211.416644
uploading working dir files to hdfs:///user/hdfs/tmp/mrjob/yn69.hdfs.20211228.103211.416644/files/wd...
Copying other local files to hdfs:///user/hdfs/tmp/mrjob/yn69.hdfs.20211228.103211.416644/files/
Running step 1 of 1...
  WARNING: Use "yarn jar" to launch YARN applications.
  packageJobJar: [] [/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/jars/hadoop-streaming-3.0.0-cdh6.2.0.jar] /tmp/streamjob3958123564344371990.jar tmpDir=null
  Connecting to ResourceManager at cm1.example.com/192.168.5.20:8032
  Connecting to ResourceManager at cm1.example.com/192.168.5.20:8032
  Disabling Erasure Coding for path: /user/hdfs/.staging/job_1640645331135_0003
  Total input files to process : 6
  number of splits:6
  yarn.resourcemanager.system-metrics-publisher.enabled is deprecated. Instead, use yarn.system-metrics-publisher.enabled
  Submitting tokens for job: job_1640645331135_0003
  Executing with tokens: []
  resource-types.xml not found
  Unable to find 'resource-types.xml'.
  Submitted application application_1640645331135_0003
  The url to track the job: http://cm1.example.com:8088/proxy/application_1640645331135_0003/
  Running job: job_1640645331135_0003
  Job job_1640645331135_0003 running in uber mode : false
   map 0% reduce 0%
  Task Id : attempt_1640645331135_0003_m_000000_0, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)

  Task Id : attempt_1640645331135_0003_m_000001_0, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)

  Task Id : attempt_1640645331135_0003_m_000000_1, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)

  Task Id : attempt_1640645331135_0003_m_000001_1, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)

  Task Id : attempt_1640645331135_0003_m_000000_2, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)

  Task Id : attempt_1640645331135_0003_m_000001_2, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)

   map 100% reduce 100%
  Job job_1640645331135_0003 failed with state FAILED due to: Task failed task_1640645331135_0003_m_000000
Job failed as tasks failed. failedMaps:1 failedReduces:0 killedMaps:0 killedReduces: 0

  Job not successful!
  Streaming Command Failed!
Counters: 11
        Job Counters
                Data-local map tasks=2
                Failed map tasks=7
                Killed map tasks=5
                Killed reduce tasks=1
                Launched map tasks=7
                Other local map tasks=5
                Total megabyte-milliseconds taken by all map tasks=40361984
                Total time spent by all map tasks (ms)=39416
                Total time spent by all maps in occupied slots (ms)=39416
                Total time spent by all reduces in occupied slots (ms)=0
                Total vcore-milliseconds taken by all map tasks=39416
Scanning logs for probable cause of failure...
Looking for history log in /var/log/hadoop-yarn...

Probable cause of failure:

Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:325)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:538)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:465)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:349)
        at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:174)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1875)
        at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:168)


Step 1 of 1 failed: Command '['/bin/hadoop', 'jar', '/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/hadoop-mapreduce/hadoop-streaming.jar', '-files', 'hdfs:///user/hdfs/tmp/mrjob/yn69.hdfs.20211228.103211.416644/files/wd/mrjob.zip#mrjob.zip,hdfs:///user/hdfs/tmp/mrjob/yn69.hdfs.20211228.103211.416644/files/wd/setup-wrapper.sh#setup-wrapper.sh,hdfs:///user/hdfs/tmp/mrjob/yn69.hdfs.20211228.103211.416644/files/wd/yn69.py#yn69.py', '-input', 'hdfs:///tmp/input/*', '-input', 'hdfs:///tmp/output/', '-output', 'hdfs:///user/hdfs/tmp/mrjob/yn69.hdfs.20211228.103211.416644/output', '-mapper', '/bin/sh -ex setup-wrapper.sh python3 yn69.py --step-num=0 --mapper', '-reducer', '/bin/sh -ex setup-wrapper.sh python3 yn69.py --step-num=0 --reducer']' returned non-zero exit status 256.
(base) [root@cm1 pycharm_project_842]#
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.store.hdfs;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockLostException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockReleaseFailedException;
import org.apache.solr.common.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsLockFactory extends LockFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final long DEFAULT_LOCK_ACQUIRE_TIMEOUT = 90_000L;
  public static final long DEFAULT_LOCK_HOLD_TIMEOUT = DEFAULT_LOCK_ACQUIRE_TIMEOUT / 2;
  public static final long DEFAULT_UPDATE_DELAY = 5000L;
  public static final HdfsLockFactory INSTANCE = new HdfsLockFactory();

  public static final String LOCK_HOLD_TIMEOUT_KEY = "hdfs.lock.update.hold.timeout";
  public static final String UPDATE_DELAY_KEY = "hdfs.lock.update.delay";
  public static final String HDFS_LOCK_ACQUIRE_TIMEOUT_KEY = "hdfs.lock.acquire.timeout";

  private SleepService sleeper;

  private ScheduledExecutorService executor;
  private Consumer<Exception> exceptionHandler;

  private HdfsLockFactory() {
    reset();
  }

  void reset() {
    sleeper = m -> {
      try {
        Thread.sleep(m);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
    executor = Executors.newScheduledThreadPool(5);
  }

  public void setExecutorService(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public void setSleeper(SleepService sleeper) {
    this.sleeper = sleeper;
  }

  public void setExceptionHandler(Consumer<Exception> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
  }


  public interface SleepService {
    public void sleep(long millis);
  }

  @Override
  public Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (!(dir instanceof HdfsDirectory)) {
      throw new UnsupportedOperationException("HdfsLockFactory can only be used with HdfsDirectory subclasses, got: " + dir);
    }
    final HdfsDirectory hdfsDir = (HdfsDirectory) dir;
    final Configuration conf = hdfsDir.getConfiguration();
    final Path lockPath = hdfsDir.getHdfsDirPath();
    final Path lockFile = new Path(lockPath, lockName);
    
    FSDataOutputStream file = null;
    final FileSystem fs = FileSystem.get(lockPath.toUri(), conf);
    log.info("Trying to lock on " + fs.getClass().getName() + " the following path:" + lockPath);
    long lockTimeout = Long.valueOf(System.getProperty(HDFS_LOCK_ACQUIRE_TIMEOUT_KEY, String.valueOf(DEFAULT_LOCK_ACQUIRE_TIMEOUT)));
    HdfsLock hdfsLock = null;
    while (true) {
      try {
        if (!fs.exists(lockPath)) {
          boolean success = fs.mkdirs(lockPath);
          if (!success) {
            throw new RuntimeException("Could not create directory: " + lockPath);
          }
        } else {
          // just to check for safe mode
          fs.mkdirs(lockPath);
        }

        log.info("Trying to create directory {}, exists: {}", lockPath.getName(), fs.exists(lockFile));
        if(fs.exists(lockFile)) {
          removeIfUntouched(lockFile, fs, lockTimeout);
        }
        hdfsLock = new HdfsLock(conf, lockFile, fs);
        hdfsLock.init();
        break;
      } catch (FileAlreadyExistsException e) {
        throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
      } catch (RemoteException e) {
        if (e.getClassName().equals(
            "org.apache.hadoop.hdfs.server.namenode.SafeModeException")) {
          log.warn("The NameNode is in SafeMode - Solr will wait 5 seconds and try again.");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            Thread.interrupted();
          }
          continue;
        }
        throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
      } catch (IOException e) {
        throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
      }
    }
    hdfsLock.startScheduledUpdate();
    return hdfsLock;
  }

  private void removeIfUntouched(Path lockFile, FileSystem fs, long lockTimeout) throws IOException {
    long before = readLockCycle(lockFile, fs);
    sleeper.sleep(lockTimeout);
    long after = readLockCycle(lockFile, fs);
    if(after == before) {
      fs.delete(lockFile);
      log.info("Removed stale lock at path {}", lockFile.toString());
    }

  }
  private long readLockCycle(Path lockFile, FileSystem fs) throws IOException {
    try (FSDataInputStream lockStream = fs.open(lockFile)) {
      lockStream.readUTF();
      return lockStream.readLong();
    }
  }


  public final class HdfsLock extends Lock {
    private final UUID id = UUID.randomUUID();
    private final Configuration conf;
    private final Path lockFile;
    private final long updateDelay;
    private final long updateTimeoutHold;
    private FileSystem fs;
    private volatile boolean closed;
    private ScheduledFuture<?> scheduledFileUpdate;
    private long cycle = 0;
    long lastLockUpdate = 0;

    HdfsLock(Configuration conf, Path lockFile, FileSystem fs) {
      this.conf = conf;
      this.lockFile = lockFile;
      this.fs = fs;
      updateDelay = Long.valueOf(System.getProperty(UPDATE_DELAY_KEY, String.valueOf(DEFAULT_UPDATE_DELAY)));
      updateTimeoutHold = Long.valueOf(System.getProperty(LOCK_HOLD_TIMEOUT_KEY, String.valueOf(DEFAULT_LOCK_HOLD_TIMEOUT)));
    }
    
    @Override
    public void close() throws IOException {
      final FileSystem fs = FileSystem.get(lockFile.toUri(), conf);
      if (closed) {
        return;
      }
      closed = true;
      stopUpdates();
      try {
        if (fs.exists(lockFile) && !fs.delete(lockFile, false)) {
          throw new LockReleaseFailedException("failed to delete: " + lockFile);
        }
      } finally {
        IOUtils.closeQuietly(fs);
      }
    }

    @Override
    public void ensureValid() throws IOException {
      UUID actualId = readLockId();
      if ( !actualId.equals(id) )
        throw new LockLostException("Another process took ownership of the lock. Lock id: " + actualId + " own id: " + id);
    }

    @Override
    public String toString() {
      return "HdfsLock(lockFile=" + lockFile + ")";
    }

    void stopUpdates() {
      scheduledFileUpdate.cancel(true);
    }

    void startScheduledUpdate() {
      scheduledFileUpdate = executor.scheduleWithFixedDelay(() -> {
        try {
          ensureValid();
          createLockFile(true);
          lastLockUpdate = System.currentTimeMillis();
        } catch (IOException | RuntimeException e) {
          if (System.currentTimeMillis() - lastLockUpdate  >= updateTimeoutHold) {
            stopUpdates();
            exceptionHandler.accept(new LockLostException("Unable to keep the lock, another process may have already took ownership of it", e));
          } else {
            exceptionHandler.accept(e);
          }
          throw new RuntimeException(e);
        }
      }, updateDelay, updateDelay, TimeUnit.MILLISECONDS);
    }

    public void init() throws IOException {
      createLockFile(false);

    }

    private UUID readLockId() throws IOException {
      try (FSDataInputStream lockStream = fs.open(lockFile)) {
        return UUID.fromString(lockStream.readUTF());
      }
    }

    private void createLockFile(boolean overwrite) throws IOException {
      try (FSDataOutputStream file = fs.create(lockFile, overwrite)) {
        file.writeUTF(id.toString());
        file.writeLong(cycle++);
      }
    }
  }
}

package org.example;

import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RefCountedContainer<T extends AutoCloseable> {
  private static final Logger logger = LoggerFactory.getLogger(RefCountedContainer.class);
  private final Object lock = new Object();

  @GuardedBy("lock")
  @Nullable
  private T value;

  @GuardedBy("lock")
  private int refCounter;

  RefCountedContainer() {
    this.value = null;
    this.refCounter = 0;
  }

  Lease getOrCreate(Supplier<T> supplier) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(supplier = {})",supplier.toString());
    synchronized (lock) {
      if (value == null) {
        value = supplier.get();
      }
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
      return new Lease();
    }
  }

  class Lease implements AutoCloseable {

    private boolean valid;

    private Lease() {
      valid = true;
      synchronized (lock) {
        refCounter += 1;
      }
    }

    @Override
    public void close() throws Exception {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      if (valid) {
        synchronized (lock) {
          refCounter -= 1;

          if (refCounter == 0 && value != null) {
            value.close();
            value = null;
          }
        }

        valid = false;
      }
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    }

    T deref() {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      Preconditions.checkState(valid, "Lease is no longer valid");
      synchronized (lock) {
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return Preconditions.checkNotNull(
            value, "Value is null while there are still valid leases. This should not happen.");
      }
    }
  }
}

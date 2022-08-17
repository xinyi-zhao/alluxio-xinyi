/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.annotator;

import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link BlockAnnotator} implementation of Isometric scheme.
 */
public class IsometricAnnotator implements BlockAnnotator<IsometricAnnotator.IsometricSortedField> {
  private static final Logger LOG = LoggerFactory.getLogger(IsometricAnnotator.class);

  /** LRU logical clock. */
  private final AtomicLong mLRUClock = new AtomicLong();
  private final AtomicLong mCount = new AtomicLong();

  /** When DROP_FACTOR = 0, we will never replace the value first added in.*/
  private static final double DISTANCE_FACTOR;
  private static final double DROP_FACTOR;

  static {
    DISTANCE_FACTOR = Configuration.getInt(
        PropertyKey.WORKER_BLOCK_ISOMETRIC_ANNOTATOR_DISTANCE);
    DROP_FACTOR = Configuration.getDouble(
        PropertyKey.WORKER_BLOCK_ISOMETRIC_ANNOTATOR_DROP_FACTOR);
  }

  @Override
  public BlockSortedField updateSortedField(long blockId, IsometricSortedField oldValue) {
    return getNewSortedField(blockId, oldValue, mLRUClock.incrementAndGet());
  }

  @Override
  public void updateSortedFields(List<Pair<Long, IsometricSortedField>> blockList) {
    // Grab the current logical clock, for updating the given entries under.
    long clockValue = mLRUClock.get();
    for (Pair<Long, IsometricSortedField> blockField : blockList) {
      blockField.setSecond(getNewSortedField(
          blockField.getFirst(), blockField.getSecond(), clockValue));
    }
  }

  /**
   * Isometric is an offline scheme.
   *
   * @return {@code false}
   */
  @Override
  public boolean isOnlineSorter() {
    return false;
  }

  private IsometricSortedField getNewSortedField(long blockId,
      IsometricSortedField oldValue, long clock) {
    double crfValue;
    if (oldValue != null) {
      crfValue = oldValue.mCrfValue;
      if (clock != oldValue.mClockValue) {
        crfValue = oldValue.mCrfValue - DROP_FACTOR;
      }
    }
    else {
      long cCount = mCount.incrementAndGet();
      crfValue = cCount % DISTANCE_FACTOR;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Isometric update for Block: {}. Clock:{}, CRF: {}", blockId, clock, crfValue);
    }

    return new IsometricSortedField(clock, crfValue);
  }

  /**
   * Sorted-field for Isometric.
   */
  protected static class IsometricSortedField implements BlockSortedField {
    private final long mClockValue;
    private final double mCrfValue;

    private IsometricSortedField(long clockValue, double crfValue) {
      mClockValue = clockValue;
      mCrfValue = crfValue;
    }

    @Override
    public int compareTo(BlockSortedField o) {
      Preconditions.checkState(o instanceof IsometricSortedField);
      return Double.compare(mCrfValue, ((IsometricSortedField) o).mCrfValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IsometricSortedField)) {
        return false;
      }
      return Double.compare(mCrfValue, ((IsometricSortedField) o).mCrfValue) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mCrfValue);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("Clock",  mClockValue)
          .add("CRF", mCrfValue)
          .toString();
    }
  }
}

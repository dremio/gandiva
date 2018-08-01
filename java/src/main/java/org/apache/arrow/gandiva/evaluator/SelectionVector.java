/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType;

public abstract class SelectionVector {
  private int recordCount;
  private ArrowBuf buffer;

  public SelectionVector(ArrowBuf buffer) {
    this.buffer = buffer;
  }

  public final ArrowBuf getBuffer() {
    return this.buffer;
  }

  public final int getMaxRecords() {
    return buffer.capacity() / getRecordSize();
  }

  public final int getRecordCount() {
    return this.recordCount;
  }

  final void setRecordCount(int recordCount) {
    if (recordCount * getRecordSize() >= buffer.capacity()) {
      throw new IllegalArgumentException("recordCount " + recordCount
        + " exceeds buffer capacity " + buffer.capacity());
    }

    this.recordCount = recordCount;
  }

  public abstract int getIndex(int index);

  abstract int getRecordSize();

  abstract SelectionVectorType getType();

  final void checkReadBounds(int index) {
    if (index >= this.recordCount) {
      throw new IllegalArgumentException("index " + index + " is >= recordCount " + recordCount);
    }
  }

}

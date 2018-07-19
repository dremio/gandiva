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

import com.google.common.collect.Lists;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class ExpressionRegistryTest {

  @Test
  public void testTypes() throws GandivaException {
    Set<ArrowType> types = ExpressionRegistry.getInstance().getSupportedTypes();
    ArrowType.Int UINT8 = new ArrowType.Int(8, false);
    Assert.assertTrue(types.contains(UINT8));

  }

  @Test
  public void testFunctions() throws GandivaException {
    ArrowType.Int UINT8 = new ArrowType.Int(8, false);
    FunctionSignature signature = new FunctionSignature("add", UINT8,Lists.newArrayList(UINT8,UINT8));
    Set<FunctionSignature> functions = ExpressionRegistry.getInstance().getSupportedFunctions();
    Assert.assertTrue(functions.contains(signature));
  }
}

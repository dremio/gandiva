/* Copyright (C) 2017-2018 Dremio Corporation
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

package org.apache.arrow.gandiva.util;

import org.apache.arrow.gandiva.exceptions.GandivaException;

/**
 * Utility class to determine the OS on which Gandiva is being executed.
 * Has helper functions to return environment specific library to load.
 */
public class EnvironmentHelper {

  private static String OS = System.getProperty("os.name").toLowerCase();
  private static String ARCH = System.getProperty("os.arch").toLowerCase();

  /**
   * Returns the library name specific to the OS and Architecture.
   * @param libraryName - the base library name.
   * @return library name specific to the environment.
   * @throws GandivaException if OS is not supported.
   */
  public static String getEnvironmentSpecificLibraryName(String libraryName)
          throws GandivaException {
    if (isMac()) {
      return "lib" + libraryName + ".dylib";
    } else if (isUnix()) {
      return "lib" + libraryName + ".so";
    }
    throw new GandivaException("Unsupported Environment.");
  }

  private static boolean isMac() {
    return (OS.contains("mac"));
  }

  private static boolean isUnix() {
    return (OS.contains("nix")
            || OS.contains("nux")
            || OS.contains("aix"));
  }
}

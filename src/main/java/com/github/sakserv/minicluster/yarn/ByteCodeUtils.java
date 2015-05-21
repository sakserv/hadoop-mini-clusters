/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.minicluster.yarn;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ByteCodeUtils {

    /**
     * @param clazz
     * @return
     */
    public static boolean hasSystemExit(Class<?> clazz) {
//    URL classLocationUrl = clazz.getProtectionDomain().getCodeSource().getLocation();
//    System.out.println(classLocationUrl.getFile());
//    String command = "javap -c -classpath /Users/ozhurakousky/.m2/repository/org/apache/tez/tez-dag/0.5.0-SNAPSHOT/tez-dag-0.5.0-SNAPSHOT.jar " + clazz.getName();
//    System.out.println(command);
//    BufferedReader reader = null;
//    try {
//      Process p = Runtime.getRuntime().exec(command);
//      reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
//      String line;
//      while ((line = reader.readLine()) != null){
//        if (line.contains("System.exit")){
//          return true;
//        }
//      }
//    }
//    catch (Exception e) {
//      throw new IllegalStateException("Failed to successfully execute " + command, e);
//    }
//    finally {
//      try {
//        reader.close();
//      } catch (Exception e2) {
//        // ignore
//      }
//    }
//    return false;
        // need to rework the above code, but for now the assumption is that AppMaster contains System.exit
        // (as it typically yet unjustly does in known implementations)
        return true;
    }
}
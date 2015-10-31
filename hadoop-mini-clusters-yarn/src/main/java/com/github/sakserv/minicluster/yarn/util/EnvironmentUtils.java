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
package com.github.sakserv.minicluster.yarn.util;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleg Zhurakousky
 *
 */
public class EnvironmentUtils {

    /**
     * Allows dynamic update to the environment variables.
     *
     * @param key
     * @param value
     */
    public static void put(String key, String value) throws Exception {
        Map<String, String> environemnt = new HashMap<String, String>(System.getenv());
        environemnt.put(key, value);
        updateEnvironment(environemnt);
    }

    public static synchronized void putAll(Map<String, String> additionalEnvironment) throws Exception {
        Map<String, String> environemnt = new HashMap<String, String>(System.getenv());
        environemnt.putAll(additionalEnvironment);
        updateEnvironment(environemnt);
    }

    /**
     *
     * @param newenv
     */
    @SuppressWarnings("unchecked")
    private static void updateEnvironment(Map<String, String> environemnt) throws Exception {

            Class<?>[] classes = Collections.class.getDeclaredClasses();
            for (Class<?> clazz : classes) {
                if ("java.util.Collections$UnmodifiableMap".equals(clazz.getName())) {
                    Field field = ReflectionUtils.getFieldAndMakeAccessible(clazz, "m");
                    Object obj = field.get(System.getenv());
                    Map<String, String> map = (Map<String, String>) obj;
                    map.clear();
                    map.putAll(environemnt);
                }
            }

    }
}
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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class ObjectAssertUtils {
    /**
     *
     * @param value
     */
    public static void assertNotNull(Object value){
        if (value == null){
            throw new IllegalArgumentException("'value' must not be null");
        }
    }

    /**
     * Asserts if the provided class is Java Main. That is if it contains
     * 'public static void main(String[] args)'
     *
     * @param clazz
     */
    public static void assertIsMain(Class<?> clazz){
        boolean valid = false;
        ObjectAssertUtils.assertNotNull(clazz);
        try {
            Method m = clazz.getMethod("main", new Class[] {String[].class});
            Class<?> returnType = m.getReturnType();
            if (m.getModifiers() == (Modifier.STATIC | Modifier.PUBLIC) && returnType.getName().equals("void")){
                valid = true;
            }
        } catch (Exception e) {
            // ignore
        }
        if (!valid){
            throw new IllegalArgumentException("Provided class is not does not contain 'public static void main(String[] args) signature'");
        }
    }
}
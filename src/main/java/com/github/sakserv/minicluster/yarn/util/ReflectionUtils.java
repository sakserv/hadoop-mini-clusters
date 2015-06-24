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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author Oleg Zhurakousky
 *
 */
public class ReflectionUtils {

    /**
     *
     * @param clazz
     * @param name
     * @param arguments
     * @return
     */
    public static Method getMethodAndMakeAccessible(Class<?> clazz, String name, Class<?>... arguments) {

        try {
            Method m = org.springframework.util.ReflectionUtils.findMethod(clazz, name, arguments);
            m.setAccessible(true);
            return m;
        }
        catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     *
     * @param clazz
     * @param fieldName
     * @return
     */
    public static Field getFieldAndMakeAccessible(Class<?> clazz, String fieldName) {
        Class<?> searchType = clazz;
        while (!Object.class.equals(searchType) && searchType != null) {
            Field[] fields = searchType.getDeclaredFields();
            for (Field field : fields) {
                if (fieldName == null || fieldName.equals(field.getName())) {
                    field.setAccessible(true);
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    /**
     *
     * @param instance
     * @param fieldName
     * @return
     */
    public static Object getFieldValue(Object instance, String fieldName) {
        Field f = getFieldAndMakeAccessible(instance.getClass(), fieldName);
        try {
            if (f != null){
                return f.get(instance);
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return null;
    }

    /**
     *
     * @param className
     * @return
     */
    public static Object newDefaultInstance(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
//			Object instance = clazz.newInstance();
            Object instance = constructor.newInstance();
            return instance;
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to create instance of " + className + " using default constructor", e);
        }
    }

    /**
     *
     * @param className
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> Constructor<T> getInvocableConstructor(String className, Class<?>... parameterTypes) {
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<T> constructor = (Constructor<T>) clazz.getDeclaredConstructor(parameterTypes);
            constructor.setAccessible(true);
            return constructor;
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to create instance of " + className + " using default constructor", e);
        }
    }
}
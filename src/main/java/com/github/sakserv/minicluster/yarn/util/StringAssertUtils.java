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

import org.springframework.util.Assert;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class StringAssertUtils {

    /**
     *
     * @param value
     */
    public static void assertNotEmptyAndNoSpaces(String value){
        Assert.hasText(value);
        if (value.contains(" ")){
            throw new IllegalArgumentException("'value' must not contain spaces");
        }
    }

    /**
     *
     * @param value
     * @param endsWith
     */
    public static void assertNotEmptyAndNoSpacesAndEndsWith(String value, String endsWith){
        assertNotEmptyAndNoSpaces(value);
        if (!value.endsWith(endsWith)){
            throw new IllegalArgumentException("'value' must end with '" + endsWith + "'");
        }
    }

    /**
     *
     * @param value
     * @return
     */
    public static boolean isEmpty(String value) {
        return value == null || value.equals("");
    }
}
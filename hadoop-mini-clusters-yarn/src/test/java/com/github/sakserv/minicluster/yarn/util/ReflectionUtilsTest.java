package com.github.sakserv.minicluster.yarn.util;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
 */public class ReflectionUtilsTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    ReflectionUtils reflectionUtils = new ReflectionUtils();

    public class ReflectTester {

        private String foo = "baz";

        public ReflectTester() {
            setFoo("bar");
        }

        private String getFoo() {
            return foo;
        }

        private void setFoo(String foo) {
            this.foo = foo;
        }
    }

    @Test
    public void testGetMethodAndMakeAccessible() throws Exception {
        Method m = reflectionUtils.getMethodAndMakeAccessible(ReflectTester.class, "getFoo");
        assertTrue(m.isAccessible());
    }

    @Test
    public void testIllegalArgumentExceptionGetMethodAndMakeAccessible() {
        exception.expect(IllegalArgumentException.class);
        Method m = reflectionUtils.getMethodAndMakeAccessible(ReflectTester.class, "getBar");
    }

    @Test
    public void testGetFieldAndMakeAccessible() throws Exception {
        Field f = reflectionUtils.getFieldAndMakeAccessible(ReflectTester.class, "foo");
        assertTrue(f.isAccessible());
    }

    @Test
    public void testVoidGetFieldAndMakeAccessible() {
        assertNull(reflectionUtils.getFieldAndMakeAccessible(ReflectTester.class, "bar"));
    }

    @Test
    public void testGetFieldValue() throws Exception {
    }

    @Test
    public void testGetInvocableConstructor() throws Exception {

    }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sakserv.minicluster.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.Proxy;

import org.junit.Test;

public class HttpUtilsTest {

    @Test
    public void testReturnProxyIfProxyPropsAreSetToNull() {
        System.clearProperty("HTTP_PROXY");
        System.clearProperty("ALL_PROXY");
        assertNull(HttpUtils.returnProxyIfEnabled());
    }

    @Test
    public void testReturnProxyIfHTTPProxyIsSet() {
        System.setProperty("HTTP_PROXY", "http://104.207.145.113:3128");
        System.clearProperty("ALL_PROXY");
        assertNotNull(HttpUtils.returnProxyIfEnabled());
        assertEquals("/104.207.145.113:3128", HttpUtils.returnProxyIfEnabled().address().toString());
        assertEquals(Proxy.Type.HTTP, HttpUtils.returnProxyIfEnabled().type());
    }

    @Test
    public void testReturnProxyIfSOCKProxyIsSet() {
        System.setProperty("HTTP_PROXY", "sock5://207.98.253.161:10200");
        System.clearProperty("ALL_PROXY");
        assertNotNull(HttpUtils.returnProxyIfEnabled());
        assertEquals("/207.98.253.161:10200", HttpUtils.returnProxyIfEnabled().address().toString());
        assertEquals(Proxy.Type.SOCKS, HttpUtils.returnProxyIfEnabled().type());
    }

    @Test
    public void testReturnProxyIfSOCKProxyIsSetGnomeClient() {
        System.clearProperty("HTTP_PROXY");
        System.setProperty("ALL_PROXY", "sock5://207.98.253.161:10200");
        assertNotNull(HttpUtils.returnProxyIfEnabled());
        assertEquals("/207.98.253.161:10200", HttpUtils.returnProxyIfEnabled().address().toString());
        assertEquals(Proxy.Type.SOCKS, HttpUtils.returnProxyIfEnabled().type());
    }

    @Test
    public void testReturnProxyIfHTTPProxyIsSetGnomeClient() {
        System.clearProperty("HTTP_PROXY");
        System.setProperty("ALL_PROXY", "104.207.145.113:3128");
        assertNotNull(HttpUtils.returnProxyIfEnabled());
        assertEquals("/104.207.145.113:3128", HttpUtils.returnProxyIfEnabled().address().toString());
        assertEquals(Proxy.Type.HTTP, HttpUtils.returnProxyIfEnabled().type());
    }

    @Test
    public void testReturnProxyIfProxyHasInvalidUrl() {
        System.setProperty("HTTP_PROXY", "104.207.145.113");
        System.clearProperty("ALL_PROXY");
        assertNull(HttpUtils.returnProxyIfEnabled());
    }

    @Test
    public void testReturnProxyIfProxyHasInvalidUrlWithoutPort() {
        System.setProperty("HTTP_PROXY", "http104.207.145.113");
        System.clearProperty("ALL_PROXY");
        assertNull(HttpUtils.returnProxyIfEnabled());
    }
}
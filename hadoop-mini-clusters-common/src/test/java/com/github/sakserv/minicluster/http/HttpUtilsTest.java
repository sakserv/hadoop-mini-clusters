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
		System.setProperty("HTTP_PROXY","http://104.207.145.113:3128");
		System.clearProperty("ALL_PROXY");
		assertNotNull(HttpUtils.returnProxyIfEnabled());
		assertEquals("/104.207.145.113:3128",HttpUtils.returnProxyIfEnabled().address().toString());
		assertEquals(Proxy.Type.HTTP,HttpUtils.returnProxyIfEnabled().type());
	}
	
	@Test
	public void testReturnProxyIfSOCKProxyIsSet() {
		System.setProperty("HTTP_PROXY","sock5://207.98.253.161:10200");
		System.clearProperty("ALL_PROXY");
		assertNotNull(HttpUtils.returnProxyIfEnabled());
		assertEquals("/207.98.253.161:10200",HttpUtils.returnProxyIfEnabled().address().toString());
		assertEquals(Proxy.Type.SOCKS,HttpUtils.returnProxyIfEnabled().type());
	}
	
	@Test
	public void testReturnProxyIfSOCKProxyIsSetGnomeClient() {
		System.clearProperty("HTTP_PROXY");
		System.setProperty("ALL_PROXY", "sock5://207.98.253.161:10200");
		assertNotNull(HttpUtils.returnProxyIfEnabled());
		assertEquals("/207.98.253.161:10200",HttpUtils.returnProxyIfEnabled().address().toString());
		assertEquals(Proxy.Type.SOCKS,HttpUtils.returnProxyIfEnabled().type());
	}
	
	@Test
	public void testReturnProxyIfHTTPProxyIsSetGnomeClient() {
		System.clearProperty("HTTP_PROXY");
		System.setProperty("ALL_PROXY","104.207.145.113:3128");
		assertNotNull(HttpUtils.returnProxyIfEnabled());
		assertEquals("/104.207.145.113:3128",HttpUtils.returnProxyIfEnabled().address().toString());
		assertEquals(Proxy.Type.HTTP,HttpUtils.returnProxyIfEnabled().type());
	}
	
	@Test
	public void testReturnProxyIfProxyHasInvalidUrl() {
		System.setProperty("HTTP_PROXY","104.207.145.113");
		System.clearProperty("ALL_PROXY");
		assertNull(HttpUtils.returnProxyIfEnabled());
	}
	
	@Test
	public void testReturnProxyIfProxyHasInvalidUrlWithoutPort() {
		System.setProperty("HTTP_PROXY","http104.207.145.113");
		System.clearProperty("ALL_PROXY");
		assertNull(HttpUtils.returnProxyIfEnabled());
	}
}

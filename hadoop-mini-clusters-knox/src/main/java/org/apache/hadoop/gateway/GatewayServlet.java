/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.gateway;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Enumeration;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.gateway.audit.api.Action;
import org.apache.hadoop.gateway.audit.api.ActionOutcome;
import org.apache.hadoop.gateway.audit.api.AuditService;
import org.apache.hadoop.gateway.audit.api.AuditServiceFactory;
import org.apache.hadoop.gateway.audit.api.Auditor;
import org.apache.hadoop.gateway.audit.api.ResourceType;
import org.apache.hadoop.gateway.audit.log4j.audit.AuditConstants;
import org.apache.hadoop.gateway.descriptor.GatewayDescriptor;
import org.apache.hadoop.gateway.descriptor.GatewayDescriptorFactory;
import org.apache.hadoop.gateway.filter.AbstractGatewayFilter;
import org.apache.hadoop.gateway.i18n.messages.MessagesFactory;
import org.apache.hadoop.gateway.i18n.resources.ResourcesFactory;

public class GatewayServlet implements Servlet, Filter {

    public static final String GATEWAY_DESCRIPTOR_LOCATION_DEFAULT = "gateway.xml";
    public static final String GATEWAY_DESCRIPTOR_LOCATION_PARAM = "gatewayDescriptorLocation";

    private static final GatewayResources res = ResourcesFactory.get( GatewayResources.class );
    private static final GatewayMessages LOG = MessagesFactory.get( GatewayMessages.class );

    private static AuditService auditService = AuditServiceFactory.getAuditService();
    private static Auditor auditor = AuditServiceFactory.getAuditService()
            .getAuditor( AuditConstants.DEFAULT_AUDITOR_NAME,
                    AuditConstants.KNOX_SERVICE_NAME, AuditConstants.KNOX_COMPONENT_NAME );

    private FilterConfigAdapter filterConfig;
    private volatile GatewayFilter filter;

    public GatewayServlet( GatewayFilter filter ) {
        this.filterConfig = null;
        this.filter = filter;
    }

    public GatewayServlet() {
        this( null );
    }

    public synchronized GatewayFilter getFilter() {
        return filter;
    }

    public synchronized void setFilter( GatewayFilter filter ) throws ServletException {
        Filter prev = filter;
        if( filterConfig != null ) {
            filter.init( filterConfig );
        }
        this.filter = filter;
        if( prev != null && filterConfig != null ) {
            prev.destroy();
        }
    }

    @Override
    public synchronized void init( ServletConfig servletConfig ) throws ServletException {
        try {
            if( filter == null ) {
                filter = createFilter( servletConfig );
            }
            filterConfig = new FilterConfigAdapter( servletConfig );
            if( filter != null ) {
                filter.init( filterConfig );
            }
        } catch( ServletException e ) {
            LOG.failedToInitializeServletInstace( e );
            throw e;
        } catch( RuntimeException e ) {
            LOG.failedToInitializeServletInstace( e );
            throw e;
        }
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException {
        try {
            if( filter == null ) {
                filter = createFilter( filterConfig );
            }
            if( filter != null ) {
                filter.init( filterConfig );
            }
        } catch( ServletException e ) {
            LOG.failedToInitializeServletInstace( e );
            throw e;
        } catch( RuntimeException e ) {
            LOG.failedToInitializeServletInstace( e );
            throw e;
        }
    }

    @Override
    public ServletConfig getServletConfig() {
        return filterConfig.getServletConfig();
    }

    @Override
    public void service( ServletRequest servletRequest, ServletResponse servletResponse ) throws ServletException, IOException {
        try {
            auditService.createContext();
            GatewayFilter f = filter;
            if( f != null ) {
                try {
                    f.doFilter( servletRequest, servletResponse, null );
                } catch( IOException e ) {
                    LOG.failedToExecuteFilter( e );
                    throw e;
                } catch( ServletException e ) {
                    LOG.failedToExecuteFilter( e );
                    throw e;
                } catch( RuntimeException e ) {
                    LOG.failedToExecuteFilter( e );
                    throw e;
                }
            } else {
                ((HttpServletResponse)servletResponse).setStatus( HttpServletResponse.SC_SERVICE_UNAVAILABLE );
            }
            String requestUri = (String)servletRequest.getAttribute( AbstractGatewayFilter.SOURCE_REQUEST_CONTEXT_URL_ATTRIBUTE_NAME );
            //int status = ((HttpServletResponse)servletResponse).getStatus();
            //auditor.audit( Action.ACCESS, requestUri, ResourceType.URI, ActionOutcome.SUCCESS, res.responseStatus( status ) );
        } finally {
            auditService.detachContext();
        }
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain ) throws IOException, ServletException {
        try {
            auditService.createContext();
            GatewayFilter f = filter;
            if( f != null ) {
                try {
                    f.doFilter( servletRequest, servletResponse );
                    //TODO: This should really happen naturally somehow as part of being a filter.  This way will cause problems eventually.
                    chain.doFilter( servletRequest, servletResponse );
                } catch( IOException e ) {
                    LOG.failedToExecuteFilter( e );
                    throw e;
                } catch( ServletException e ) {
                    LOG.failedToExecuteFilter( e );
                    throw e;
                } catch( RuntimeException e ) {
                    LOG.failedToExecuteFilter( e );
                    throw e;
                }
            } else {
                ((HttpServletResponse)servletResponse).setStatus( HttpServletResponse.SC_SERVICE_UNAVAILABLE );
            }
            String requestUri = (String)servletRequest.getAttribute( AbstractGatewayFilter.SOURCE_REQUEST_CONTEXT_URL_ATTRIBUTE_NAME );
            //int status = ((HttpServletResponse)servletResponse).;
            //auditor.audit( Action.ACCESS, requestUri, ResourceType.URI, ActionOutcome.SUCCESS, res.responseStatus( status ) );
        } finally {
            auditService.detachContext();
        }
    }

    @Override
    public String getServletInfo() {
        return res.gatewayServletInfo();
    }

    @Override
    public synchronized void destroy() {
        if( filter != null ) {
            filter.destroy();
        }
        filter = null;
    }

    private static GatewayFilter createFilter( InputStream stream ) throws ServletException {
        try {
            GatewayFilter filter = null;
            if( stream != null ) {
                try {
                    GatewayDescriptor descriptor = GatewayDescriptorFactory.load( "xml", new InputStreamReader( stream ) );
                    filter = GatewayFactory.create( descriptor );
                } finally {
                    stream.close();
                }
            }
            return filter;
        } catch( IOException e ) {
            throw new ServletException( e );
        } catch( URISyntaxException e ) {
            throw new ServletException( e );
        }
    }

    private static GatewayFilter createFilter( FilterConfig filterConfig ) throws ServletException {
        GatewayFilter filter;
        InputStream stream;
        String location = filterConfig.getInitParameter( GATEWAY_DESCRIPTOR_LOCATION_PARAM );
        if( location != null ) {
            stream = filterConfig.getServletContext().getResourceAsStream( location );
            if( stream == null ) {
                stream = filterConfig.getServletContext().getResourceAsStream( "/WEB-INF/" + location );
            }
        } else {
            stream = filterConfig.getServletContext().getResourceAsStream( GATEWAY_DESCRIPTOR_LOCATION_DEFAULT );
        }
        filter = createFilter( stream );
        return filter;
    }

    private static GatewayFilter createFilter( ServletConfig servletConfig ) throws ServletException {
        GatewayFilter filter;
        InputStream stream;
        String location = servletConfig.getInitParameter( GATEWAY_DESCRIPTOR_LOCATION_PARAM );
        if( location != null ) {
            stream = servletConfig.getServletContext().getResourceAsStream( location );
            if( stream == null ) {
                stream = servletConfig.getServletContext().getResourceAsStream( "/WEB-INF/" + location );
            }
        } else {
            stream = servletConfig.getServletContext().getResourceAsStream( GATEWAY_DESCRIPTOR_LOCATION_DEFAULT );
        }
        filter = createFilter( stream );
        return filter;
    }

    private class FilterConfigAdapter implements FilterConfig {

        private ServletConfig config;

        private FilterConfigAdapter( ServletConfig config ) {
            this.config = config;
        }

        private ServletConfig getServletConfig() {
            return config;
        }

        @Override
        public String getFilterName() {
            return config.getServletName();
        }

        @Override
        public ServletContext getServletContext() {
            return config.getServletContext();
        }

        @Override
        public String getInitParameter( String name ) {
            return config.getInitParameter( name );
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return config.getInitParameterNames();
        }
    }

}
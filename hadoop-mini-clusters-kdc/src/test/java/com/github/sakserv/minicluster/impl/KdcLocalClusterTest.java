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

package com.github.sakserv.minicluster.impl;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KdcLocalClusterTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KdcLocalClusterTest.class);

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static KdcLocalCluster kdcLocalCluster;

    @BeforeClass
    public static void setUp() {
        kdcLocalCluster = new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }

    @Test
    public void testKdcPort() {
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)),
                (int) kdcLocalCluster.getPort());
    }

    @Test
    public void testKdcHost() {
        assertEquals(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY),
                kdcLocalCluster.getHost());
    }

    @Test
    public void testMissingKdcHost() {
        exception.expect(IllegalArgumentException.class);
        new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }

    @Test
    public void testKdcBaseDir() {
        assertEquals(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY),
                kdcLocalCluster.getBaseDir());
    }

    @Test
    public void testMissingKdcBaseDir() {
        exception.expect(IllegalArgumentException.class);
        new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }

    @Test
    public void testKdcDomain() {
        assertEquals(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY),
                kdcLocalCluster.getOrgDomain());
    }

    @Test
    public void testMissingKdcDomain() {
        exception.expect(IllegalArgumentException.class);
        new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(null)
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }

    @Test
    public void testKdcName() {
        assertEquals(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY),
                kdcLocalCluster.getOrgDomain());
    }

    @Test
    public void testMissingKdcName() {
        exception.expect(IllegalArgumentException.class);
        new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(null)
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }

    @Test
    public void testKdcPrincipals() {
        assertEquals(Arrays.asList(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(",")),
                kdcLocalCluster.getPrincipals());
    }

    @Test
    public void testMissingKdcPrincipals() {
        exception.expect(IllegalArgumentException.class);
        new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals((List<String>) null)
                .setKrbInstance(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY))
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }

    @Test
    public void testKdcKrbInstance() {
        assertEquals(propertyParser.getProperty(ConfigVars.KDC_KRBINSTANCE_KEY),
                kdcLocalCluster.getKrbInstance());
    }

    @Test
    public void testMissingKdcKrbInstance() {
        exception.expect(IllegalArgumentException.class);
        new KdcLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_PORT_KEY)))
                .setHost(propertyParser.getProperty(ConfigVars.KDC_HOST_KEY))
                .setBaseDir(propertyParser.getProperty(ConfigVars.KDC_BASEDIR_KEY))
                .setOrgDomain(propertyParser.getProperty(ConfigVars.KDC_ORG_DOMAIN_KEY))
                .setOrgName(propertyParser.getProperty(ConfigVars.KDC_ORG_NAME_KEY))
                .setPrincipals(propertyParser.getProperty(ConfigVars.KDC_PRINCIPALS_KEY).split(","))
                .setKrbInstance(null)
                .setInstance(propertyParser.getProperty(ConfigVars.KDC_INSTANCE_KEY))
                .setTransport(propertyParser.getProperty(ConfigVars.KDC_TRANSPORT))
                .setMaxTicketLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_TICKET_LIFETIME_KEY)))
                .setMaxRenewableLifetime(Integer.parseInt(propertyParser.getProperty(ConfigVars.KDC_MAX_RENEWABLE_LIFETIME)))
                .setDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.KDC_DEBUG)))
                .build();
    }
}

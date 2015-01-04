package com.github.skumpf.minicluster;

import com.github.skumpf.minicluster.impl.ZookeeperLocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by skumpf on 12/30/14.
 */
public class ZookeeperLocalClusterTest {

    ZookeeperLocalCluster zkCluster;

    @Before
    public void setUp() {
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();
    }

    @After
    public void tearDown() {
        zkCluster.stop(true);
    }

    @Test
    public void testZookeeperCluster() {
        zkCluster.dumpConfig();
    }

}

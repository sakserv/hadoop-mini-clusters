package com.github.sakserv.minicluster.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

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
 */public class LocalSystemTimeTest {

    @Test
    public void testMilliseconds() throws Exception {
        LocalSystemTime localSystemTime = new LocalSystemTime();
        Long millis = Long.valueOf(localSystemTime.milliseconds());
        assertTrue(millis instanceof Long);
    }

    @Test
    public void testNanoseconds() throws Exception {
        LocalSystemTime localSystemTime = new LocalSystemTime();
        Long nanos = Long.valueOf(localSystemTime.nanoseconds());
        assertTrue(nanos instanceof Long);
    }

    @Test
    public void testSleep() throws Exception {
        LocalSystemTime localSystemTime = new LocalSystemTime();
        localSystemTime.sleep(500);
    }

    @Test
    public void testSleepInterrupt() throws Exception {
        Thread t = new Thread(new LocalSystemTimeTest.LocalSystemTimeThread());
        t.start();
        t.interrupt();
    }

    private static class LocalSystemTimeThread implements Runnable {
        @Override
        public void run() {
            LocalSystemTime localSystemTime = new LocalSystemTime();
            localSystemTime.sleep(10000);
        }
    }
}
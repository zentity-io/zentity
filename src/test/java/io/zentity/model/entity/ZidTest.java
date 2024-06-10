/*
 * zentity
 * Copyright © 2018-2024 Dave Moore
 * https://zentity.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zentity.model.entity;

import org.junit.Assert;
import org.junit.Test;

public class ZidTest {

    @Test
    public void testEncodeEntity() {
        String _zid = Zid.encode("person", "my_index", "1", 0);
        String expected = "person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeEntityCrossClusterSearch() {
        String _zid = Zid.encode("person", "us:my_index", "1", 0);
        String expected = "person|us:my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }
}

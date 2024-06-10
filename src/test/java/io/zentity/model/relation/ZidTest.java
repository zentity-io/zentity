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
package io.zentity.model.relation;

import io.zentity.model.ValidationException;
import org.junit.Assert;
import org.junit.Test;

public class ZidTest {

    @Test
    public void testEncodeRelation() throws Exception {
        String _zid = Zid.encode("resides", "a>b","person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "resides#a<b#address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationBidirectional() throws Exception {
        String _zid = Zid.encode("resides", "a<>b","person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "resides#a<>b#address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationEmptyType() throws Exception {
        String _zid = Zid.encode("", "a>b","person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "#a<b#address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationNullType() throws Exception {
        String _zid = Zid.encode(null, "a>b","person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "#a<b#address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationEmptyDirection() throws Exception {
        String _zid = Zid.encode("resides", "","person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "resides##address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationNullDirection() throws Exception {
        String _zid = Zid.encode("resides", null,"person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "resides##address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationEmptyTypeAndDirection() throws Exception {
        String _zid = Zid.encode("", "","person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "##address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationNullTypeAndDirection() throws Exception {
        String _zid = Zid.encode(null, null,"person|my_index|MQ==|0", "address|my_index|MQ==|0");
        String expected = "##address|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationOrder() throws Exception {
        String _zid = Zid.encode("owns", "a<b","person|my_index|MQ==|0", "phone|my_index|MQ==|0");
        String expected = "owns#a<b#person|my_index|MQ==|0#phone|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationOrderBidirectional() throws Exception {
        String _zid = Zid.encode("owns", "a<>b","person|my_index|MQ==|0", "phone|my_index|MQ==|0");
        String expected = "owns#a<>b#person|my_index|MQ==|0#phone|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationOrderReverse() throws Exception {
        String _zid = Zid.encode("owns", "a>b","person|my_index|MQ==|0", "card|my_index|MQ==|0");
        String expected = "owns#a<b#card|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationOrderReverseBidirectional() throws Exception {
        String _zid = Zid.encode("owns", "a<>b","person|my_index|MQ==|0", "card|my_index|MQ==|0");
        String expected = "owns#a<>b#card|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationOrderReverseEmptyDirection() throws Exception {
        String _zid = Zid.encode("owns", "","person|my_index|MQ==|0", "card|my_index|MQ==|0");
        String expected = "owns##card|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test
    public void testEncodeRelationOrderReverseNullDirection() throws Exception {
        String _zid = Zid.encode("owns", null,"person|my_index|MQ==|0", "card|my_index|MQ==|0");
        String expected = "owns##card|my_index|MQ==|0#person|my_index|MQ==|0";
        Assert.assertEquals(expected, _zid);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidRelationType() throws Exception {
        Zid.encode("resides", "A ? B","person|my_index|MQ==|0", "address|my_index|MQ==|0");
    }
}

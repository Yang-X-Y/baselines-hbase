/*
 * Copyright 2012 Shoji Nishimura
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package tiny.mdhbase;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author shoji
 *
 */
public class RangeFilter extends FilterBase {

    private Range rx;
    private Range ry;
    private boolean filterRow = true;

    public RangeFilter(final Range rx, final Range ry) {
        this.rx = rx;
        this.ry = ry;
    }

    @Override
    public byte[] toByteArray() {
        byte[] rxMin = Bytes.toBytes(rx.min);
        byte[] rxMax = Bytes.toBytes(rx.max);
        byte[] ryMin = Bytes.toBytes(ry.min);
        byte[] ryMax = Bytes.toBytes(ry.min);
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.put(rxMin);
        buf.put(rxMax);
        buf.put(ryMin);
        buf.put(ryMax);
        return buf.array();
    }
    //0.解析传入参数, 初始化filter;
    public static RangeFilter parseFrom(final byte[] bytes) {
        byte[] rxMin = new byte[4];
        byte[] rxMax = new byte[4];
        byte[] ryMin = new byte[4];
        byte[] ryMax = new byte[4];

        System.arraycopy(bytes, 0, rxMin, 0, 4);
        System.arraycopy(bytes, 4, rxMax, 0, 4);
        System.arraycopy(bytes, 8, ryMin, 0, 4);
        System.arraycopy(bytes, 12, ryMax, 0, 4);
        Range rangX = new Range(Bytes.toInt(rxMin),Bytes.toInt(rxMax));
        Range rangY = new Range(Bytes.toInt(ryMin),Bytes.toInt(ryMax));
        return new RangeFilter(rangX,rangY);
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        byte[] value = CellUtil.cloneValue(cell);
        int x = Bytes.toInt(value, 0);
        int y = Bytes.toInt(value, 4);
        if (rx.include(x) && ry.include(y)) {
            return ReturnCode.INCLUDE;
        } else {
            return ReturnCode.NEXT_ROW;
        }
    }

}

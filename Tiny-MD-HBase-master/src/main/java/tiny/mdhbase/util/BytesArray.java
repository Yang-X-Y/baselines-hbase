package tiny.mdhbase.util;

import javax.validation.constraints.NotNull;

public class BytesArray implements Comparable<BytesArray> {

    private byte[][] bytesArr;

    public BytesArray(byte[][] arr){
        this.bytesArr = arr;
    }

    @Override
    public int compareTo(@NotNull BytesArray o) {
        for(int i = 0 ; i < bytesArr.length && i < o.bytesArr.length; i++){
            int result;
            result = ByteArrayUtils.lexicographicalComparator(bytesArr[i], o.bytesArr[i]);
//            if (i==0) {
//                result = ByteArrayUtils.compareRowKey(bytesArr[i], o.bytesArr[i]);
//            } else {
//                result = ByteArrayUtils.compare(bytesArr[i], o.bytesArr[i]);
//            }
            if(result != 0)
                return result;
        }
        return bytesArr.length - o.bytesArr.length;
    }
}



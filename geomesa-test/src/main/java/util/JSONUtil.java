package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;

public class JSONUtil {

    /**
     * Read text file from local disk. Only for small file.
     *
     * @param path file path on local disk.
     * @return string of file content.
     */
    public static JSONObject readLocalJSONFile(String path) {
        File file = new File(path);
        StringBuilder sb = new StringBuilder();
        try {
            Reader reader = new InputStreamReader(new FileInputStream(file));
            BufferedReader br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JSONObject.parseObject(sb.toString());
    }

    /**
     * 读取HDFS文件
     *
     * @param path 文件路径
     * @param conf HDFS配置
     */
    public static String readHDFSFile(String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path);
        FSDataInputStream inStream = fs.open(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

        StringBuilder text = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            text.append(line);
        }
        inStream.close();
        br.close();

        return text.toString();
    }

    /**
     * 写文件到HDFS
     *
     * @param text 写入的文件内容
     * @param path 文件路径
     * @param conf HDFS配置
     */
    public static void writeHDFSText(String text, String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path);

        FSDataOutputStream outStream = fs.create(file);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream));
        bw.write(text);
        bw.flush();
        bw.close();
        outStream.close();
    }
}

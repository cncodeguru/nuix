package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.FileOutputStream;

public class S3Util {

    public static void downloadFile(AmazonS3 s3, String bucket, String key, String destDir) throws Exception {
        try {
            S3Object o = s3.getObject(bucket, key);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(new File(destDir, key));
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (Exception e) {
            throw new Exception("Failed to download " + key, e);
        }
    }

    public static void uploadFile(AmazonS3 s3, String bucket, String key, String file) throws Exception {
        try {
            s3.putObject(bucket, key, new File(file));
        } catch (Exception e) {
            throw new Exception("Failed to upload " + key, e);
        }
    }
}

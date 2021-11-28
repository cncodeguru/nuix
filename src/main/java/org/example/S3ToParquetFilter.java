package org.example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3ToParquetFilter {

    private static final Logger log = LoggerFactory.getLogger(S3ToParquetFilter.class);

    public static final String REGION = "ap-southeast-2";

    public static final String BUCKET = "candidate-103-s3-bucket";

    public static final String WORK_DIR = "/nuix";

    public static final String SEARCH_TEXT = "ellipsis";

    private String bucket;

    private String workDir;

    private String searchText;

    private AmazonS3 s3;

    private ParquetUtil parquetUtil;

    public static void main(String[] args) {
        S3ToParquetFilter filter = new S3ToParquetFilter(REGION, BUCKET, WORK_DIR, SEARCH_TEXT);
        filter.run();
    }

    public S3ToParquetFilter(String region, String bucket, String workDir, String searchText) {
        this.bucket = bucket;
        this.workDir = workDir;
        this.searchText = searchText;

        File wd = new File(workDir);
        if (!wd.isDirectory() && !wd.mkdirs()) {
            throw new RuntimeException("Cannot create work folder " + workDir);
        }

        s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(region)).build();
        parquetUtil = new ParquetUtil(null);
    }

    private void run() {
        ListObjectsV2Result result = s3.listObjectsV2(bucket);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            String key = os.getKey();
            if (key.toLowerCase().endsWith(".zip")) {
                log.info("Found {}", key);
                try {
                    S3Util.downloadFile(s3, bucket, key, workDir);
                    log.info("Downloaded {}", key);
                    List<File> unzippedFiles = ZipUtil.unzip(workDir, key);
                    log.info("Unzipped {}", key);
                    for (File csvFile : unzippedFiles) {
                        processCsvFile(csvFile);
                    }
                } catch (Exception e) {
                    log.error("Failed to process " + key, e);
                }
            }
        }
        log.info("Done.");
    }

    private void processCsvFile(File csvFile) {
        try {
            List<String> linesFound = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            String line = reader.readLine();
            while (line != null) {
                if (line.toLowerCase().contains(this.searchText)) {
                    linesFound.add(line);
                }
                line = reader.readLine();
            }
            reader.close();
            if (linesFound.isEmpty()) {
                return;
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile, false));
            for (String l : linesFound) {
                writer.write(l);
            }
            writer.close();
            String parquetFile = csvFile.getAbsolutePath().substring(0, csvFile.getAbsolutePath().lastIndexOf(".")) + ".parquet";
            parquetUtil.convert(Arrays.asList(csvFile.getAbsolutePath()), parquetFile);
            log.info("Parquet file {} generated.", parquetFile);
            String key = new File(parquetFile).getName();
            S3Util.uploadFile(s3, this.bucket, key, parquetFile);
            log.info("Parquet file {} uploaded.", key);
        } catch (Exception e) {
            log.error("Failed to process " + csvFile.getPath(), e);
        }
    }

}

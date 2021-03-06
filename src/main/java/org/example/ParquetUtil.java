package org.example;

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.csv.AvroCSV;
import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

public class ParquetUtil extends BaseCommand {

    private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);

    public ParquetUtil(Logger console) {
        super(log);
    }

    @Parameter(description = "<csv path>")
    List<String> targets;

    @Parameter(
            names = {"-o", "--output"},
            description = "Output file path",
            required = true)
    String outputPath = null;

    @Parameter(
            names = {"-2", "--format-version-2", "--writer-version-2"},
            description = "Use Parquet format version 2",
            hidden = true)
    boolean v2 = false;

    @Parameter(names = "--delimiter", description = "Delimiter character")
    String delimiter = ",";

    @Parameter(names = "--escape", description = "Escape character")
    String escape = "\\";

    @Parameter(names = "--quote", description = "Quote character")
    String quote = "\"";

    @Parameter(names = "--no-header", description = "Don't use first line as CSV header")
    boolean noHeader = false;

    @Parameter(names = "--skip-lines", description = "Lines to skip before CSV start")
    int linesToSkip = 0;

    @Parameter(names = "--charset", description = "Character set name", hidden = true)
    String charsetName = Charset.defaultCharset().displayName();

    @Parameter(names = "--header",
            description = "Line to use as a header. Must match the CSV settings.")
    String header;

    @Parameter(names = "--require",
            description = "Do not allow null values for the given field")
    List<String> requiredFields;

    @Parameter(names = {"-s", "--schema"},
            description = "The file containing the Avro schema.")
    String avroSchemaFile;

    @Parameter(names = {"--compression-codec"},
            description = "A compression codec name.")
    String compressionCodecName = "GZIP";

    @Parameter(names = "--row-group-size", description = "Target row group size")
    int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

    @Parameter(names = "--page-size", description = "Target page size")
    int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

    @Parameter(names = "--dictionary-size", description = "Max dictionary page size")
    int dictionaryPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

    @Parameter(
            names = {"--overwrite"},
            description = "Remove any data already in the target view or dataset")
    boolean overwrite = true;

    @Override
    @SuppressWarnings("unchecked")
    public int run() throws IOException {
        Preconditions.checkArgument(targets != null && targets.size() == 1,
                "CSV path is required.");

        CSVProperties props = new CSVProperties.Builder()
                .delimiter(delimiter)
                .escape(escape)
                .quote(quote)
                .header(header)
                .hasHeader(false)
                .linesToSkip(linesToSkip)
                .charset(charsetName)
                .build();

        String source = targets.get(0);

        Schema csvSchema;
        if (avroSchemaFile != null) {
            csvSchema = Schemas.fromAvsc(open(avroSchemaFile));
        } else {
            Set<String> required = ImmutableSet.of();
            if (requiredFields != null) {
                required = ImmutableSet.copyOf(requiredFields);
            }

            String filename = new File(source).getName();
            String recordName;
            if (filename.contains(".")) {
                recordName = filename.substring(0, filename.indexOf("."));
            } else {
                recordName = filename;
            }

            csvSchema = AvroCSV.inferNullableSchema(
                    recordName, open(source), props, required);
        }

        long count = 0;
        try (AvroCSVReader<GenericData.Record> reader = new AvroCSVReader<>(
                open(source), props, csvSchema, GenericData.Record.class, true)) {
            CompressionCodecName codec = Codecs.parquetCodec(compressionCodecName);
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                    .<GenericData.Record>builder(qualifiedPath(outputPath))
                    .withWriterVersion(v2 ? PARQUET_2_0 : PARQUET_1_0)
                    .withWriteMode(overwrite ?
                            ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE)
                    .withCompressionCodec(codec)
                    .withDictionaryEncoding(true)
                    .withDictionaryPageSize(dictionaryPageSize)
                    .withPageSize(pageSize)
                    .withRowGroupSize(rowGroupSize)
                    .withDataModel(GenericData.get())
                    .withConf(getConf())
                    .withSchema(csvSchema)
                    .build()) {
                for (GenericData.Record record : reader) {
                    writer.write(record);
                }
            } catch (RuntimeException e) {
                throw new RuntimeException("Failed on record " + count, e);
            }
        }

        return 0;
    }

    @Override
    public List<String> getExamples() {
        return new ArrayList<>();
    }

    public void convert(List<String> targets, String outputPath) throws Exception {
        this.targets = targets;
        this.outputPath = outputPath;
        this.run();
    }
}
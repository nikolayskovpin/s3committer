package com.netflix.bdp.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HBCPartitionedOutputCommitter extends S3PartitionedOutputCommitter {

    public HBCPartitionedOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    @Override
    protected Object findClient(Path path, Configuration conf) {
        String key = conf.get("fs.s3.cse.kms.keyId");
        return AmazonS3EncryptionClientBuilder.standard()
                .withEncryptionMaterials(new KMSEncryptionMaterialsProvider(key))
                .build();
    }
}

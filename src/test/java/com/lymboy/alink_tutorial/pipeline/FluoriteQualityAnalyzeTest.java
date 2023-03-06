package com.lymboy.alink_tutorial.pipeline;

import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.regression.RandomForestRegressor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;

/**
 * @author 小怪兽
 * @version 1.0
 * @since 2023-03-06
 */
public class FluoriteQualityAnalyzeTest {

    @DisplayName("训练所有模型")
    @Test
    @SneakyThrows(Exception.class)
    public void test245() {
        String parentDir = "E:\\LYM\\tmp\\FluoriteQuality";
        String[] datasets = new File(Paths.get(parentDir, "dataset").toString()).list();
        for (String dataset : datasets) {
            String datasetPath = Paths.get(parentDir, "dataset", dataset).toString();
            String modelName = dataset.substring(0, dataset.indexOf('.'));
            String modelPath = Paths.get(parentDir, "model",DigestUtil.md5Hex(modelName), "model.ak").toString();

            BatchOperator<?> source = new CsvSourceBatchOp()
                    .setSchemaStr("f0 double, f1 double, f2 double, f3 double, f4 double, y double")
                    .setIgnoreFirstLine(true)
                    .setFilePath(datasetPath);

            Pipeline pipeline = new Pipeline()
                    .add(new RandomForestRegressor()
                            .setFeatureCols("f0", "f1", "f2", "f3", "f4")
                            .setLabelCol("y")
                            .setPredictionCol("y_pred")
                            .setModelStreamFilePath(Paths.get("/tmp",  "fq_rf_models", DigestUtil.md5Hex(modelName)).toString())
                    );

            pipeline.fit(source).save(modelPath);
            BatchOperator.execute();
        }
    }

    @Test
    @SneakyThrows(Exception.class)
    public void test52() {
        for (int i = 0; i < 5; i++) {

            String s = DigestUtil.md5Hex("你好");
            System.out.println(s);
        }
    }
}

package com.lymboy.alink_tutorial.pipeline;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.regression.RandomForestRegressor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 小怪兽
 * @version 1.0
 * @since 2023-03-06
 */
@Slf4j
@Component
public class FluoriteQualityAnalyze implements InitializingBean {

    private String modelDir = "/tmp/fq_rf_models";
    private Map<String, LocalPredictor> predictorMap = new ConcurrentHashMap<>();

    private final int OUT_STEP = 3;

    private final int IN_STEP = 5;

    public static void main(String[] args) throws Exception {

    }

    void train() throws Exception {
        String parentDir = "E:\\LYM\\tmp\\FluoriteQuality";
        String[] datasets = new File(Paths.get(parentDir, "dataset").toString()).list();
        for (String dataset : datasets) {
            String datasetPath = Paths.get(parentDir, "dataset", dataset).toString();
            String modelName = dataset.substring(0, dataset.indexOf('.'));
            String modelPath = Paths.get(parentDir, "model", DigestUtil.md5Hex(modelName), "model.ak").toString();

            BatchOperator<?> source = new CsvSourceBatchOp()
                    .setSchemaStr("f0 double, f1 double, f2 double, f3 double, f4 double, y double")
                    .setIgnoreFirstLine(true)
                    .setFilePath(datasetPath);

            Pipeline pipeline = new Pipeline()
                    .add(new RandomForestRegressor()
                            .setFeatureCols("f0", "f1", "f2", "f3", "f4")
                            .setLabelCol("y")
                            .setPredictionCol("y_pred")
                            .setModelStreamFilePath(Paths.get("/tmp", "fq_rf_models", DigestUtil.md5Hex(modelName)).toString())
                    );

            pipeline.fit(source).save(modelPath);
            BatchOperator.execute();
        }
    }

    public List<Double> predict(String provider, String comp, List<Double> values) throws Exception {
        String modelName = DigestUtil.md5Hex(provider + "_" + comp);
        if (!predictorMap.containsKey(modelName)) {
            synchronized (this) {
                if (FileUtil.exist(Paths.get(modelDir, modelName, "model.ak").toString())) {
                    try {
                        predictorMap.put(
                                modelName,
                                new LocalPredictor(Paths.get(modelDir, modelName, "model.ak").toString(),
                                        "f0 double, f1 double, f2 double, f3 double, f4 double")
                        );
                    } catch (Exception e) {
                        log.info("模型导入出错，请联系技术人员检查！");
                        throw new RuntimeException("模型导入出错，请联系技术人员检查！");
                    }
                } else {
                    throw new IllegalArgumentException("指定的企业数据缺失或尚未建模，请检查！");
                }
            }
        }
        values = validate(values);

        LocalPredictor predictor = predictorMap.get(modelName);
        // 预测未来3次产品质量
        for (int i = 0; i < OUT_STEP; i++) {
            int len = values.size();
            Object[] ret = predictor.predict(values.subList(len - 5, len).toArray());
            values.add((Double) ret[ret.length - 1]);
        }
        int len = values.size();
        return values.subList(len - OUT_STEP, len);
    }

    @Override
    public void afterPropertiesSet() {
        loadAllModels();
    }

    private List<Double> validate(List<Double> data) {
        if (CollectionUtils.isEmpty(data)) {
            log.warn("输入数据为空");
            throw new IllegalArgumentException("输入数据为空");
        }
        int length = data.size();
        if (length < IN_STEP) {
            log.warn("数据步长不足{}", IN_STEP);
            throw new IllegalArgumentException("数据步长不足");
        }
        return data.subList(length - IN_STEP, length);
    }

    void loadAllModels() {
        //加载所有模型
        String[] paths = new File(modelDir).list();
        if (!ArrayUtil.isEmpty(paths)) {
            for (String path : paths) {
                String modelName = DigestUtil.md5Hex(path);
                try {
                    predictorMap.put(
                            modelName,
                            new LocalPredictor(Paths.get(modelDir, modelName, "model.ak").toString(),
                                    "f0 double, f1 double, f2 double, f3 double, f4 double")
                    );
                } catch (Exception e) {
                    throw new RuntimeException("模型导入出错，请联系技术人员检查！");
                }
            }
        }

    }
}

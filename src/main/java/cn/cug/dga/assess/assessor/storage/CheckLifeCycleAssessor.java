package cn.cug.dga.assess.assessor.storage;

import ch.qos.logback.core.spi.LifeCycle;
import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.utils.AssessParam;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * author song
 * date 2024/3/1 13:52
 * Desc 判断生命周期是否合理
 */
@Component("LIFECYCLE")
public class CheckLifeCycleAssessor extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {

        /**
         *  如果table_mata_info_extra的lifecycle_type类型为Unset 类型 则给0分
         *  如果设置了并且是永久 拉链表类型 则给10分
         *  如果周期类型是日分区
         *       如果说没有分区信息 即 tablemetainfo中的partition_col_name_json 为[] 即数组为空类型则给0分
         *       如果说生命周期天数字段未设置 即为-1 也是给0分 即在tablemetainfoextra中的lifecycle_days 为 -1 设为0分
         *       如果说声明周期天数字段的天数超过建议周期天数 则给5分
         *
         */
        //先获取建议周期天数字段即在指标明细表中的mertic_param_json字段中
        //然后获取每个表的生命周期类型
        String lifecycleType = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType();
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        Long tableId = assessParam.getTableMetaInfo().getId();
        //获取天数
        Integer days = JSON.parseObject(metricParamsJson).getInteger("days");
        if (lifecycleType.equals(MetaConstant.LIFECYCLE_TYPE_UNSET)){
            //给0分
            assessScore(BigDecimal.ZERO,"未设置生命周期类型","",true,tableId.toString(),assessDetail);

        }else if (lifecycleType.equals(MetaConstant.LIFECYCLE_TYPE_DAY)){
            if ("[]".equals(assessParam.getTableMetaInfo().getPartitionColNameJson())){
                assessScore(BigDecimal.ZERO,"日分区信息设置不合理","无分区信息字段",true,tableId.toString(),assessDetail);


            }
            if (assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleDays() == -1){
                assessScore(BigDecimal.ZERO,"日分区信息设置不合理","生命周期天数为设置",true,tableId.toString(),assessDetail);

            }else if (assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleDays() > days){
                assessScore(BigDecimal.valueOf(5),"生命周期天数超过建议天数","建议天数为"+ days,true,tableId.toString(),assessDetail);
            }
        }

    }
}

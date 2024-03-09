package cn.cug.dga.assess.assessor.quality;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.ds.bean.TDsTaskInstance;
import cn.cug.dga.ds.service.TDsTaskInstanceService;
import cn.cug.dga.utils.AssessParam;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.aspectj.apache.bcel.classfile.ModulePackages;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;

/**
 * author song
 * date 2024/3/3 19:14
 * Desc  考评表产出的时效监控
 *       前一天产出时效，超过前{days}天产出时效平均值{percent}%‘
 *       {"days":7,"percent":70}
 *       则给0分，其余10分
 *
 *       所谓时效就是指的是taskinstance中的start_time 和end_time中间的花的时间 即表示该表的时效
 *
 *       要和 前几天的时效平均值比较 那么就是在sql过滤时 过滤这几天的数据 然后求这些天的start_time 和end_time
 *       然后再取一个平均值就可以得到
 *       然后乘以一个参数中比例 就能得到比较
 *
 *
 */
@Component("TIME_LINESS")
public class CheckTableEfficiencyAssessor extends AssessTemplate {

    @Autowired
    private TDsTaskInstanceService  tDsTaskInstanceService;

    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {

        //获取参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        Integer days = JSONObject.parseObject(metricParamsJson).getInteger("days");
        Integer percent = JSONObject.parseObject(metricParamsJson).getInteger("percent");

        //对于ods层以及dim_date的表不需要判断
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        if (dwLevel.equals(MetaConstant.DW_LEVEL_ODS) || "dim_date".equals(assessParam.getTableMetaInfo().getTableName())){
            return;
        }

        // 根据表名来获取到tdstaskinstance
        String schemaName = assessParam.getTableMetaInfo().getSchemaName();
        String tableName = assessParam.getTableMetaInfo().getTableName();
        //获取考评时间
        String assessDate = assessParam.getAssessDate();
        //封装task的名称
        String taskName = schemaName + "." + tableName;

        //查询当天的task的时效数据 //因为我们其实只有部分信息 可以只查部分字段来封装
        QueryWrapper<TDsTaskInstance> selectSecquery = new QueryWrapper<TDsTaskInstance>()
                .eq("date(start_time)", assessDate)
                .eq("state", MetaConstant.TASK_STATE_SUCCESS)
                .eq("name", taskName)
                //只需要时效字段即可 在mysql中有timestampdiff方法来得到两个列的差值 第一个参数可以指定是按照秒还是按照其他单位来计算
                .select("timestampdiff(second,start_time,end_time) sec");//还可以取别名


        //调用tdsservice层的方法
        //其中getmap表示的是如果查询结果只有一行就使用getmap 返回的map的k就是字段名，v就是字段值 如果有多个字段就这样存入即可
        Map<String, Object> mapCol = tDsTaskInstanceService.getMap(selectSecquery);

        //获取时效字段值
        Long secondSec = (Long)mapCol.get("sec");


        //接下来就是获取前几天的时效平均值 那么在构造wrapper时 明确start_time即可 其中ge表示的是大于 gt表示的是大于等于 le表示的是小于 lt表示的是小于等于

        //获取ge的值 即要大于这个值
        LocalDate localDate = LocalDate.parse(assessDate).minusDays(days); //minus表示的是减
        //将其转为string类型
        String dayBeforeDay = localDate.toString();
        
        //lt的值就是assessdate

        //封装querywrapper
        QueryWrapper<TDsTaskInstance> wrapper = new QueryWrapper<TDsTaskInstance>()
                .eq("name", taskName)
                .eq("state", MetaConstant.TASK_STATE_SUCCESS)
                .ge("date(start_time)", dayBeforeDay)
                .lt("date(start_time)", assessDate)
                .select("avg(timestampdiff(second,start_time,end_time)) avgSpec");

        Map<String, Object> mapLimit = tDsTaskInstanceService.getMap(wrapper);

        //如果说没有那么多天的话 这个maplimit 就会为空 为空就不需要比较
        if (mapLimit == null || mapLimit.isEmpty()){
            return  ;
        }

        Long avgSpec = (Long)mapLimit.get("avgSpec");

        BigDecimal limitSpec = BigDecimal.valueOf(avgSpec).multiply(BigDecimal.valueOf(100 + percent)).movePointLeft(2);

        //判断是否超过阈值 超过就需要判0分
        if (BigDecimal.valueOf(secondSec).compareTo(limitSpec) == 1){
            assessScore(BigDecimal.ZERO,"时效超过阈值","今天运行的时效:"+secondSec +",超过了过去"+days+"时效的均值:"+avgSpec+"的"+percent+"%",false,null,assessDetail);
        }







    }
}

package cn.cug.dga.assess.assessor.cal;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.utils.AssessParam;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.ivy.util.DateUtil;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * author song
 * date 2024/3/2 14:26
 * Desc 一张表{days}天内没有产出数据  则给0分，其余给10
 */
@Component
public class CheckTableProduceAssessor extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {
        //如何定义表产出数据：如果说该表产生了数据 就说明往这个表插入了数据 那么就会有最后一次修改时间
        //那最后一次修改时间和考评时间来做差得到多少天的时间 从而利用这个时间来判断
        //获取表的最后一次修改时间
        Timestamp tableLastModifyTime = assessParam.getTableMetaInfo().getTableLastModifyTime();
        //获取参数阈值
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        Integer day = JSONObject.parseObject(metricParamsJson).getInteger("day");

        //获取考考评时间
        String assessDate = assessParam.getAssessDate();

        //将string 即 考评时间解析成时间对象 使用dateutil工具
        //在lang3包下有dateutils工具类 可以将日期字符串 转为时间对象 dateformatutils可以将时间对象转为日期字符串
        Date assessTime = DateUtils.parseDate(assessDate, "yyyy-MM-dd");

        //将最后一次修改时间和考评时间去差值 根据毫秒时间戳来去差值
        long diffmills = Math.abs(tableLastModifyTime.getTime() - assessTime.getTime());
        //将毫秒差值转为填
        long diffDays = TimeUnit.DAYS.convert(diffmills, TimeUnit.MILLISECONDS);
        if (diffDays > day){
            //说明超过了就要赋值
            assessScore(BigDecimal.ZERO,"超过了" + diffDays + "天 该表没有产出数据","",false,null,assessDetail);
        }
        // 接下来的工作还有很多 实际我一件事都完成不了了
    }
}

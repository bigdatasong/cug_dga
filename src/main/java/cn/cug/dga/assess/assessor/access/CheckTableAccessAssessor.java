package cn.cug.dga.assess.assessor.access;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.utils.AssessParam;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * author song
 * date 2024/3/2 14:59
 * Desc 一张表{days}天内没有访问 则给0分 ， 其余给10
 */
@Component("NO_ACCESS")
public class CheckTableAccessAssessor extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {
        //上一个长期未产出用的思路就是获取的是毫秒时间的差值来得到相差多少天 然后再去判断这个天数是否超过阈值
        // 获取阈值
        Integer limit_days = JSONObject.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson()).getInteger("days");

        // 接下来直接用日期来进行做差 使用新的api
        //获取最后一次访问时间
        Timestamp tableLastAccessTime = assessParam.getTableMetaInfo().getTableLastAccessTime();
        //将时间戳对象转为localdatetime
        // 其中ofinstant中第一个参数是需要一个instant 可以根据毫秒值来得到instant，另外第二个参数就是指定时区
        LocalDateTime lastAccessTimeLDT = LocalDateTime.ofInstant(Instant.ofEpochMilli(tableLastAccessTime.getTime()), ZoneId.of("Asia/Shanghai"));

        // 我们接下来的思路即使拿考评时间 - 阈值时间 只要这个最后一次访问时间早于这个符合要求的差值 就说明已经长期未被访问了
        //获取考评时间
        String assessDate = assessParam.getTableMetaInfo().getAssessDate();

        LocalDate parse = LocalDate.parse(assessDate); //这个时间是只有到天的 没有时分秒
        LocalDateTime localDateTime = parse.atStartOfDay();

        // 将这个加上天的值减去阈值
        LocalDateTime localDateTime1 = localDateTime.plusDays(limit_days);
        //只要这个时间早于最后一次未被访问的时间就说明长期未被访问
        if (localDateTime1.isBefore(lastAccessTimeLDT)){
            assessScore(BigDecimal.ZERO,"超过"+limit_days +"未被访问","",false,null,assessDetail);
        }


    }
}

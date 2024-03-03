package cn.cug.dga.assess.assessor.cal;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.assess.mapper.GovernanceAssessDetailMapper;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.ds.bean.TDsTaskInstance;
import cn.cug.dga.ds.service.TDsTaskInstanceService;
import cn.cug.dga.utils.AssessParam;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * author song
 * date 2024/3/2 19:14
 * Desc 判断当天当前表对应的任务是否有报错
 */
@Component("TASK_FAILED")
public class CheckTaskFailed extends AssessTemplate {

    @Autowired
    private TDsTaskInstanceService tDsTaskInstanceService;

    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {
        //首先肯定是需要操作 任务实例表 在任务实例表中有status字段 其中6表示的是任务失败的意思 7表示的是任务成功的意思 在页面中也是有分别对应的字段的
        // 另外我们是要判断当天的表的对应的任务的运行情况 在该表中有一个start_time 字段 记录的时间 只不过这个时间精确到秒
        // 而我们只需要到天即可 所以说可以在sql中使用date函数用于返回日期到天

        // 因为有些表是不需要任务实例计算的 比如 ods层的表 以及dim_date表 这个表一年只导入一次
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        String tableName = assessParam.getTableMetaInfo().getTableName();
        if (dwLevel.equals(MetaConstant.DW_LEVEL_ODS) || "dim_date".equals(tableName)){
            return ;
        }

        //先获取表和库名 用于表示任务实例的名称
        String instanceName = assessParam.getTableMetaInfo().getSchemaName() + "." + assessParam.getTableMetaInfo().getTableName();
        // 获取考评时间
        String assessDate = assessParam.getTableMetaInfo().getAssessDate();
        //调用service 来操作任务实例这张表
        //根据 实例名称以及考评时间来查询得到当前表对应的任务实例
        //查询出当天任务失败的对应当前表的任务实例
            QueryWrapper<TDsTaskInstance> wrapper = new QueryWrapper<TDsTaskInstance>().eq("name", instanceName).eq("date(start_time)", assessDate)
                .eq("state", MetaConstant.TASK_STATE_FAILD);

        // 因为同一天的时候可能会运行同一个实例多次 所以在这个表中就会记录这个实例多次的信息 这些信息也是对应的当前表的 所以需要查出来
        List<TDsTaskInstance> tDsTaskInstances = tDsTaskInstanceService.list(wrapper);
        if (!tDsTaskInstances.isEmpty()){
            //说明当前表对应的有失败的运行实例
            //那么我们可以把这些失败实例 使用stream api 返回需要的信息 比如 任务实例的id 任务实例的名称 以及任务实例开始时间和结束时间
            // collectors.joining 这个方法的意思就是说前面我返回的其实多个由id、name、时间拼接好的字符串 那么我们可以不返回list
            //而是直接返回一个字符串 这个字符串包含了list中所有字符串的信息
            String collect = tDsTaskInstances.stream().map(t -> t.getId() + "-" + t.getName() + "-" + t.getStartTime() + "-" + t.getEndTime())
                    .collect(Collectors.joining(","));

            assessScore(BigDecimal.ZERO,instanceName +"实例出现了失败的情况",collect,false,null,assessDetail);
        }


    }
}

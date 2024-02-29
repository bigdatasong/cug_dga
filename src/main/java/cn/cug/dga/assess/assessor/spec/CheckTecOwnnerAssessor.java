package cn.cug.dga.assess.assessor.spec;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoExtra;
import cn.cug.dga.utils.AssessParam;
import jodd.util.StringUtil;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * author song
 * date 2024/2/28 16:16
 * Desc 定义是否有技术负责人子类 来处理
 */
@Component("TEC_OWNER")
public class CheckTecOwnnerAssessor  extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {

        //判断是否有技术负责人的逻辑就是 将额外信息中的技术负责人取出 判断是否存在
        //如果不存在 就需要根据要求判分 即修改分数,还有就是继续赋其他的值 如果有治理链接就将tableid将其链接替换
        //不妨在父类声明一个方法 用于处理这些逻辑
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();
        // stringutill.isblank 判断字符串是否为null 或者'' 或者'白字符'   如果是就返回true
        if (StringUtil.isBlank(tableMetaInfoExtra.getTecOwnerUserName())){
            //需要修改分数
            assessScore(BigDecimal.ONE,"没有技术负责人","",true,tableMetaInfo.getId().toString(),assessDetail);

        }


    }
}

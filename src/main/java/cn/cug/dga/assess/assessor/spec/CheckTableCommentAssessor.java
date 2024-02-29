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
 * date 2024/2/28 17:12
 * Desc 是否有表备注
 */
@Component("TABLE_COMMENT")
public class CheckTableCommentAssessor  extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();
        if (StringUtil.isBlank(tableMetaInfo.getTableComment())){
            assessScore(BigDecimal.ZERO,"没有表备注","",false,null,assessDetail);


        }
    }
}

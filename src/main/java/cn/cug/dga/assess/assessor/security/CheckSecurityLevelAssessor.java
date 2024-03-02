package cn.cug.dga.assess.assessor.security;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.utils.AssessParam;
import jodd.util.StringUtil;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * author song
 * date 2024/3/1 21:01
 * Desc 判断是否明确安全等级
 */
@Component("SECURITY_LEVEL")
public class CheckSecurityLevelAssessor extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        //获取安全等级字段
        String securityLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getSecurityLevel();
        Long tableID = assessParam.getTableMetaInfo().getId();
        if (StringUtil.isBlank(securityLevel) || securityLevel.equals(MetaConstant.SECURITY_LEVEL_UNSET)){
            assessScore(BigDecimal.ZERO,"未明确安全等级","",true,tableID.toString(),assessDetail);
        }
    }
}

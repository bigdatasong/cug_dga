package cn.cug.dga.assess.assessor.spec;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoExtra;
import cn.cug.dga.utils.AssessParam;
import com.mysql.cj.util.StringUtils;
import jodd.util.StringUtil;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * author song
 * date 2024/2/28 17:04
 * Desc
 */
@Component("BUSI_OWNER")
public class CheckBusOwnnerAssessor extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();
        if (StringUtil.isBlank(tableMetaInfoExtra.getBusiOwnerUserName())){
            assessScore(BigDecimal.ZERO,"没有指定业务负责人","",true,tableMetaInfo.getId().toString(),assessDetail);
        }
    }
}

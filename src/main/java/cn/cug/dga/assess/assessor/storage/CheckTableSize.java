package cn.cug.dga.assess.assessor.storage;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.meta.service.TableMetaInfoService;
import cn.cug.dga.utils.AssessParam;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * author song
 * date 2024/3/1 15:14
 * Desc 判断是否为空表
 */
@Component("TABLE_EMPTY")
public class CheckTableSize extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        Long tableSize = assessParam.getTableMetaInfo().getTableSize();

        if (tableSize == 0){
            assessScore(BigDecimal.ZERO,"表大小为空","",false,null,assessDetail);
        }
    }
}

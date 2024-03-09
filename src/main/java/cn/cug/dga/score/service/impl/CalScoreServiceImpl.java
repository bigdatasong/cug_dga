package cn.cug.dga.score.service.impl;

import cn.cug.dga.score.bean.GovernanceAssessGlobal;
import cn.cug.dga.score.service.CalScoreService;
import cn.cug.dga.score.service.GovernanceAssessGlobalService;
import cn.cug.dga.score.service.GovernanceAssessTableService;
import cn.cug.dga.score.service.GovernanceAssessTecOwnerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * author song
 * date 2024/3/9 21:34
 * Desc
 */
@Service
public class CalScoreServiceImpl implements CalScoreService {

    @Autowired
    private GovernanceAssessTableService governanceAssessTableService;

    @Autowired
    private GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    private GovernanceAssessGlobalService governanceAssessGlobalService;

    @Override
    public void CalScore(String assessDate) {
        //调用编写好的三个service方法完成逻辑
        //首先就是计算每个表的分
        //其次就是计算技术负责人的得分
        //最后就是计算全局得分
        governanceAssessTableService.calScorePerTable(assessDate);

        governanceAssessTecOwnerService.calScoreByTecOwner(assessDate);

        governanceAssessGlobalService.calGlobalScore(assessDate);

    }
}

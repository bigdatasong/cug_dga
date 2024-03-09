package cn.cug.dga.score.service.impl;

import cn.cug.dga.score.bean.GovernanceAssessGlobal;
import cn.cug.dga.score.mapper.GovernanceAssessGlobalMapper;
import cn.cug.dga.score.service.GovernanceAssessGlobalService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 治理总考评表 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
@Service
public class GovernanceAssessGlobalServiceImpl extends ServiceImpl<GovernanceAssessGlobalMapper, GovernanceAssessGlobal> implements GovernanceAssessGlobalService {

    @Override
    public void calGlobalScore(String assessDate) {
        remove(new QueryWrapper<GovernanceAssessGlobal>().eq("assess_date",assessDate));

        GovernanceAssessGlobal governanceAssessGlobal = this.baseMapper.calScorePerglobal(assessDate);

        save(governanceAssessGlobal);
    }
}

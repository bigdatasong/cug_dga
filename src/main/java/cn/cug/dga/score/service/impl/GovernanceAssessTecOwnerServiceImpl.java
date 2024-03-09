package cn.cug.dga.score.service.impl;

import cn.cug.dga.score.bean.GovernanceAssessTecOwner;
import cn.cug.dga.score.mapper.GovernanceAssessTecOwnerMapper;
import cn.cug.dga.score.service.GovernanceAssessTecOwnerService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 技术负责人治理考评表 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
@Service
public class GovernanceAssessTecOwnerServiceImpl extends ServiceImpl<GovernanceAssessTecOwnerMapper, GovernanceAssessTecOwner> implements GovernanceAssessTecOwnerService {

    @Override
    public void calScoreByTecOwner(String assessDate) {

        //同样写remove掉
        remove(new QueryWrapper<GovernanceAssessTecOwner>().eq("assess_date",assessDate));

        List<GovernanceAssessTecOwner> governanceAssessTecOwners = this.baseMapper.calScoreByTecOwner(assessDate);
        saveBatch(governanceAssessTecOwners);
    }
}

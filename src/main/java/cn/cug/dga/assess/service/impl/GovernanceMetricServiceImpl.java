package cn.cug.dga.assess.service.impl;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.assess.bean.GovernanceMetric;
import cn.cug.dga.assess.mapper.GovernanceMetricMapper;
import cn.cug.dga.assess.service.GovernanceMetricService;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.service.TableMetaInfoService;
import cn.cug.dga.utils.AssessParam;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 考评指标参数表 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-02-27
 */
@Service
public class GovernanceMetricServiceImpl extends ServiceImpl<GovernanceMetricMapper, GovernanceMetric> implements GovernanceMetricService {


}

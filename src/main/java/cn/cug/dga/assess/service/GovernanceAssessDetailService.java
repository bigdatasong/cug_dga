package cn.cug.dga.assess.service;

import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理考评结果明细 服务类
 * </p>
 *
 * @author song
 * @since 2024-02-27
 */
public interface GovernanceAssessDetailService extends IService<GovernanceAssessDetail> {
    void assessFordate(String schemaName,String accessDate);

}

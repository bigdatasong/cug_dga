package cn.cug.dga.score.service;

import cn.cug.dga.score.bean.GovernanceAssessTecOwner;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 技术负责人治理考评表 服务类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
public interface GovernanceAssessTecOwnerService extends IService<GovernanceAssessTecOwner> {

    //定义方法封装GovernanceAssessTecOwner
    void calScoreByTecOwner(String assessDate);

}

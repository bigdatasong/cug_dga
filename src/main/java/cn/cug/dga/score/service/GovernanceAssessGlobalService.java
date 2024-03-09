package cn.cug.dga.score.service;

import cn.cug.dga.score.bean.GovernanceAssessGlobal;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理总考评表 服务类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
public interface GovernanceAssessGlobalService extends IService<GovernanceAssessGlobal> {

    //定义一个方法实现全局分数的封装
    void calGlobalScore(String assessDate);

}

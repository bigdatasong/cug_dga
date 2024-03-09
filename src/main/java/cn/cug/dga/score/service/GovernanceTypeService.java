package cn.cug.dga.score.service;

import cn.cug.dga.score.bean.GovernanceType;
import com.baomidou.mybatisplus.extension.service.IService;

import java.math.BigDecimal;
import java.util.Map;

/**
 * <p>
 * 治理考评类别权重表 服务类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
public interface GovernanceTypeService extends IService<GovernanceType> {

    //定义方法 只返回权重信息表中的权重编码以及权重占比
    //可以返回一个map k为权重编码 v为权重值
    Map<String, BigDecimal> getWeightMap();

}

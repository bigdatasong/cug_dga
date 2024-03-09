package cn.cug.dga.score.service.impl;

import cn.cug.dga.score.bean.GovernanceType;
import cn.cug.dga.score.mapper.GovernanceTypeMapper;
import cn.cug.dga.score.service.GovernanceTypeService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.injector.methods.SelectMaps;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 治理考评类别权重表 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
@Service
public class GovernanceTypeServiceImpl extends ServiceImpl<GovernanceTypeMapper, GovernanceType> implements GovernanceTypeService {

    //定义一个方式实现 权重指标以及权重编码的返回
    @Override
    public Map<String, BigDecimal> getWeightMap() {

        //之前会说用listmaps来实现 但是listmap返回的是list list中是map map中的k和v分别是字段名 以及字段值。所以说这是不符合的
        //所以说我们应该先全部查出来 然后再封装成一个map
        Map<String,BigDecimal> map = new HashMap<>();

        List<GovernanceType> list = list();
        list.stream().forEach(
                l -> map.put(l.getTypeCode(),l.getTypeWeight())
        );

        return map;
    }
}

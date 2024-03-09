package cn.cug.dga.score.service.impl;

import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.score.bean.GovernanceAssessTable;
import cn.cug.dga.score.mapper.GovernanceAssessTableMapper;
import cn.cug.dga.score.service.GovernanceAssessTableService;
import cn.cug.dga.score.service.GovernanceTypeService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.datanucleus.enhancer.methods.IsXXX;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * 表治理考评情况 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
@Service
public class GovernanceAssessTableServiceImpl extends ServiceImpl<GovernanceAssessTableMapper, GovernanceAssessTable> implements GovernanceAssessTableService {

    @Autowired
    private GovernanceAssessTableMapper governanceAssessTableMapper;

    @Autowired
    private GovernanceTypeService governanceTypeService;

    //在这里实现将GovernanceAssessTable数据进行封装 到数据库中
    @Override
    public void calScorePerTable(String assessDate) {
        //同样为了避免重复得数据 每次封装前 都需要将之前的相同数据的表数据进行删除
        QueryWrapper<GovernanceAssessTable> removeWrapper = new QueryWrapper<GovernanceAssessTable>()
                .eq("assess_date", assessDate);
        remove(removeWrapper); // 即删除今天的数据

        //先将已经封装号的字段数据查出来
        List<GovernanceAssessTable> governanceAssessTables = governanceAssessTableMapper.calScorePerTable(assessDate);

        // 上述的governanceassesstable中是没有五维的权重分数字段的  即scoreOnTypeWeight
        //我们应该调用权重service 将权重信息查出来 但是因为我们只要权重编码以及权重值 所以需要在权重service层中来编写方法 实现只返回权重编码以及权重值

        Map<String, BigDecimal> weightMap = governanceTypeService.getWeightMap();

        //接下来就是说对于governanceAssessTables中每一个GovernanceAssessTable 都需要算五位的加权值
        //思路就是说如果是calc类型就调用getcalcaverage * 权重类型的权重 + 。。。。
        //在这里我们使用两种方法 来进行代码的实现
        //在这里定义一个方法 根据类型来调用对象中的与类型有关的权重方法 即使用反射的方式来获取到方法对象
        //返回值就是分数类型
        Set<String> typeSet = weightMap.keySet();
        governanceAssessTables.stream().forEach(
                g -> {
                    //将权重类型遍历 然后在遍历中调用callGetterByStr 得到每个权重加权后的值
                    //所以我们需要获得所有的权重类型

                    BigDecimal scoreOnTypeWeight = BigDecimal.ZERO;

                    for (String s : typeSet) {
                        //因为bigdecimal中add方法是有返回值的 所以我们需要重新赋值给scoreOnTypeWeight
                        try {
                            scoreOnTypeWeight = scoreOnTypeWeight.add(callGetterByStr(s,g));
                        } catch (Exception e) {
                            throw new RuntimeException(e);

                        }
                    }

                    //假设总分是100分 此时应该移动小数点1位
                    g.setScoreOnTypeWeight(scoreOnTypeWeight.movePointLeft(1));
                }
        );

        //最后就是存入到数据库中
        saveBatch(governanceAssessTables);


        //还有一种思路就是说直接对governanceAssessTables进行遍历 然后再遍历中赋值时，
        //直接计算其加权以后的平均值
//        for (GovernanceAssessTable governanceAssessTable : governanceAssessTables) {
//            //将每个权重的分数都算出来 通过算好的avg乘以相对应的权重
//            //在常量类中定义好各个类型的权重名称
//            governanceAssessTable.getScoreSpecAvg().multiply(weightMap.get(MetaConstant.xxxx);
//            governanceAssessTable.getScoreCalcAvg().multiply(weightMap.get(IsXXX));
//            //然后将上述的所有值加在一起 然后给scoretypeweight赋值
//        }
    }

    //因为需要通过反射来获取方法对象 所以说我们需要将对象传入
    private BigDecimal callGetterByStr(String type,GovernanceAssessTable governanceAssessTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //            type: CACL
        //                调用一个 对象的 getScoreCalcAvg()方法

        //定义方法名称  type.substring(0,1) 因为在方法中第一个是大写 后面的就是小写
        String methodName = "getScore" + type.substring(0,1) + type.substring(1).toLowerCase() + "Avg";

        //获取方法对象
        Class<? extends GovernanceAssessTable> aClass = governanceAssessTable.getClass();
        //通过方法对象调用指定的方法  getmethod 方法参数中第一个参数就是方法名称 第二个就是方法的参数 没有参数就不需要填
        Method method = aClass.getMethod(methodName);
        //执行方法 获取方法的返回值
        BigDecimal invoke = (BigDecimal) method.invoke(governanceAssessTable);

        return invoke;


    }


}

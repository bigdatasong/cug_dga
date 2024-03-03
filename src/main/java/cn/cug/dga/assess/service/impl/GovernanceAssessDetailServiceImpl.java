package cn.cug.dga.assess.service.impl;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.assess.bean.GovernanceMetric;
import cn.cug.dga.assess.mapper.GovernanceAssessDetailMapper;
import cn.cug.dga.assess.service.GovernanceAssessDetailService;
import cn.cug.dga.assess.service.GovernanceMetricService;
import cn.cug.dga.ds.bean.TDsTaskInstance;
import cn.cug.dga.ds.service.TDsTaskInstanceService;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.service.TableMetaInfoService;
import cn.cug.dga.utils.AssessParam;
import cn.cug.dga.utils.MetaUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>
 * 治理考评结果明细 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-02-27
 */
@Service
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {



    @Autowired
    private TableMetaInfoService tableMetaInfoService;

    @Autowired
    private GovernanceMetricService governanceMetricService;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private MetaUtil metaUtil;

    @Autowired
    private TDsTaskInstanceService tDsTaskInstanceService;
    @Override
    public void assessFordate(String schemaName,String accessDate) {

        // 考评前需要先获取源数据表数据
        List<TableMetaInfo> tableMetaInfoList = tableMetaInfoService.queryMetainfoBydate(schemaName, accessDate);

        //为了后续需要这个所有表的元数据信息 在这里查出来的元数据信息封装成一个map
        // 定义一个工具类 在工具类中来new hashmap
        //可以使用传统的遍历方式 一个一个将k和v put进去
        //也可以使用stream流的方式
        //在存入之前 先清空
        metaUtil.getTableMetaInfoMap().clear();
        Map<String, TableMetaInfo> map = tableMetaInfoList.stream().collect(
                Collectors.toMap(
                        tablemeta -> tablemeta.getSchemaName() + '.' + tablemeta.getTableName(),
                        Function.identity()
                )
        );
        metaUtil.setTableMetaInfoMap(map);

        Set<String> strings = metaUtil.getTableMetaInfoMap().keySet();

        // 同样我们在这将所有taskinstance表查出来封  装在utils中 并且这个taskinstance中有专门的字段表示sql
        //那我们就需要taskinstanceserivce中编写方法 封装出所有的包含sql的taskinstance
        //所需参数应该要有一个表示库名加表名的字段 因为我们在之前的metautil中已经封装了每个库和表 对应的tablemetainfo信息
        //所以他的key的组合就是库名加表名的集合
        List<TDsTaskInstance> allTdsTaskInstance = tDsTaskInstanceService.getAllTdsTaskInstance(accessDate, strings);

        //将其封装到metautil中
        Map<String, TDsTaskInstance> collect = allTdsTaskInstance.stream().collect(
                Collectors.toMap(
                        task -> task.getName(),
                        Function.identity()
                )
        );

        metaUtil.setTDsTaskInstanceMap(collect);


        //获取所有指标 需要那些未禁用的指标
        List<GovernanceMetric> governanceMetrics = governanceMetricService.list(new QueryWrapper<GovernanceMetric>().eq("is_disabled", "否"));

        // 定义封装好的指标明细 将其放入到list中
        List<GovernanceAssessDetail> result = new ArrayList<>();

        //考评的流程就是对这些所有元数据表考评每一个指标从而得到一个指标详细信息 然后将这个详细信息封装成指标详细信息表存入到mysql中
        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {

            for (GovernanceMetric governanceMetric : governanceMetrics) {

                //根据指标类型 通过容器获取不同的对象
                AssessTemplate access = applicationContext.getBean(governanceMetric.getMetricCode(), AssessTemplate.class);

                //封装考评参数
                AssessParam assessParam = new AssessParam();
                assessParam.setTableMetaInfo(tableMetaInfo);
                assessParam.setGovernanceMetric(governanceMetric);
                assessParam.setAssessDate(accessDate);

                GovernanceAssessDetail governanceAssessDetail = access.doassess(assessParam);

                result.add(governanceAssessDetail);

            }
        }

        // 将结果批量存入数据库中
        saveBatch(result);



    }

}

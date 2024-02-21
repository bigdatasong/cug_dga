package cn.cug.dga.meta.service.impl;

import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.meta.bean.TableMetaInfoExtra;
import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import cn.cug.dga.meta.mapper.TableMetaInfoExtraMapper;
import cn.cug.dga.meta.service.TableMetaInfoExtraService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreInitContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-02-20
 */
@Service
public class TableMetaInfoExtraServiceImpl extends ServiceImpl<TableMetaInfoExtraMapper, TableMetaInfoExtra> implements TableMetaInfoExtraService {

    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;

    //初始化表的辅助信息
    //同样就是需要根据库名查询出所有表，然后对表名进行遍历，如果说这个表名在表辅助信息中不存在的话，就需要初始化表辅助信息，
    @Override
    public void initExtraMetaInfo(String db) throws MetaException {

        //辅助信息中技术负责人、业务负责人的数据模拟
        String [] tecOwners = {"张三","李强","王红","赵五","陈帅"};
        String [] busiOwners = {"张四","李不强","王绿","赵六","陈丑"};

        //根据库民获取所有表名同样需要hive的客户端
        List<String> allTables = hiveMetaStoreClient.getAllTables(db);

        // 需要将辅助信息封装成一个list中，然后批量存入数据库
        List<TableMetaInfoExtra> result = new ArrayList<>();

        for (String tableName : allTables) {

            // 判断表辅助信息是否存在
            TableMetaInfoExtra one = this.getOne(new QueryWrapper<TableMetaInfoExtra>().eq("schema_name", db).eq("table_name", tableName));

            //不存在 就需要初始化辅助信息
            if (one == null){
                //封装表辅助信息
                TableMetaInfoExtra tableMetaInfoExtra = new TableMetaInfoExtra();

                tableMetaInfoExtra.setSchemaName(db);
                tableMetaInfoExtra.setTableName(tableName);
                //对于技术负责人、业务负责人、以及其他的信息
                tableMetaInfoExtra.setTecOwnerUserName(tecOwners[RandomUtils.nextInt(0,tecOwners.length)]);
                tableMetaInfoExtra.setBusiOwnerUserName(busiOwners[RandomUtils.nextInt(0,busiOwners.length)]);

                //对于存储周期类型、生命周期安全级别、以及数仓层级都用常量类来封装
                tableMetaInfoExtra.setLifecycleType(MetaConstant.LIFECYCLE_TYPE_UNSET);
                tableMetaInfoExtra.setLifecycleDays(-1L);
                tableMetaInfoExtra.setSecurityLevel(MetaConstant.SECURITY_LEVEL_UNSET);

                //数仓所在层级 ，就需要根据表明来进行判断
                //定义一个方法，通过表名这个参数来判断这个表属于哪个层级
                tableMetaInfoExtra.setDwLevel(setLevel(tableName));
                tableMetaInfoExtra.setCreateTime(new Timestamp(System.currentTimeMillis()));

                //将每个都加入
                result.add(tableMetaInfoExtra);

            }

        }

        //最后保存在数据库中
        saveBatch(result);

    }



    //根据表名返回数仓层级
    private String setLevel(String tableName) {
        //在常量类中的数仓层级中，都是大写，不妨将表名转为大写，并且常量类中最多只有五个字符，所以只需要判断前五位是否匹配即可
        String upperCase = tableName.toUpperCase();
        // 截取前五位看是否包含指定字符
        String prefix = upperCase.substring(0, 5);
        if (prefix.contains(MetaConstant.DW_LEVEL_ODS)){
            return MetaConstant.DW_LEVEL_ODS;
        }else if (prefix.contains(MetaConstant.DW_LEVEL_DWD)){
            return MetaConstant.DW_LEVEL_DWD;
        }else if (prefix.contains(MetaConstant.DW_LEVEL_DIM)){
            return MetaConstant.DW_LEVEL_DIM;
        }else if (prefix.contains(MetaConstant.DW_LEVEL_DWS)){
            return MetaConstant.DW_LEVEL_DWS;
        }else if (prefix.contains(MetaConstant.DW_LEVEL_DM)){
            return MetaConstant.DW_LEVEL_DM;
        }else if (prefix.contains(MetaConstant.DW_LEVEL_UNSET)){
            return MetaConstant.DW_LEVEL_UNSET;
        }else {
            return MetaConstant.DW_LEVEL_OTHER;
        }

    }
}

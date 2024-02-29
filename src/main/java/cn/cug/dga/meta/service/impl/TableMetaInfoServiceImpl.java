package cn.cug.dga.meta.service.impl;

import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoExtra;
import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import cn.cug.dga.meta.mapper.TableMetaInfoMapper;
import cn.cug.dga.meta.service.TableMetaInfoExtraService;
import cn.cug.dga.meta.service.TableMetaInfoService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.math3.analysis.function.Max;
import org.apache.commons.math3.analysis.function.Sin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.codehaus.janino.IClass;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * 元数据表 服务实现类
 * </p>
 *
 * @author song
 * @since 2024-02-07
 */
@Service
public class TableMetaInfoServiceImpl extends ServiceImpl<TableMetaInfoMapper, TableMetaInfo> implements TableMetaInfoService {

    //1、手动更新元数据
    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;  //并且这种方式还是所有方法公用这个客户端对象

    //需要用到extraservice的方法
    @Autowired
    private TableMetaInfoExtraService tableMetaInfoExtraService;

    @Override
    public void initMetaInfoTables(String schemaName, String assessDate) throws Exception {
        //每次点击手动更新时，为保证幂等性，即用户可能多次点击更新元数据，为了保证每次执行结果不重复，可以先去根据指定日期以及指定库来查询元数据是否存在，
        //如果存在就删除即可。
        //调用service中根据条件remove方法来remove
        this.remove(new QueryWrapper<TableMetaInfo>().eq("schema_name",schemaName).eq("assess_date",assessDate)); //不返回值即可
        //封装tablemetainfo属性主要包含两个部分，一个就是hive表的元数据信息，一个就是hdfs中元数据信息，
        //封装hive表的元数据就是每一张hive表都要封装成tablemetainfo，所以先定义一个方法，用于抽取hive表中元数据信息，其方法的返回值就是list集合，集合内容类型为tablemetainfo
        List<TableMetaInfo> tableMetaInfoList = extractMetaInfoFromHive(schemaName,assessDate);

        //抽取hdfs相关元数据信息 将上述list传入，直接补充list中元素的剩余信息，所以不需要返回值
        extractMetaInfoFromHdfs(tableMetaInfoList);

      //  System.out.println(tableMetaInfoList);
        //最后就是保存到数据库中
        saveOrUpdateBatch(tableMetaInfoList);


    }

    //抽取从hdfs中的元数据信息
    private void extractMetaInfoFromHdfs(List<TableMetaInfo> tableMetaInfoList) throws Exception {
        //访问hdfs需要客户端对象即filesystem对象，可以通过其get方法返回
        //在get方法中需要url，即hdfs的文件路径，因为涉及到每个表的路径不同，并且所属者也是不同，所以需要的客户端对象也可能不相同
        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {

            //获取filesystem对象
            FileSystem fileSystem = FileSystem.get(new URI(tableMetaInfo.getTableFsPath()), new Configuration(), tableMetaInfo.getTableFsOwner());

            //首先就是当前文件系统容量、使用量、剩余量
            tableMetaInfo.setFsCapcitySize(fileSystem.getStatus().getCapacity());
            tableMetaInfo.setFsUsedSize(fileSystem.getStatus().getUsed());
            tableMetaInfo.setFsRemainSize(fileSystem.getStatus().getRemaining());

            // 另外还有封装这个元数据信息时的创建时间
            tableMetaInfo.setCreateTime(new Timestamp(System.currentTimeMillis()));

            //表中所属数据量大小，所有副本数量总量大小，以及修改时间、最后访问时间，都需要经过遍历递归获取并且判断
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(tableMetaInfo.getTableFsPath()));
            // 上述就表示已经到了表这一层目录中，
            //接下来就是递归，可以创建一个递归方法 方便调用
            statTableSize(fileStatuses,tableMetaInfo,fileSystem); //之所以还需要filesystem对象是因为，可能目录中还有目录，所以需要
            //用它的liststatus的方法来得到下一层的目录状态，进而再调用递归方法

        }

    }
    //递归调用
    private void statTableSize(FileStatus[] fileStatuses, TableMetaInfo tableMetaInfo, FileSystem fileSystem) throws IOException {

        //遍历目录下的文件
        for (FileStatus fileStatus : fileStatuses) {
            //如果是文件
            if (fileStatus.isFile()) {
                //
                tableMetaInfo.setTableSize(tableMetaInfo.getTableSize() + fileStatus.getLen());
                tableMetaInfo.setTableTotalSize(tableMetaInfo.getTableTotalSize() + fileStatus.getLen() * fileStatus.getReplication());
                //最后修改时间和访问时间都需要通过比较来获取
                //1 第一种方式 直接比较时间戳,即timestamp对象中有settime方法 只需将时间戳比较得到最大的set进去即可
                tableMetaInfo.getTableLastModifyTime().setTime(
                        //从这里可以发现，如果在bean中不给属性赋初始值的话，就会出现空指针异常，解决方法要么就是在属性中加初值
                        //要么就是像课件中一样，在代码层面处理为null时应该怎么赋值的方法
                        Math.max(tableMetaInfo.getTableLastModifyTime().getTime(),fileStatus.getModificationTime())
                );
                tableMetaInfo.getTableLastAccessTime().setTime(
                        Math.max(tableMetaInfo.getTableLastAccessTime().getTime(),fileStatus.getAccessTime())

                );

                //2第二种方式就是直接set一个timestamp对象即可
                //timestmap对象有一个比较方法 即比较timestamp对象即可，然后根据判断结果来给tablemetainfo的属性赋值
//                int i = tableMetaInfo.getTableLastModifyTime().compareTo(new Timestamp(fileStatus.getModificationTime()));
//                if (i < 0 ){
//                    //说明后面的时间更大
//                    tableMetaInfo.setTableLastModifyTime(new Timestamp(fileStatus.getModificationTime()));
//                }


            }else{
                //是目录就需要获取下一层的文件路径，再递归调用
                FileStatus[] status = fileSystem.listStatus(fileStatus.getPath());
                statTableSize(status,tableMetaInfo,fileSystem);
            }
        }
    }

    //用户抽取hdfs中的元数据信息
    private List<TableMetaInfo> extractMetaInfoFromHive(String schemaName, String assessDate) throws Exception {
        //封装具体逻辑就是在一个lis集合中循环遍历，然后对于获取到的元数据信息table封装成tablemetainfo
        //在new list的时候，因为要封装的list的容量就是tablemetainfo的数量，然后呢我们也可以将table的数量获取到，这样提前说明有多少容量的话，可以优化，不需要动态扩容
        //获取所有表
        List<String> allTables = hiveMetaStoreClient.getAllTables(schemaName);
        List<TableMetaInfo> result = new ArrayList<>(allTables.size()); //要封装好的集合对象
        //因为对于每张表封装时，里面的字段也比较多，逻辑也有可能不一样，所以可以再定义一个方法来封装单个表
        for (String table : allTables) {
            //对没一张表进行抽取 ，获取某一表对象需要表的名称
            Table tableMeta = hiveMetaStoreClient.getTable(schemaName, table);
            //定义方法 返回tablemetainfo
            TableMetaInfo  tableMetaInfo = extractSingleTableMeta(tableMeta);
            //封装考评时间
            tableMetaInfo.setAssessDate(assessDate);
            result.add(tableMetaInfo);
        }
        return result;
    }
    //抽取单个表的信息
    private TableMetaInfo extractSingleTableMeta(Table tableMeta) {
        //通过debug的计算器来获得hive元数据中表的元数据信息 然后json格式化好
        TableMetaInfo tableMetaInfo = new TableMetaInfo();

        tableMetaInfo.setTableName(tableMeta.getTableName());
        tableMetaInfo.setSchemaName(tableMeta.getDbName());
        //分区字段的封装，因为分区字段不固定，不确定有多少个字段，所以可以封装成一个json字符串，这个json字符串是一个数组，数组中每个元素就是json
        //字符串，表示的是一个分区字段的信息包括name以及类型
        //tableMeta.getPartitionKeys() 返回的是一个list，元素表示的是分区字段，即有可能有多个分区字段,元素为json格式，里面包含分区字段的基本信息
        //另外因为在元素的json格式中还有一些不需要封装的信息，所以可以使用json工具中重载的方法，即过滤出需要的信息
        SimplePropertyPreFilter simplePropertyPreFilter = new SimplePropertyPreFilter("name", "comment", "type");
        tableMetaInfo.setPartitionColNameJson(JSON.toJSONString(tableMeta.getPartitionKeys(),simplePropertyPreFilter));

        // 字段名的封装
        tableMetaInfo.setColNameJson(JSON.toJSONString(tableMeta.getSd().getCols(),simplePropertyPreFilter));
        tableMetaInfo.setTableFsOwner(tableMeta.getOwner());
        tableMetaInfo.setTableParametersJson(JSON.toJSONString(tableMeta.getParameters()));
        tableMetaInfo.setTableComment(tableMeta.getParameters().get("comment"));
        tableMetaInfo.setTableFsPath(tableMeta.getSd().getLocation());
        tableMetaInfo.setTableInputFormat(tableMeta.getSd().getInputFormat());
        tableMetaInfo.setTableOutputFormat(tableMeta.getSd().getOutputFormat());
        tableMetaInfo.setTableRowFormatSerde(tableMeta.getSd().getSerdeInfo().getSerializationLib());
        //表的创建时间 ，因为在元数据中时间是十位，表示的是秒，需要转换成毫秒，在转换的过程中，因为十位表示的是int类型，如果转成毫秒乘以1000的话
        //可能会超出int类型的范围，从而导致时间计算出现错误，所以需要在转换的过程中需要转成long类型
        tableMetaInfo.setTableCreateTime(new Timestamp(tableMeta.getCreateTime()*1000L));
        tableMetaInfo.setTableType(tableMeta.getTableType());
        //对于分桶信息，如果说分桶个数存在的话，就说明就有分桶信息，没有分桶就说明就没有分桶信息
        tableMetaInfo.setTableBucketNum(tableMeta.getSd().getNumBuckets() + 0L);
        if (tableMeta.getSd().getNumBuckets() > 0) {
            //存在分桶信息
            tableMetaInfo.setTableBucketColsJson(JSON.toJSONString(tableMeta.getSd().getBucketCols()));
            tableMetaInfo.setTableSortColsJson(JSON.toJSONString(tableMeta.getSd().getSortCols()));

        }
        return tableMetaInfo;
    }

    //获取分页列表信息
    @Override
    public List<TableMetaInfoPageVo> queryPageDataForTables(TableMetaInfoForQuery tableMetaInfoForQuery) {

        // 由于需要从两张表中查询数据，所以需要自己写sql，所以也需要在mapper层定义方法
        List<TableMetaInfoPageVo> tableMetaInfoPageVos = this.baseMapper.queryPageDataForTables(tableMetaInfoForQuery);

        return tableMetaInfoPageVos;
    }

    //获取分页列表信息总数
    @Override
    public int queryPageDataForNum(TableMetaInfoForQuery tableMetaInfoForQuery) {

        int i = this.baseMapper.queryPageDataForNum(tableMetaInfoForQuery);

        return i ;
    }

    //单表数据详细查询其中包括辅助信息，用于表数据回显的接口需求
    @Override
    public TableMetaInfo tableDetailByid(String tableId) {

        //根据id查询tablemetainfo
        //然后根据表名和库名 查询表的辅助信息
        //将辅助信息bean封装到tablemetainfo中
        TableMetaInfo metaInfo = this.getById(tableId);

        TableMetaInfoExtra metaInfoExtraServiceOne = tableMetaInfoExtraService.getOne(new QueryWrapper<TableMetaInfoExtra>().eq("schema_name", metaInfo.getSchemaName()).eq("table_name", metaInfo.getTableName()));

        metaInfo.setTableMetaInfoExtra(metaInfoExtraServiceOne);

        return metaInfo;
    }

    /**
     *  根据库名以及考评时间查询元数据表信息
     *  实现思路，我们可以说用双层for循环去封装，但是和数据库交互时间过多不利于。所以不妨自己写sql来实现一次交互完成结果的封装
     * @param schemaName
     * @param asscessDate
     * @return
     */
    @Override
    public List<TableMetaInfo> queryMetainfoBydate(String schemaName, String asscessDate) {

        List<TableMetaInfo> tableMetaInfoList = baseMapper.queryMetaInfoBydate(schemaName, asscessDate);


        return tableMetaInfoList;
    }
}

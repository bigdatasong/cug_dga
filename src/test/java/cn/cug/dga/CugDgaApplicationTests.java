package cn.cug.dga;


import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import cn.cug.dga.meta.mapper.TableMetaInfoMapper;
import cn.cug.dga.meta.service.TableMetaInfoService;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class CugDgaApplicationTests {

    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;
    @Autowired
    private TableMetaInfoService tableMetaInfoService;

    @Autowired
    private TableMetaInfoMapper tableMetaInfoMapper;
    @Test
    public void testHivemetaClient() throws Exception {
        List<String> allDatabases = hiveMetaStoreClient.getAllDatabases();

        System.out.println(allDatabases);

    }

    @Test
    public void testExtractMetaInfo() throws Exception {
        tableMetaInfoService.initMetaInfoTables("gmall","2023-08-22");
    }

    @Test
    public void testQueryPageMapper(){

        TableMetaInfoForQuery tableMetaInfoForQuery = new TableMetaInfoForQuery();

        tableMetaInfoForQuery.setSchemaName("gmall");
        tableMetaInfoForQuery.setTableName("");
        tableMetaInfoForQuery.setPageNo(1);
        tableMetaInfoForQuery.setPageSize(10);

        List<TableMetaInfoPageVo> tableMetaInfoPageVos = tableMetaInfoMapper.queryPageDataForTables(tableMetaInfoForQuery);

        System.out.println(tableMetaInfoPageVos);

    }

    @Test
    public void testQueryPageForNum(){

        TableMetaInfoForQuery tableMetaInfoForQuery = new TableMetaInfoForQuery();

        tableMetaInfoForQuery.setSchemaName("gmall");
        tableMetaInfoForQuery.setTableName("o");

        int i = tableMetaInfoMapper.queryPageDataForNum(tableMetaInfoForQuery);


        System.out.println(i);

    }

    //测试一下根据库名以及考评时间 来查询表的元数据信息
    @Test
    public void testquerytableinfoBydate(){
        List<TableMetaInfo> tableMetaInfoList = tableMetaInfoMapper.queryMetaInfoBydate("gmall", "2023-08-22");

        System.out.println(tableMetaInfoList);
    }

}

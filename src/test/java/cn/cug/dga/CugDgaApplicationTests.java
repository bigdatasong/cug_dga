package cn.cug.dga;

import cn.cug.dga.meta.service.TableMetaInfoService;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class CugDgaApplicationTests {

    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;
    @Autowired
    private TableMetaInfoService tableMetaInfoService;
    @Test
    public void testHivemetaClient() throws Exception {
        List<String> allDatabases = hiveMetaStoreClient.getAllDatabases();

        System.out.println(allDatabases);

    }

    @Test
    public void testExtractMetaInfo() throws Exception {
        tableMetaInfoService.initMetaInfoTables("gmall","2023-08-22");
    }

}

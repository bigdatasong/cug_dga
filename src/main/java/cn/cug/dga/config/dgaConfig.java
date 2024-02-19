package cn.cug.dga.config;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * author song
 * date 2024/2/10 19:48
 * Desc 配置类对象
 */
@Configuration
public class dgaConfig {

    @Value("${hive.meta-server.url}")
    private String hiveMetaServerUrl;

    // 定义一个方法实现返回一个hivemetastoreclient 方法应该不是private类型
    @Bean
    public HiveMetaStoreClient createHiveMetaStoreClient()  {

        //需要一个configuration，并且是haddoop下的configuration，并且通过继承关系可知，hiveconf继承了这个configuration
        //另外为了方便封装参数，可以使用metastoreconf来封装参数
        //即通过metastoreconf.confvars.各种封装好的参数属性，然后val赋值给其属性，然后将这个参数在set到
        //new configuration中，其中val可以通过配置文件的注入可得，因为我们需要连接metastore服务，所以需要他的地址信息
        //这里我们可以不new configuration，因为我们这个类是一个配置类，需要一个configuration注解，这就和这个new的名字冲突了，就需要new全类名
        //不妨直接new他的子类，比如new hiveconf也是可以的
        HiveConf conf = new HiveConf();
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS,hiveMetaServerUrl);

        try {
            return new HiveMetaStoreClient(conf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }


}

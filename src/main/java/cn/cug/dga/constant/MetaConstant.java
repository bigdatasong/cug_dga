package cn.cug.dga.constant;

//避免被实例化
public interface MetaConstant
{

    //存储周期
    String LIFECYCLE_TYPE_PERM="PERM";  //永久
    String LIFECYCLE_TYPE_ZIP="ZIP";   //拉链
    String LIFECYCLE_TYPE_DAY="DAY";  //日分区
    String LIFECYCLE_TYPE_OTHER="OTHER";  //其他
    String LIFECYCLE_TYPE_UNSET="UNSET";  //未设置

    //安全级别
    String SECURITY_LEVEL_UNSET="UNSET";  //未设置
    String SECURITY_LEVEL_PUBLIC="PUBLIC";  //公开
    String SECURITY_LEVEL_INTERNAL="INTERNAL";  //内部
    String SECURITY_LEVEL_SECRET="SECRET";  //保密
    String SECURITY_LEVEL_HIGH="HIGH";  //高度机密

    //层级 对应页面上的下拉框中的选项
    String DW_LEVEL_UNSET = "UNSET";
    String DW_LEVEL_ODS = "ODS";
    String DW_LEVEL_DWD = "DWD";
    String DW_LEVEL_DWS = "DWS";
    String DW_LEVEL_DIM = "DIM";
    //Data Market 一张表中存放的数据很杂，不属于以上层，放到DM
    String DW_LEVEL_DM = "DM";
    String DW_LEVEL_ADS = "ADS";
    //表不属于以上的任意一层
    String DW_LEVEL_OTHER = "OTHER";

    // 定义每一层的正则表达式的规范 用于验证表名是否符合规范
    String GMALL_ODS_REGEX = "^ods_\\w+_(inc|full)$";
    // 思路: 首先肯定是以ods开头 其次是以inc或者full结尾 然后ods和inc|full前肯定有_  最后就是中间就是表名 表名由字母构成 又因为在java中\表示
    //特殊字符 需要再加一个\来转义 又因为表名肯定会有多个字符 所以需要加一个+
    String GMALL_DIM_REGEX = "^dim_\\w+_(zip|full)$";
    String GMALL_DWD_REGEX = "^dwd_(trade|tool|interaction|traffic|user)_\\w+_(inc|full|acc)$";
    String GMALL_DWS_REGEX = "^dws_(trade|tool|interaction|traffic|user)_\\w+_(\\d+d|nd|td)$"; //可能是1d 也可能是11d ...
    String GMALL_ADS_REGEX = "^ads_\\w+$"; //下面两个都是表示以任意多少个字符结尾都可以
    String GMALL_DM_REGEX = "^dm_\\w+$";

    Integer TASK_STATE_SUCCESS = 7;
    Integer TASK_STATE_FAILD = 6;


}
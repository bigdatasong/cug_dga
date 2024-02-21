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


}